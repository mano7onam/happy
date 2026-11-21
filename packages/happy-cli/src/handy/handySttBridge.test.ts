import { createServer, type Server } from 'node:http';
import { afterEach, describe, expect, it } from 'vitest';

import {
  HANDY_STT_METHOD,
  isAudioAttachment,
  registerHandySttRpc,
  transcribeAudioBufferWithHandy,
  type HandySttRpcRequest,
  type HandySttRpcResponse,
} from './handySttBridge';

describe('handySttBridge', () => {
  let server: Server | null = null;

  afterEach(async () => {
    if (server) {
      const s = server;
      server = null;
      await new Promise<void>((resolve) => s.close(() => resolve()));
    }
  });

  it('detects audio attachments by mime type and filename extension', () => {
    expect(isAudioAttachment('memo.m4a', 'application/octet-stream')).toBe(true);
    expect(isAudioAttachment('clip.ogg')).toBe(true);
    expect(isAudioAttachment('blob', 'audio/webm')).toBe(true);
    expect(isAudioAttachment('screenshot.png', 'image/png')).toBe(false);
  });

  it('proxies streaming start -> chunk -> finish and one-shot transcribe over handy-stt RPC', async () => {
    const receivedChunks: string[] = [];
    server = createServer((req, res) => {
      let raw = '';
      req.setEncoding('utf8');
      req.on('data', (c) => {
        raw += c;
      });
      req.on('end', () => {
        res.setHeader('Content-Type', 'application/json');
        if (req.url === '/api/v1/health') {
          res.end(JSON.stringify({ ok: true, status: 'online' }));
          return;
        }
        const body = raw ? (JSON.parse(raw) as Record<string, unknown>) : {};
        if (req.url === '/api/v1/stream/start') {
          res.end(JSON.stringify({ ok: true, stream_id: 's-123' }));
          return;
        }
        if (req.url === '/api/v1/stream/chunk') {
          receivedChunks.push(String(body.pcm16_base64 ?? ''));
          res.end(JSON.stringify({ ok: true, stream_id: 's-123', text: 'Привет, проверь' }));
          return;
        }
        if (req.url === '/api/v1/stream/finish') {
          if (body.pcm16_base64) {
            receivedChunks.push(String(body.pcm16_base64));
          }
          res.end(
            JSON.stringify({
              ok: true,
              stream_id: 's-123',
              text: 'Привет, проверь PR в Antigravity.',
              duration_sec: 2.4,
            }),
          );
          return;
        }
        if (req.url === '/api/v1/transcribe') {
          res.end(JSON.stringify({ ok: true, text: 'Голосовой файл распознан.', duration_sec: 1.5 }));
          return;
        }
        res.statusCode = 404;
        res.end(JSON.stringify({ ok: false, error: 'not found' }));
      });
    });

    await new Promise<void>((resolve) => server!.listen(0, '127.0.0.1', () => resolve()));
    const addr = server.address();
    const port = typeof addr === 'object' && addr ? addr.port : 0;
    const baseUrl = `http://127.0.0.1:${port}`;

    let registeredHandler: ((p: HandySttRpcRequest) => Promise<HandySttRpcResponse>) | null = null;
    registerHandySttRpc(
      {
        registerHandler: (method, handler) => {
          expect(method).toBe(HANDY_STT_METHOD);
          registeredHandler = handler as unknown as (p: HandySttRpcRequest) => Promise<HandySttRpcResponse>;
        },
      },
      baseUrl,
    );

    expect(registeredHandler).not.toBeNull();
    const ping = await registeredHandler!({ action: 'ping' });
    expect(ping).toEqual({ ok: true });

    const started = await registeredHandler!({ action: 'start' });
    expect(started).toEqual({ ok: true, streamId: 's-123' });

    const chunkRes = await registeredHandler!({
      action: 'chunk',
      streamId: 's-123',
      pcm16Base64: Buffer.from([1, 2, 3, 4]).toString('base64'),
    });
    expect(chunkRes.text).toBe('Привет, проверь');

    const finRes = await registeredHandler!({
      action: 'finish',
      streamId: 's-123',
      pcm16Base64: Buffer.from([5, 6]).toString('base64'),
    });
    expect(finRes).toEqual({
      ok: true,
      streamId: 's-123',
      text: 'Привет, проверь PR в Antigravity.',
      durationSec: 2.4,
    });
    expect(receivedChunks).toHaveLength(2);

    const fileText = await transcribeAudioBufferWithHandy(
      new Uint8Array([9, 8, 7]),
      'memo.m4a',
      baseUrl,
    );
    expect(fileText).toBe('Голосовой файл распознан.');
  });
});
