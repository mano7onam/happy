/**
 * Local bridge between Happy's E2EE machine/session RPC (`handy-stt`) and the
 * user's local `Handy Antigravity` STT HTTP server (`127.0.0.1:8787`).
 *
 * Kept completely self-contained in `src/handy/` so local rebases onto
 * `upstream/main` only touch two hook lines in `apiMachine.ts`.
 */

import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { logger } from '@/ui/logger';

export const HANDY_STT_METHOD = 'handy-stt';
export const DEFAULT_HANDY_STT_URL = 'http://127.0.0.1:8787';

function readHandyStateFile(name: string): string {
  try {
    const p = join(homedir(), '.stt-handy-antigravity', name);
    if (existsSync(p)) {
      return readFileSync(p, 'utf8').trim();
    }
  } catch {
    // ignore
  }
  return '';
}

function resolveHandyApiToken(): string {
  return (process.env.STT_HANDY_API_TOKEN || readHandyStateFile('remote_api_token')).trim();
}

const AUDIO_EXTENSIONS = new Set([
  '.m4a',
  '.wav',
  '.ogg',
  '.oga',
  '.opus',
  '.mp3',
  '.caf',
  '.aac',
  '.webm',
  '.flac',
]);

export interface RpcRegistrar {
  registerHandler<TReq = any, TRes = any>(
    method: string,
    handler: (params: TReq) => Promise<TRes> | TRes,
  ): void;
}

export type HandySttRpcRequest =
  | { action: 'ping' }
  | { action: 'start' }
  | { action: 'chunk'; streamId: string; pcm16Base64: string }
  | { action: 'finish'; streamId: string; pcm16Base64?: string }
  | { action: 'cancel'; streamId: string }
  | { action: 'transcribe'; audioBase64: string; filename?: string };

export interface HandySttRpcResponse {
  ok: boolean;
  streamId?: string;
  text?: string;
  durationSec?: number;
  error?: string;
}

export function resolveHandySttBaseUrl(explicit?: string): string {
  if (explicit) return explicit.replace(/\/+$/, '');
  if (process.env.HANDY_STT_URL) return process.env.HANDY_STT_URL.replace(/\/+$/, '');
  const vpsTunnelUrl = readHandyStateFile('tunnel_url.txt').replace(/\/+$/, '');
  if (vpsTunnelUrl.startsWith('https://')) {
    return vpsTunnelUrl;
  }
  return DEFAULT_HANDY_STT_URL;
}

export function isAudioAttachment(filename: string, mimeType?: string): boolean {
  if (mimeType && mimeType.toLowerCase().startsWith('audio/')) {
    return true;
  }
  const dot = filename.lastIndexOf('.');
  if (dot === -1) return false;
  return AUDIO_EXTENSIONS.has(filename.slice(dot).toLowerCase());
}

async function postHandyJson(
  baseUrl: string,
  path: string,
  payload: Record<string, unknown>,
  timeoutMs: number,
): Promise<Record<string, unknown>> {
  const headers: Record<string, string> = { 'Content-Type': 'application/json; charset=utf-8' };
  const tok = resolveHandyApiToken();
  if (tok) {
    headers['Authorization'] = `Bearer ${tok}`;
  }
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const data = (await res.json()) as Record<string, unknown>;
  if (!res.ok || data.ok === false) {
    throw new Error(String(data.error || `Handy STT HTTP ${res.status}`));
  }
  return data;
}

export async function transcribeAudioBufferWithHandy(
  audioBytes: Uint8Array,
  filename = 'voice.m4a',
  baseUrl?: string,
): Promise<string> {
  const url = resolveHandySttBaseUrl(baseUrl);
  const data = await postHandyJson(
    url,
    '/api/v1/transcribe',
    {
      audio_base64: Buffer.from(audioBytes).toString('base64'),
      filename,
      source: 'happy',
    },
    60_000,
  );
  return String(data.text ?? '').trim();
}

export async function handleHandySttRpc(
  params: HandySttRpcRequest,
  baseUrl?: string,
): Promise<HandySttRpcResponse> {
  const url = resolveHandySttBaseUrl(baseUrl);
  try {
    switch (params?.action) {
      case 'ping': {
        const res = await fetch(`${url}/api/v1/health`, {
          method: 'GET',
          signal: AbortSignal.timeout(4_000),
        });
        const data = (await res.json()) as Record<string, unknown>;
        return { ok: Boolean(res.ok && data.ok) };
      }
      case 'start': {
        const data = await postHandyJson(url, '/api/v1/stream/start', {}, 10_000);
        return { ok: true, streamId: String(data.stream_id || '') };
      }
      case 'chunk': {
        const data = await postHandyJson(
          url,
          '/api/v1/stream/chunk',
          {
            stream_id: params.streamId,
            pcm16_base64: params.pcm16Base64,
          },
          15_000,
        );
        return {
          ok: true,
          streamId: String(data.stream_id || params.streamId),
          text: String(data.text ?? ''),
        };
      }
      case 'finish': {
        const data = await postHandyJson(
          url,
          '/api/v1/stream/finish',
          {
            stream_id: params.streamId,
            pcm16_base64: params.pcm16Base64 ?? '',
          },
          60_000,
        );
        return {
          ok: true,
          streamId: String(data.stream_id || params.streamId),
          text: String(data.text ?? ''),
          durationSec: typeof data.duration_sec === 'number' ? data.duration_sec : undefined,
        };
      }
      case 'cancel': {
        await postHandyJson(
          url,
          '/api/v1/stream/cancel',
          { stream_id: params.streamId },
          8_000,
        );
        return { ok: true, streamId: params.streamId };
      }
      case 'transcribe': {
        const data = await postHandyJson(
          url,
          '/api/v1/transcribe',
          {
            audio_base64: params.audioBase64,
            filename: params.filename || 'voice.wav',
            source: 'happy',
          },
          60_000,
        );
        return {
          ok: true,
          text: String(data.text ?? ''),
          durationSec: typeof data.duration_sec === 'number' ? data.duration_sec : undefined,
        };
      }
      default:
        return { ok: false, error: 'Unsupported handy-stt action' };
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    logger.debug(`[handy-stt] RPC action failed: ${message}`);
    return { ok: false, error: message };
  }
}

export function registerHandySttRpc(rpcHandlerManager: RpcRegistrar, baseUrl?: string): void {
  rpcHandlerManager.registerHandler<HandySttRpcRequest, HandySttRpcResponse>(
    HANDY_STT_METHOD,
    async (params) => handleHandySttRpc(params, baseUrl),
  );
}
