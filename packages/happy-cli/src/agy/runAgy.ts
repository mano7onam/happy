/**
 * Agy Session Runner
 *
 * Entry point for agy (Antigravity CLI) agent sessions, following the runOpenClaw.ts
 * pattern. The daemon spawns this as:
 *   `node dist/index.mjs agy --happy-starting-mode remote --started-by daemon`
 *
 * agy is a plain-text streaming CLI (no ACP), so this drives an AgyBackend that
 * spawns `agy --print` per turn, and forwards its AgentMessage stream through the
 * same session pipeline used by the other backends.
 */

import { randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { basename, extname, join } from 'node:path';
import React from 'react';
import { render, type Instance as InkInstance } from 'ink';
import { ApiClient } from '@/api/api';
import type { ApiSessionClient } from '@/api/apiSession';
import { AcpSessionManager } from '@/agent/acp/AcpSessionManager';
import type { SessionEnvelope } from '@slopus/happy-wire';
import { logger } from '@/ui/logger';
import { MessageQueue2 } from '@/utils/MessageQueue2';
import { Credentials, readSettings } from '@/persistence';
import { initialMachineMetadata } from '@/daemon/run';
import { createSessionMetadata } from '@/utils/createSessionMetadata';
import { setupOfflineReconnection } from '@/utils/setupOfflineReconnection';
import { notifyDaemonSessionStarted } from '@/daemon/controlClient';
import { encodeBase64 } from '@/api/encryption';
import { registerKillSessionHandler } from '@/claude/registerKillSessionHandler';
import { connectionState } from '@/utils/serverConnectionErrors';
import { MessageBuffer } from '@/ui/ink/messageBuffer';
import { AgyDisplay } from '@/ui/ink/AgyDisplay';
import type { AgentMessage } from '@/agent/core';
import { normalizeRemotePermissionMode } from '@/claude/utils/permissionMode';
import {
  isAudioAttachment,
  registerHandySttRpc,
  transcribeAudioBufferWithHandy,
} from '@/handy/handySttBridge';
import { AgyBackend } from './AgyBackend';
import {
  DEFAULT_AGY_EFFORT,
  DEFAULT_AGY_MODEL,
  FALLBACK_AGY_MODEL_OPTIONS,
  discoverAgyModelOptions,
  normalizeAgyEffort,
  normalizeAgyModelAlias,
  pickDefaultAgyModel,
  resolveAgyModelName,
} from './constants';
import type { PermissionMode } from '@/api/types';

export interface RunAgyOptions {
  credentials: Credentials;
  startedBy?: 'daemon' | 'terminal';
  verbose?: boolean;
  model?: string;
  effort?: string;
  permissionMode?: PermissionMode;
}

export async function runAgy(opts: RunAgyOptions): Promise<void> {
  const verbose = opts.verbose === true;
  const sessionTag = randomUUID();
  connectionState.setBackend('agy');

  const log = (msg: string) => {
    logger.debug(`[agy] ${msg}`);
    if (verbose) {
      console.log(`[agy] ${msg}`);
    }
  };

  const api = await ApiClient.create(opts.credentials);
  const settings = await readSettings();
  if (!settings?.machineId) {
    throw new Error('No machine ID found in settings');
  }

  await api.getOrCreateMachine({
    machineId: settings.machineId,
    metadata: initialMachineMetadata,
  });

  const homeDir = os.homedir();
  const spyCliPath = join(homeDir, '.local', 'bin', 'agy-spy');
  const spyShimPath = join(homeDir, 'dev', 'agy-happy-spy', 'bin', 'agy-spy-shim');
  const spyConfigPath = join(homeDir, '.agy-happy-spy', 'config.json');
  const hasSpyToolkit = existsSync(spyCliPath) && existsSync(spyShimPath);

  let configuredDefaultModel: string | undefined;
  let configuredDefaultEffort: string | undefined;
  if (!opts.model && existsSync(spyConfigPath)) {
    try {
      const rawCfg = JSON.parse(readFileSync(spyConfigPath, 'utf8')) as { model?: string; effort?: string };
      configuredDefaultModel = rawCfg.model;
      configuredDefaultEffort = rawCfg.effort;
    } catch {
      // ignore invalid config.json
    }
  }

  let selectedModel = opts.model
    ? normalizeAgyModelAlias(opts.model)
    : configuredDefaultModel
      ? normalizeAgyModelAlias(configuredDefaultModel)
      : DEFAULT_AGY_MODEL;
  let selectedEffort = opts.effort
    ? normalizeAgyEffort(opts.effort)
    : configuredDefaultEffort
      ? normalizeAgyEffort(configuredDefaultEffort)
      : DEFAULT_AGY_EFFORT;
  let displayedModel = resolveAgyModelName(selectedModel, selectedEffort);
  // Started now, answered after the session is up: `agy models` can take seconds.
  const modelDiscovery = discoverAgyModelOptions();
  let availableModels = [...FALLBACK_AGY_MODEL_OPTIONS];

  const enableMobileAttachments = process.env.HAPPY_AGY_ATTACHMENTS !== '0';
  const attachmentsDir = join(homeDir, '.agy-happy-spy', 'attachments', sessionTag.slice(0, 8));

  // Shipped iOS/Android Happy clients gate composer photo/screenshot attachments on
  // `flavor == "claude" || flavor == "codex"`, while still rendering `metadata.models`
  // (Gemini 4 Neon/Carbon/Argon/Hydrogen, Orchestrator, Flash) and the effort picker.
  const { state, metadata } = createSessionMetadata({
    flavor: enableMobileAttachments ? 'claude' : 'agy',
    machineId: settings.machineId,
    startedBy: opts.startedBy,
  });
  metadata.models = availableModels;
  metadata.currentModelCode = selectedModel;
  const customSessionTitle = process.env.HAPPY_SESSION_TITLE?.trim();
  if (customSessionTitle) {
    metadata.name = customSessionTitle;
    metadata.summary = { text: customSessionTitle, updatedAt: Date.now() };
  }
  const response = await api.getOrCreateSession({ tag: sessionTag, metadata, state });
  if (response) {
    log(`Happy Session ID: ${response.id}`);
    const sessionIdOutPath = process.env.HAPPY_SESSION_ID_OUT?.trim();
    if (sessionIdOutPath) {
      try {
        writeFileSync(sessionIdOutPath, `${response.id}\n`, 'utf8');
      } catch (err) {
        logger.debug('[agy] Failed to write HAPPY_SESSION_ID_OUT:', err);
      }
    }
  }

  let session: ApiSessionClient;
  const { session: initialSession, reconnectionHandle } = setupOfflineReconnection({
    api,
    sessionTag,
    metadata,
    state,
    response,
    onSessionSwap: (newSession) => {
      session = newSession;
    },
  });
  session = initialSession;

  if (response) {
    try {
      await notifyDaemonSessionStarted(response.id, metadata, {
        encryptionKey: encodeBase64(response.encryptionKey),
        encryptionVariant: response.encryptionVariant,
        seq: response.seq,
        metadataVersion: response.metadataVersion,
        agentStateVersion: response.agentStateVersion,
      });
    } catch (error) {
      logger.debug('[agy] Failed to report session to daemon:', error);
    }
  }

  const sessionManager = new AcpSessionManager();
  const messageQueue = new MessageQueue2<Record<string, never>>(() => '');
  let shouldExit = false;
  let abortController = new AbortController();
  let thinking = false;
  let errorReportedForCurrentTurn = false;

  let activeFeedPath = process.env.HAPPY_SPY_FEED_JSONL?.trim() || '';
  let activeTargetSid = process.env.AGY_SPY_TARGET_SESSION?.trim() || '';
  let activeSpyMode = process.env.AGY_SPY_MODE?.trim() || (activeTargetSid ? 'companion' : 'hub');
  let activeStateFile = process.env.AGY_SPY_STATE_FILE?.trim() || '';
  let feedOffset = 0;

  const buildSpyEnvOverrides = (): Record<string, string> | undefined => {
    if (!hasSpyToolkit) return undefined;
    return {
      AGY_SPY_TARGET_SESSION: activeTargetSid,
      AGY_SPY_MODE: activeSpyMode,
      AGY_SPY_STATE_FILE: activeStateFile,
      AGY_SPY_FORCE_YOLO: '1',
      PYTHONPATH: [
        join(homeDir, 'dev', 'agy-happy-spy'),
        join(homeDir, 'dev', 'puenteo'),
        process.env.PYTHONPATH || '',
      ].filter(Boolean).join(':'),
    };
  };

  const initialPermissionMode: PermissionMode =
    (!opts.permissionMode || opts.permissionMode === 'default') && hasSpyToolkit
      ? 'bypassPermissions'
      : (opts.permissionMode ?? 'default');

  const backend = new AgyBackend({
    cwd: process.cwd(),
    permissionMode: initialPermissionMode,
    model: selectedModel,
    effort: selectedEffort,
    printTimeout: process.env.HAPPY_AGY_PRINT_TIMEOUT?.trim() || '0',
    outputFormat: 'stream-json',
    agyBinOverride: process.env.HAPPY_AGY_PATH?.trim() || (hasSpyToolkit ? spyShimPath : undefined),
    envOverrides: buildSpyEnvOverrides(),
    log,
  });

  const runSpyMobileCtl = (ctlArgs: string[]): Record<string, unknown> | null => {
    if (!hasSpyToolkit) return null;
    try {
      const out = execFileSync('python3', [spyCliPath, 'mobile-ctl', ...ctlArgs], {
        encoding: 'utf8',
        timeout: 25_000,
        stdio: ['ignore', 'pipe', 'pipe'],
        env: {
          ...process.env,
          PATH: `${join(homeDir, '.local', 'bin')}:/opt/homebrew/bin:/usr/local/bin:${process.env.PATH || '/usr/bin:/bin'}`,
        },
      });
      const lastLine = out.trim().split('\n').pop() || '';
      return lastLine ? (JSON.parse(lastLine) as Record<string, unknown>) : null;
    } catch (err) {
      logger.debug('[agy] mobile-ctl failed:', err);
      return null;
    }
  };

  // Terminal UI (only with a real TTY; the daemon runs headless).
  const messageBuffer = new MessageBuffer();
  const hasTTY = Boolean(process.stdout.isTTY && process.stdin.isTTY);
  let inkInstance: InkInstance | null = null;

  const sendEnvelopes = (envelopes: SessionEnvelope[]) => {
    for (const envelope of envelopes) {
      session.sendSessionProtocolMessage(envelope);
    }
  };

  const onBackendMessage = (msg: AgentMessage) => {
    if (verbose) {
      log(`Backend message: ${JSON.stringify(msg).slice(0, 200)}`);
    }

    if (msg.type === 'model-output' && msg.textDelta) {
      messageBuffer.addMessage(msg.textDelta, 'assistant');
    } else if (msg.type === 'status') {
      const nextThinking = msg.status === 'running';
      if (thinking !== nextThinking) {
        thinking = nextThinking;
        session.keepAlive(thinking, 'remote');
      }
      if (msg.status === 'error' && msg.detail) {
        messageBuffer.addMessage(`Error: ${msg.detail}`, 'status');
      }
    }

    const envelopes = sessionManager.mapMessage(msg);
    sendEnvelopes(envelopes);
    if (msg.type === 'status' && msg.status === 'error' && envelopes.length === 0) {
      session.sendSessionEvent({
        type: 'message',
        message: `Antigravity error: ${msg.detail?.trim() || 'The agent stopped because of an unknown error.'}`,
      });
    }
    if (msg.type === 'status' && msg.status === 'error') {
      errorReportedForCurrentTurn = true;
    }
  };

  backend.onMessage(onBackendMessage);

  let availableModelCodes = availableModels.map((item) => item.code);
  let lastRemoteModel: string | undefined = undefined;
  let lastRemoteEffort: string | null | undefined = undefined;

  const publishModelMetadata = () => {
    try {
      session.updateMetadata((curr) => ({
        ...curr,
        models: availableModels,
        currentModelCode: selectedModel,
      }));
    } catch {
      // Ignore metadata update race before first flush.
    }
  };

  const syncDisplayedModel = (nextModel: string, nextEffort?: string) => {
    selectedModel = normalizeAgyModelAlias(nextModel);
    if (nextEffort !== undefined) {
      selectedEffort = normalizeAgyEffort(nextEffort);
      backend.setEffort(selectedEffort);
    }
    backend.setModel(selectedModel);
    displayedModel = resolveAgyModelName(selectedModel, selectedEffort);
    if (hasTTY) {
      messageBuffer.addMessage(`[MODEL:${displayedModel}]`, 'system');
    }
    publishModelMetadata();
  };

  const DisplayComponent = () =>
    React.createElement(AgyDisplay, {
      messageBuffer,
      logPath: process.env.DEBUG ? logger.getLogPath() : undefined,
      currentModel: displayedModel,
      availableModels: availableModelCodes,
      onSelectModel: (chosen: string) => {
        syncDisplayedModel(chosen);
        messageBuffer.addMessage(`Switched model to ${displayedModel}`, 'status');
      },
      onExit: async () => {
        logger.debug('[agy] Exiting agent via Ctrl-C');
        shouldExit = true;
        await handleAbort();
      },
    });

  if (hasTTY) {
    inkInstance = render(React.createElement(DisplayComponent), {
      exitOnCtrlC: false,
      patchConsole: false,
    });
    messageBuffer.addMessage(`[MODEL:${displayedModel}]`, 'system');

    process.stdin.resume();
    if (process.stdin.isTTY) {
      process.stdin.setRawMode(true);
    }
    process.stdin.setEncoding('utf8');
  }

  // The default model is swapped when the account doesn't list it, even when
  // an app sent it: the app's own default is the same model, picked before
  // this list reached it. Only `--model` is left alone.
  const modelsReady = modelDiscovery.then((discovered) => {
    if (!discovered) return;
    availableModels = discovered;
    availableModelCodes = discovered.map((item) => item.code);
    const fallback = pickDefaultAgyModel(availableModelCodes);
    if (!opts.model && selectedModel === DEFAULT_AGY_MODEL && fallback !== selectedModel) {
      log(`${selectedModel} is not in agy models; defaulting to ${fallback}`);
      syncDisplayedModel(fallback);
    } else {
      publishModelMetadata();
    }
    inkInstance?.rerender(React.createElement(DisplayComponent));
  }).catch((error) => {
    logger.debug('[agy] Applying agy models failed:', error);
  });

  session.onFileEvent((fileEvent) => {
    const ev = fileEvent.content.data.ev;
    log(`File attachment event received: ${ev.name} (${ev.size} bytes)`);
    const downloadPromise = (async (): Promise<{ data: Uint8Array; mimeType: string; name: string } | null> => {
      try {
        const decrypted = await session.downloadAndDecryptAttachment(ev.ref);
        if (!decrypted) {
          log(`Failed to decrypt attachment: ${ev.name}`);
          return null;
        }
        return { data: decrypted, mimeType: ev.mimeType ?? 'image/png', name: ev.name || 'screenshot.png' };
      } catch (error) {
        logger.debug('[agy] Failed to download attachment:', error);
        return null;
      }
    })();
    session.trackAttachmentDownload(downloadPromise);
  });

  const extensionForMime = (mimeType: string, rawName: string): string => {
    const existing = extname(rawName);
    if (existing) return '';
    const m = mimeType.toLowerCase();
    if (m.includes('png')) return '.png';
    if (m.includes('webp')) return '.webp';
    if (m.includes('gif')) return '.gif';
    if (m.includes('heic')) return '.heic';
    if (m.includes('pdf')) return '.pdf';
    return '.jpg';
  };

  let userMessageChain = Promise.resolve();
  session.onUserMessage((message) => {
    userMessageChain = userMessageChain
      .then(async () => {
        const attachmentsForThisMessage = await session.drainAttachmentsForUserMessage();
        const rawUserText = message.content?.text ?? '';
        if (!rawUserText && attachmentsForThisMessage.length === 0) return;

    if (message.meta?.permissionMode) {
      const mode = normalizeRemotePermissionMode(message.meta.permissionMode);
      if (mode) {
        backend.setPermissionMode(mode);
      }
    }

    const incomingModel = message.meta?.hasOwnProperty('model') ? message.meta.model : undefined;
    const incomingEffort = message.meta?.hasOwnProperty('effort') ? message.meta.effort : undefined;
    // Ignore ambient Claude default slugs ("opus", "sonnet", "claude-opus-...") sent by the mobile
    // client before the user explicitly picks from `metadata.models` when `flavor` is set to 'claude'.
    const isExplicitAgyModel =
      !incomingModel ||
      availableModelCodes.includes(incomingModel) ||
      /^(Gemini |Orchestrator|Claude Opus 5\.5|Claude Sonnet 5\.5|GPT-OSS)/i.test(incomingModel);
    const effectiveIncomingModel = isExplicitAgyModel ? incomingModel : undefined;
    const remoteModelChanged = Boolean(effectiveIncomingModel && effectiveIncomingModel !== lastRemoteModel);
    const remoteEffortChanged = incomingEffort !== undefined && incomingEffort !== lastRemoteEffort;

    if (effectiveIncomingModel !== undefined) {
      lastRemoteModel = effectiveIncomingModel ?? undefined;
    }
    if (incomingEffort !== undefined) {
      lastRemoteEffort = incomingEffort;
    }

    if (remoteModelChanged || remoteEffortChanged) {
      const nextModel = effectiveIncomingModel ? normalizeAgyModelAlias(effectiveIncomingModel) : selectedModel;
      const nextEffort = incomingEffort !== undefined ? normalizeAgyEffort(incomingEffort) : selectedEffort;
      syncDisplayedModel(nextModel, nextEffort);
    }

    const savedAttachmentPaths: string[] = [];
    const transcribedAudioTexts: string[] = [];
    if (attachmentsForThisMessage.length > 0) {
      try {
        mkdirSync(attachmentsDir, { recursive: true });
        for (let idx = 0; idx < attachmentsForThisMessage.length; idx += 1) {
          const att = attachmentsForThisMessage[idx];
          if (isAudioAttachment(att.name, att.mimeType)) {
            try {
              const sttText = await transcribeAudioBufferWithHandy(att.data, att.name || 'voice.m4a');
              if (sttText) {
                transcribedAudioTexts.push(sttText);
                emitSyntheticAgentCard(`🎙️ **Распознано (Handy Antigravity):**\n> ${sttText.replace(/\n/g, '\n> ')}`);
              }
            } catch (err) {
              logger.debug('[agy] Handy STT audio attachment transcription failed:', err);
            }
            continue;
          }
          const cleanBase = basename(att.name || `image-${idx + 1}`).replace(/[^a-zA-Z0-9._-]/g, '_');
          const ext = extensionForMime(att.mimeType, cleanBase);
          const filePath = join(attachmentsDir, `${Date.now()}-${idx + 1}-${cleanBase}${ext}`);
          writeFileSync(filePath, Buffer.from(att.data));
          savedAttachmentPaths.push(filePath);
        }
      } catch (err) {
        logger.debug('[agy] Failed to persist decrypted attachments:', err);
      }
    }

    const effectiveUserText = [rawUserText.trim(), ...transcribedAudioTexts].filter(Boolean).join('\n\n');
    const trimmedText = effectiveUserText.trim();
    if (!trimmedText && savedAttachmentPaths.length === 0) return;
    if (/^\/models$/i.test(trimmedText)) {
      const listText = availableModelCodes
        .map((code, idx) => `${idx + 1}. ${code}${code === selectedModel ? ` ✓ (current: ${displayedModel})` : ''}`)
        .join('\n');
      const reply = `Available Antigravity models:\n${listText}\n\nUse \`/model <number or name>\` (e.g. \`/model argon high\` or \`/model 1\`) or the model picker to switch.`;
      messageBuffer.addMessage(reply, 'status');
      session.sendSessionEvent({ type: 'message', message: reply });
      session.sendSessionEvent({ type: 'ready' });
      return;
    }

    const slashModelMatch = /^\/model\s+(.+)$/i.exec(trimmedText);
    if (slashModelMatch) {
      const rawArg = slashModelMatch[1].trim();
      const numericIndex = /^\d+$/.test(rawArg) ? Number(rawArg) - 1 : -1;
      if (numericIndex >= 0 && numericIndex < availableModelCodes.length) {
        syncDisplayedModel(availableModelCodes[numericIndex]);
      } else {
        const parts = rawArg.split(/\s+/);
        const maybeEffort = parts.length > 1 ? parts[parts.length - 1].toLowerCase() : '';
        if (['low', 'medium', 'high', 'xhigh'].includes(maybeEffort)) {
          syncDisplayedModel(parts.slice(0, -1).join(' '), maybeEffort);
        } else {
          syncDisplayedModel(rawArg);
        }
      }
      const confirmMsg = `Switched Antigravity model to: ${displayedModel}`;
      messageBuffer.addMessage(confirmMsg, 'status');
      session.sendSessionEvent({ type: 'message', message: confirmMsg });
      session.sendSessionEvent({ type: 'ready' });
      return;
    }

    if (hasSpyToolkit && /^\/(sessions|agy|list)$/i.test(trimmedText)) {
      const res = runSpyMobileCtl(['sessions', '--active', activeTargetSid]);
      const reply = typeof res?.text === 'string' ? res.text : 'Не удалось получить список сессий Antigravity.';
      emitSyntheticAgentCard(reply);
      return;
    }

    const spyAttachMatch = /^\/(spy|relay)(?:\s+(.*))?$/i.exec(trimmedText);
    if (hasSpyToolkit && spyAttachMatch) {
      const cmdKind = spyAttachMatch[1].toLowerCase() === 'relay' ? 'relay' : 'companion';
      const query = (spyAttachMatch[2] || 'current').trim() || 'current';
      const res = runSpyMobileCtl([
        'attach',
        '--query', query,
        '--mode', cmdKind,
        '--pid', String(process.pid),
        '--happy-id', response?.id || '',
        '--model', selectedModel,
        '--effort', selectedEffort,
      ]);
      if (res && res.ok) {
        activeTargetSid = String(res.target_session_id || '');
        activeSpyMode = String(res.mode || cmdKind);
        activeStateFile = String(res.state_path || activeStateFile);
        const nextFeed = String(res.feed_path || '');
        if (nextFeed && nextFeed !== activeFeedPath) {
          activeFeedPath = nextFeed;
          feedOffset = existsSync(activeFeedPath) ? readFileSync(activeFeedPath, 'utf8').length : 0;
        }
        const nextCwd = String(res.cwd || backend.getCwd());
        backend.setCwd(nextCwd);
        backend.resetConversation();
        backend.setEnvOverrides(buildSpyEnvOverrides());
        const nextTitle = String(res.title || '');
        if (nextTitle) {
          session.updateMetadata((curr) => ({
            ...curr,
            path: nextCwd,
            name: nextTitle,
            summary: { text: nextTitle, updatedAt: Date.now() },
          }));
        }
        emitSyntheticAgentCard(String(res.card || `Подключено к сессии ${activeTargetSid}`));
      } else {
        emitSyntheticAgentCard(`❌ ${String(res?.error || 'Не удалось подключиться к сессии Antigravity.')}`);
      }
      return;
    }

    if (hasSpyToolkit && /^\/(unspy|fresh)$/i.test(trimmedText)) {
      const res = runSpyMobileCtl([
        'detach',
        '--cwd', backend.getCwd(),
        '--pid', String(process.pid),
        '--happy-id', response?.id || '',
        '--model', selectedModel,
        '--effort', selectedEffort,
      ]);
      activeTargetSid = '';
      activeSpyMode = 'hub';
      activeFeedPath = '';
      if (res?.state_path) {
        activeStateFile = String(res.state_path);
      }
      backend.resetConversation();
      backend.setEnvOverrides(buildSpyEnvOverrides());
      const nextTitle = String(res?.title || '');
      if (nextTitle) {
        session.updateMetadata((curr) => ({
          ...curr,
          name: nextTitle,
          summary: { text: nextTitle, updatedAt: Date.now() },
        }));
      }
      emitSyntheticAgentCard(String(res?.card || '✅ Отвязался от десктопной сессии.'));
      return;
    }

    const newDesktopMatch = /^\/desktop\s+([\s\S]+)$/i.exec(trimmedText);
    if (hasSpyToolkit && newDesktopMatch) {
      const firstPrompt = newDesktopMatch[1].trim();
      emitSyntheticAgentCard('⏳ Открываю новую сессию прямо в приложении `Antigravity.app` на твоём Маке…');
      const res = runSpyMobileCtl([
        'new-desktop',
        '--query', firstPrompt,
        '--mode', 'companion',
        '--pid', String(process.pid),
        '--happy-id', response?.id || '',
        '--model', selectedModel,
        '--effort', selectedEffort,
      ]);
      if (res && res.ok) {
        activeTargetSid = String(res.target_session_id || '');
        activeSpyMode = String(res.mode || 'companion');
        activeStateFile = String(res.state_path || activeStateFile);
        activeFeedPath = String(res.feed_path || '');
        feedOffset = 0;
        const nextCwd = String(res.cwd || backend.getCwd());
        backend.setCwd(nextCwd);
        backend.resetConversation();
        backend.setEnvOverrides(buildSpyEnvOverrides());
        const nextTitle = String(res.title || '');
        if (nextTitle) {
          session.updateMetadata((curr) => ({
            ...curr,
            path: nextCwd,
            name: nextTitle,
            summary: { text: nextTitle, updatedAt: Date.now() },
          }));
        }
        emitSyntheticAgentCard(
          `✅ **Новая сессия открыта прямо в Antigravity Desktop на Маке!**\n\n${String(res.card || '')}`,
        );
      } else {
        emitSyntheticAgentCard(`❌ Не удалось открыть новую сессию в Antigravity Desktop: ${String(res?.error || 'unknown error')}`);
      }
      return;
    }

    let outgoingPrompt = effectiveUserText;
    if (savedAttachmentPaths.length > 0) {
      const filesList = savedAttachmentPaths.map((p) => `- ${p}`).join('\n');
      const header =
        `[Прикреплены скриншоты/изображения с телефона (${savedAttachmentPaths.length} шт.). ` +
        `Обязательно открой и посмотри каждый файл через инструмент \`read_file\` перед ответом:\n${filesList}]`;
      outgoingPrompt = effectiveUserText.trim()
        ? `${header}\n\n${effectiveUserText}`
        : `${header}\n\nПосмотри на прикреплённый скриншот и опиши, что видишь / помоги с этим.`;
    }

    messageBuffer.addMessage(outgoingPrompt, 'user');
    messageQueue.push(outgoingPrompt, {});
      })
      .catch((err) => {
        logger.debug('[agy] Error handling user message:', err);
      });
  });
  session.keepAlive(thinking, 'remote');

  const keepAliveInterval = setInterval(() => {
    session.keepAlive(thinking, 'remote');
  }, 2000);

  async function handleAbort() {
    log('Abort requested');
    try {
      await backend.cancel();
    } catch (error) {
      logger.debug('[agy] Abort failed:', error);
    }
    thinking = false;
    session.keepAlive(false, 'remote');
    abortController.abort();
    abortController = new AbortController();
  }

  session.rpcHandlerManager.registerHandler('abort', handleAbort);
  registerHandySttRpc(session.rpcHandlerManager);
  const requestGracefulShutdown = async () => {
    if (shouldExit) return;
    shouldExit = true;
    messageQueue.close();
    await handleAbort();
  };
  registerKillSessionHandler(session.rpcHandlerManager, requestGracefulShutdown);
  const onSigTerm = () => {
    void requestGracefulShutdown();
  };
  process.once('SIGTERM', onSigTerm);
  if (!hasTTY) {
    process.once('SIGINT', onSigTerm);
  }

  const emitSyntheticAgentCard = (text: string) => {
    const trimmed = text.trim();
    if (!trimmed) return;
    if (hasTTY) {
      messageBuffer.addMessage(trimmed, 'assistant');
    }
    sendEnvelopes(sessionManager.startTurn());
    sendEnvelopes(sessionManager.mapMessage({ type: 'model-output', textDelta: trimmed }));
    sendEnvelopes(sessionManager.endTurn('completed'));
    session.sendSessionEvent({ type: 'ready' });
  };

  const feedInterval = setInterval(() => {
    if (thinking || shouldExit || !activeFeedPath || !existsSync(activeFeedPath)) return;
    try {
      const content = readFileSync(activeFeedPath, 'utf8');
      if (content.length <= feedOffset) return;
      const slice = content.slice(feedOffset);
      const lastNl = slice.lastIndexOf('\n');
      if (lastNl === -1) return;
      feedOffset += lastNl + 1;
      const lines = slice.slice(0, lastNl).split('\n');
      for (const line of lines) {
        if (!line.trim()) continue;
        try {
          const payload = JSON.parse(line) as { text?: string; title?: string };
          if (payload.title) {
            session.updateMetadata((curr) => ({
              ...curr,
              name: payload.title,
              summary: { text: payload.title!, updatedAt: Date.now() },
            }));
          }
          if (payload.text) {
            emitSyntheticAgentCard(payload.text);
          }
        } catch {
          // ignore malformed line
        }
      }
    } catch {
      // ignore transient read errors
    }
  }, 1500);

  try {
    await backend.startSession();
    log('Backend ready');

    const initialNoteFile = process.env.HAPPY_SPY_INITIAL_NOTE_FILE?.trim();
    if (initialNoteFile && existsSync(initialNoteFile)) {
      try {
        const initialNote = readFileSync(initialNoteFile, 'utf8');
        emitSyntheticAgentCard(initialNote);
      } catch (err) {
        logger.debug('[agy] Failed to read HAPPY_SPY_INITIAL_NOTE_FILE:', err);
      }
    } else if (hasSpyToolkit && !activeTargetSid) {
      // Auto-initialize sessions created directly from the Happy mobile app (`+ -> Antigravity`)!
      const initRes = runSpyMobileCtl([
        'init',
        '--cwd', backend.getCwd(),
        '--pid', String(process.pid),
        '--happy-id', response?.id || '',
        '--model', selectedModel,
        '--effort', selectedEffort,
      ]);
      if (initRes && initRes.ok) {
        activeTargetSid = String(initRes.target_session_id || '');
        activeSpyMode = String(initRes.mode || 'hub');
        activeStateFile = String(initRes.state_path || '');
        activeFeedPath = String(initRes.feed_path || '');
        feedOffset = 0;
        const nextCwd = String(initRes.cwd || backend.getCwd());
        backend.setCwd(nextCwd);
        backend.setEnvOverrides(buildSpyEnvOverrides());
        const nextTitle = String(initRes.title || '');
        if (nextTitle) {
          session.updateMetadata((curr) => ({
            ...curr,
            path: nextCwd,
            name: nextTitle,
            summary: { text: nextTitle, updatedAt: Date.now() },
          }));
        }
        if (typeof initRes.card === 'string' && initRes.card.trim()) {
          emitSyntheticAgentCard(initRes.card);
        }
      }
    }

    while (!shouldExit) {
      const waitSignal = abortController.signal;
      const batch = await messageQueue.waitForMessagesAndGetAsString(waitSignal);
      if (!batch) {
        if (shouldExit) break;
        if (waitSignal.aborted) continue;
        break;
      }

      log(`Incoming prompt: ${batch.message.slice(0, 200)}`);
      errorReportedForCurrentTurn = false;
      // The first turn waits (at most agy models' timeout) so a default the
      // account lacks is swapped before it fails.
      await modelsReady;
      sendEnvelopes(sessionManager.startTurn());
      try {
        await backend.sendPrompt(
          backend.getCwd(),
          batch.message,
          existsSync(attachmentsDir) ? { addDirs: [attachmentsDir] } : undefined,
        );
        sendEnvelopes(sessionManager.endTurn('completed'));
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        if (!errorReportedForCurrentTurn) {
          session.sendSessionEvent({ type: 'message', message: `Antigravity error: ${msg}` });
          errorReportedForCurrentTurn = true;
        }
        log(`Turn ended: ${msg}`);
        sendEnvelopes(sessionManager.endTurn('failed'));
      }
      thinking = false;
      session.keepAlive(false, 'remote');
      session.sendSessionEvent({ type: 'ready' });
    }
  } finally {
    process.removeListener('SIGTERM', onSigTerm);
    process.removeListener('SIGINT', onSigTerm);
    if (feedInterval) {
      clearInterval(feedInterval);
    }
    clearInterval(keepAliveInterval);
    reconnectionHandle?.cancel();

    backend.offMessage(onBackendMessage);
    await backend.dispose();
    inkInstance?.unmount();

    try {
      session.updateMetadata((currentMetadata) => ({
        ...currentMetadata,
        lifecycleState: 'archived',
        lifecycleStateSince: Date.now(),
        archivedBy: 'cli',
        archiveReason: 'Session ended',
      }));
      session.sendSessionDeath();
      await session.flush();
      await session.close();
    } catch (error) {
      logger.debug('[agy] Session close failed:', error);
    }
  }
}
