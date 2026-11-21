/**
 * Agy AgentBackend Implementation
 *
 * Custom AgentBackend that drives the agy (Antigravity) CLI. Unlike the ACP-based
 * backends, agy has no streaming-event protocol — its only non-interactive surface
 * is `agy --print "<prompt>"`, which streams the final answer as plain text and
 * exits. So this backend spawns one `agy --print` process per turn and maps:
 *
 *   spawn            → { type: 'status', status: 'running' }
 *   stdout chunk     → { type: 'model-output', textDelta }
 *   exit code 0      → { type: 'status', status: 'idle' }
 *   exit code != 0   → { type: 'status', status: 'error', detail }
 *
 * There are no tool-call or permission events: print mode is one-shot and governed
 * by the CLI flags (see cliArgs.ts) plus agy's own settings.json.
 */

import { spawn, type ChildProcess } from 'node:child_process';
import type { PermissionMode } from '@/api/types';
import type {
  AgentBackend,
  AgentMessage,
  AgentMessageHandler,
  SessionId,
  StartSessionResult,
} from '@/agent/core/AgentBackend';
import { resolveAgyBin, AGY_PRINT_TIMEOUT, resolveAgyModelName } from './constants';
import { buildAgyArgs } from './cliArgs';
import { readAgyConversationId } from './conversationStore';

/** Signature of node's `spawn`, injectable so tests can supply a fake process. */
export type SpawnFn = typeof spawn;

const MAX_AGY_ERROR_DETAIL_CHARS = 2_000;

function cleanAgyErrorOutput(value: string): string {
  return value
    .replace(/\u001B\[[0-?]*[ -/]*[@-~]/g, '')
    .trim()
    .slice(-MAX_AGY_ERROR_DETAIL_CHARS);
}

export interface AgyBackendOptions {
  /** Working directory the agy process runs in (and the conversation cache key). */
  cwd: string;
  /** Initial permission mode; updated per turn from message meta. */
  permissionMode: PermissionMode;
  /** Initial model display name; updated per turn from message meta. */
  model?: string;
  /** Initial Happy effort key; used to resolve models with agy-specific variants. */
  effort?: string;
  /** Value for `--print-timeout`. Defaults to AGY_PRINT_TIMEOUT ("0" disables timeout). */
  printTimeout?: string;
  /** Output format passed to `agy --output-format` ('stream-json' emits live tool & text events). */
  outputFormat?: 'text' | 'stream-json';
  /** Optional logger. */
  log?: (msg: string) => void;
  /** Injectable spawn (defaults to node's child_process.spawn). */
  spawnFn?: SpawnFn;
  /** Optional override for resolving the resume conversation id (tests). */
  resolveConversationId?: (cwd: string) => string | null;
  /** Optional override for the agy binary/shim path. */
  agyBinOverride?: string;
  /** Optional per-session env overrides passed to spawned agy processes. */
  envOverrides?: Record<string, string>;
}

/** Parse an agy duration string ("10m", "30s", "1h", "0") to milliseconds; 0 disables watchdog. */
function parsePrintTimeoutMs(value: string): number {
  const trimmed = value.trim();
  if (trimmed === '0' || trimmed === '0s' || trimmed === '0m') {
    return 0;
  }
  const m = /^(\d+)\s*(s|m|h)$/.exec(trimmed);
  if (!m) return 10 * 60_000;
  const n = Number(m[1]);
  return m[2] === 'h' ? n * 3_600_000 : m[2] === 'm' ? n * 60_000 : n * 1_000;
}

export class AgyBackend implements AgentBackend {
  private readonly handlers = new Set<AgentMessageHandler>();
  private cwd: string;
  private readonly printTimeout: string;
  private readonly outputFormat: 'text' | 'stream-json';
  private readonly log: (msg: string) => void;
  private readonly spawnFn: SpawnFn;
  private readonly resolveConversationId: (cwd: string) => string | null;
  private agyBinOverride?: string;
  private envOverrides?: Record<string, string>;

  private permissionMode: PermissionMode;
  private model?: string;
  private effort?: string;
  private conversationId: string | null = null;
  private child: ChildProcess | null = null;

  constructor(opts: AgyBackendOptions) {
    this.cwd = opts.cwd;
    this.permissionMode = opts.permissionMode;
    this.model = opts.model;
    this.effort = opts.effort;
    this.printTimeout = opts.printTimeout ?? AGY_PRINT_TIMEOUT;
    this.outputFormat = opts.outputFormat ?? (opts.spawnFn ? 'text' : 'stream-json');
    this.log = opts.log ?? (() => {});
    this.spawnFn = opts.spawnFn ?? spawn;
    this.resolveConversationId = opts.resolveConversationId ?? readAgyConversationId;
    this.agyBinOverride = opts.agyBinOverride;
    this.envOverrides = opts.envOverrides;
  }

  /** Update the working directory applied to subsequent turns. */
  setCwd(cwd: string): void {
    this.cwd = cwd;
  }

  getCwd(): string {
    return this.cwd;
  }

  /** Reset pinned agy conversation id (e.g. when attaching/switching target session). */
  resetConversation(): void {
    this.conversationId = null;
  }

  /** Update per-session environment overrides passed to agy/shim. */
  setEnvOverrides(overrides: Record<string, string> | undefined): void {
    this.envOverrides = overrides;
  }

  /** Update the permission mode applied to subsequent turns. */
  setPermissionMode(mode: PermissionMode): void {
    this.permissionMode = mode;
  }

  /** Update the model applied to subsequent turns. */
  setModel(model: string | undefined): void {
    this.model = model;
  }

  /** Update the effort applied to subsequent turns. */
  setEffort(effort: string | undefined): void {
    this.effort = effort;
  }

  async startSession(): Promise<StartSessionResult> {
    // agy spawns lazily per prompt; there is nothing long-lived to start.
    // Deliberately do NOT seed from the cwd conversation cache: it holds whatever
    // conversation last ran in this cwd — possibly another live session's — so
    // seeding would cross-resume it. A fresh session starts a fresh conversation;
    // resuming a specific one across restarts needs explicit id plumbing (follow-up).
    return { sessionId: this.cwd };
  }

  async sendPrompt(
    _sessionId: SessionId,
    prompt: string,
    opts?: { addDirs?: readonly string[] },
  ): Promise<void> {
    const addDirs = Array.from(new Set([this.cwd, ...(opts?.addDirs ?? [])].filter(Boolean)));
    const args = buildAgyArgs({
      prompt,
      model: this.model ? resolveAgyModelName(this.model, this.effort) : undefined,
      conversationId: this.conversationId,
      permissionMode: this.permissionMode,
      addDirs,
      printTimeout: this.printTimeout,
      outputFormat: this.outputFormat === 'stream-json' ? 'stream-json' : undefined,
    });

    this.emit({ type: 'status', status: 'running' });

    // Until we have pinned our own conversation, snapshot the cwd cache so we can
    // tell after the turn whether the entry is ours (changed → our turn wrote it)
    // or a leftover from another session (unchanged → not ours to adopt).
    const preTurnCacheId =
      this.conversationId === null ? this.resolveConversationId(this.cwd) : null;

    await new Promise<void>((resolve, reject) => {
      const bin = this.agyBinOverride || resolveAgyBin();
      const childEnv = this.envOverrides ? { ...process.env, ...this.envOverrides } : process.env;
      const child = this.spawnFn(bin, args, {
        cwd: this.cwd,
        env: childEnv,
        windowsHide: true,
        // agy --print blocks until stdin reaches EOF. We never write stdin, so
        // give the child an empty stdin (immediate EOF) instead of an open pipe;
        // otherwise it hangs forever and the turn never completes.
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      this.child = child;

      // Node can fire both 'error' and 'close' on spawn failure; act on the first only.
      let settled = false;
      let stderrTail = '';
      const timeoutMs = parsePrintTimeoutMs(this.printTimeout);
      const watchdog = timeoutMs > 0
        ? setTimeout(() => {
            this.log(`agy turn exceeded ${this.printTimeout}; killing process`);
            child.kill('SIGKILL');
          }, timeoutMs + 30_000)
        : null;
      const cleanup = () => {
        if (watchdog) {
          clearTimeout(watchdog);
        }
        if (this.child === child) {
          this.child = null;
        }
      };

      let lineBuf = '';
      let emittedTextDelta = false;
      const startedTools = new Set<string>();

      const handleStreamLine = (rawLine: string, isFinalFlush = false) => {
        const trimmed = rawLine.trim();
        if (!trimmed) return;
        if (!trimmed.startsWith('{')) {
          const suffix = isFinalFlush ? '' : '\n';
          this.emit({ type: 'model-output', textDelta: `${rawLine}${suffix}` });
          emittedTextDelta = true;
          return;
        }
        try {
          const ev = JSON.parse(trimmed) as Record<string, unknown>;
          const eventType = ev.event;
          if (eventType === 'init' && typeof ev.conversation_id === 'string' && ev.conversation_id) {
            if (this.conversationId !== ev.conversation_id) {
              this.conversationId = ev.conversation_id;
              this.log(`pinned agy conversation from stream-json init: ${ev.conversation_id}`);
            }
            return;
          }
          if (eventType === 'step_update' && ev.step_update && typeof ev.step_update === 'object') {
            const su = ev.step_update as Record<string, unknown>;
            const stepType = String(su.step_type ?? '');
            const state = String(su.state ?? '');
            const stepIndex = su.step_index ?? 0;

            if (typeof su.thinking_delta === 'string' && su.thinking_delta) {
              this.emit({
                type: 'event',
                name: 'thinking',
                payload: { text: su.thinking_delta, streaming: true },
              });
            }

            if (stepType === 'tool') {
              const callId = `step-${String(stepIndex)}`;
              const toolInfo =
                su.tool_info && typeof su.tool_info === 'object'
                  ? (su.tool_info as Record<string, unknown>)
                  : undefined;
              const toolName = String(su.tool_name || toolInfo?.name || 'tool');
              if (state === 'ACTIVE' && !startedTools.has(callId)) {
                startedTools.add(callId);
                const rawParams =
                  toolInfo?.parameters && typeof toolInfo.parameters === 'object'
                    ? (toolInfo.parameters as Record<string, unknown>)
                    : {};
                this.emit({
                  type: 'tool-call',
                  callId,
                  toolName,
                  args: rawParams,
                });
              } else if (state === 'DONE' || state === 'ERROR') {
                if (!startedTools.has(callId)) {
                  startedTools.add(callId);
                  this.emit({
                    type: 'tool-call',
                    callId,
                    toolName,
                    args: {},
                  });
                }
                this.emit({
                  type: 'tool-result',
                  callId,
                  toolName,
                  result: toolInfo?.output ?? '',
                });
              }
              return;
            }

            if (stepType === 'agent_response' && typeof su.text_delta === 'string' && su.text_delta.length > 0) {
              emittedTextDelta = true;
              this.emit({ type: 'model-output', textDelta: su.text_delta });
              return;
            }
            return;
          }
          if (eventType === 'result' && ev.result && typeof ev.result === 'object') {
            const res = ev.result as Record<string, unknown>;
            if (typeof res.conversation_id === 'string' && res.conversation_id) {
              this.conversationId = res.conversation_id;
            }
            if (!emittedTextDelta && typeof res.response === 'string' && res.response.length > 0) {
              emittedTextDelta = true;
              this.emit({ type: 'model-output', textDelta: res.response });
            }
            return;
          }
        } catch {
          const suffix = isFinalFlush ? '' : '\n';
          this.emit({ type: 'model-output', textDelta: `${rawLine}${suffix}` });
          emittedTextDelta = true;
        }
      };

      child.stdout?.setEncoding('utf8');
      child.stdout?.on('data', (chunk: string) => {
        if (!chunk) return;
        if (this.outputFormat !== 'stream-json') {
          this.emit({ type: 'model-output', textDelta: chunk });
          return;
        }
        lineBuf += chunk;
        let nlIdx = lineBuf.indexOf('\n');
        while (nlIdx !== -1) {
          const line = lineBuf.slice(0, nlIdx).replace(/\r$/, '');
          lineBuf = lineBuf.slice(nlIdx + 1);
          handleStreamLine(line, false);
          nlIdx = lineBuf.indexOf('\n');
        }
      });

      child.stderr?.setEncoding('utf8');
      child.stderr?.on('data', (chunk: string) => {
        const text = chunk.trimEnd();
        if (text) {
          this.log(`stderr: ${text}`);
        }
        stderrTail = `${stderrTail}${chunk}`.slice(-MAX_AGY_ERROR_DETAIL_CHARS * 2);
      });

      child.on('error', (err: Error) => {
        if (settled) return;
        settled = true;
        cleanup();
        const detail = (err as NodeJS.ErrnoException).code === 'ENOENT'
          ? `agy executable not found. Install the Antigravity CLI, or set HAPPY_AGY_PATH to its absolute path (tried 'agy' on PATH and ~/.local/bin/agy).`
          : err.message;
        this.emit({ type: 'status', status: 'error', detail });
        reject(err);
      });

      child.on('close', (code: number | null) => {
        if (settled) return;
        settled = true;
        cleanup();
        if (this.outputFormat === 'stream-json' && lineBuf.trim().length > 0) {
          handleStreamLine(lineBuf.replace(/\r$/, ''), true);
          lineBuf = '';
        }
        // Fallback cache pinning if stream-json init did not provide conversation_id
        // (e.g. plain text outputFormat or older agy binary).
        if (this.conversationId === null) {
          const cid = this.resolveConversationId(this.cwd);
          if (cid && cid !== preTurnCacheId) {
            this.conversationId = cid;
            this.log(`pinned agy conversation ${cid}`);
          } else {
            this.log('agy conversation cache unchanged after turn; not adopting (will retry next turn)');
          }
        }

        if (code === 0) {
          this.emit({ type: 'status', status: 'idle' });
          resolve();
        } else {
          const stderrDetail = cleanAgyErrorOutput(stderrTail);
          const detail = stderrDetail
            ? `agy exited with code ${code ?? 'null'}: ${stderrDetail}`
            : `agy exited with code ${code ?? 'null'}`;
          this.emit({ type: 'status', status: 'error', detail });
          reject(new Error(detail));
        }
      });
    });
  }

  async cancel(_sessionId?: SessionId): Promise<void> {
    if (this.child) {
      this.child.kill('SIGTERM');
      this.child = null;
    }
  }

  onMessage(handler: AgentMessageHandler): void {
    this.handlers.add(handler);
  }

  offMessage(handler: AgentMessageHandler): void {
    this.handlers.delete(handler);
  }

  async dispose(): Promise<void> {
    await this.cancel();
    this.handlers.clear();
  }

  private emit(msg: AgentMessage): void {
    for (const handler of this.handlers) {
      handler(msg);
    }
  }
}
