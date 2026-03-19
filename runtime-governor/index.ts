import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import type { OpenClawPluginApi } from "openclaw/plugin-sdk";

type PolicyRow = {
  lane?: string;
  strict?: boolean;
  sidecar?: boolean;
};

type PolicyState = {
  baseline?: {
    subagents?: string;
  };
  routing_policy?: {
    coding?: PolicyRow;
    research?: PolicyRow;
    verification?: PolicyRow;
    override_always_wins?: boolean;
  };
};

type SessionEntry = {
  modelOverride?: string;
  providerOverride?: string;
  spawnedBy?: string;
};

type OpenClawConfig = {
  auth?: {
    profiles?: Record<string, { provider?: string }>;
  };
  models?: {
    providers?: Record<string, { models?: Array<{ id?: string }> }>;
    default?: string;
  };
  agents?: {
    defaults?: {
      subagents?: {
        model?: string;
      };
    };
  };
};

type AuditRow = {
  ts: string;
  kind: string;
  sessionKey?: string;
  sessionId?: string;
  agentId?: string;
  runId?: string;
  taskType?: "coding" | "research" | "verification";
  routeMode?: "primary" | "subagent";
  requestedLane?: string;
  selectedLane?: string;
  provider?: string;
  model?: string;
  strict?: boolean;
  sidecar?: boolean;
  outcome?: string;
  reason?: string;
  explicitOverride?: boolean;
  toolName?: string;
};

const DEFAULT_STATE_PATH = path.join(os.homedir(), ".openclaw", "codexbar-state.json");
const DEFAULT_CONFIG_PATH = path.join(os.homedir(), ".openclaw", "openclaw.json");
const DEFAULT_AUDIT_PATH = path.join(os.homedir(), ".openclaw", "runtime-governor-audit.jsonl");
const KEYWORD_GROUPS = {
  research: [
    "research",
    "look up",
    "lookup",
    "search",
    "investigate",
    "compare",
    "docs",
    "documentation",
    "find sources",
    "deep research",
    "summarize sources",
  ],
  verification: [
    "verify",
    "verification",
    "review",
    "audit",
    "test",
    "tests",
    "regression",
    "validate",
    "lint",
    "check logs",
    "confirm",
  ],
  coding: [
    "code",
    "coding",
    "implement",
    "fix",
    "patch",
    "refactor",
    "build",
    "write",
    "edit",
    "router",
    "plugin",
    "function",
    "script",
  ],
} as const;

function readJsonFile<T>(filePath: string): T | null {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8")) as T;
  } catch {
    return null;
  }
}

function appendJsonLine(filePath: string, row: AuditRow): void {
  try {
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.appendFileSync(filePath, `${JSON.stringify(row)}\n`, "utf8");
  } catch {
    // Never break the agent path on audit write failure.
  }
}

function expandHome(input: string): string {
  if (!input.startsWith("~/")) {
    return input;
  }
  return path.join(os.homedir(), input.slice(2));
}

function resolveStatePath(api: OpenClawPluginApi): string {
  const configured = typeof api.pluginConfig?.statePath === "string" ? api.pluginConfig.statePath.trim() : "";
  return expandHome(configured || DEFAULT_STATE_PATH);
}

function resolveAuditPath(api: OpenClawPluginApi): string {
  const configured = typeof api.pluginConfig?.auditPath === "string" ? api.pluginConfig.auditPath.trim() : "";
  return expandHome(configured || DEFAULT_AUDIT_PATH);
}

function loadPolicyState(api: OpenClawPluginApi): PolicyState {
  return readJsonFile<PolicyState>(resolveStatePath(api)) || {};
}

function loadPolicy(api: OpenClawPluginApi): NonNullable<PolicyState["routing_policy"]> {
  return loadPolicyState(api).routing_policy || {};
}

function resolveSessionStorePath(agentId: string): string {
  return path.join(os.homedir(), ".openclaw", "agents", agentId, "sessions", "sessions.json");
}

function loadSessionEntry(agentId?: string, sessionKey?: string): SessionEntry | null {
  if (!agentId || !sessionKey) {
    return null;
  }
  const store = readJsonFile<Record<string, SessionEntry>>(resolveSessionStorePath(agentId));
  if (!store) {
    return null;
  }
  return store[sessionKey] || null;
}

function hasExplicitUserOverride(entry: SessionEntry | null): boolean {
  return Boolean(entry?.modelOverride?.trim() || entry?.providerOverride?.trim());
}

function splitModelRef(modelRef: string): { provider: string; model: string } | null {
  const trimmed = String(modelRef || "").trim();
  if (!trimmed || !trimmed.includes("/")) {
    return null;
  }
  const slash = trimmed.indexOf("/");
  const provider = trimmed.slice(0, slash).trim();
  const model = trimmed.slice(slash + 1).trim();
  if (!provider || !model) {
    return null;
  }
  return { provider, model };
}

function includesAny(text: string, needles: readonly string[]): boolean {
  return needles.some((needle) => text.includes(needle));
}

function isSubagentSessionKey(sessionKey?: string): boolean {
  return String(sessionKey || "").includes(":subagent:");
}

function isCodexbarSmokeSession(sessionId?: string, sessionKey?: string): boolean {
  return String(sessionId || "").startsWith("codexbar-smoke-") || String(sessionKey || "").includes("codexbar-smoke-");
}

function classifyTask(prompt: string, agentId?: string, sessionKey?: string): "coding" | "research" | "verification" {
  const lowerPrompt = String(prompt || "").toLowerCase();
  const lowerAgent = String(agentId || "").toLowerCase();
  const lowerSession = String(sessionKey || "").toLowerCase();

  if (lowerAgent === "code") {
    return "coding";
  }
  if (lowerAgent === "simple") {
    return "research";
  }
  if (lowerAgent === "reasoning") {
    return "verification";
  }
  if (includesAny(lowerPrompt, KEYWORD_GROUPS.verification) || includesAny(lowerSession, ["verify", "review", "audit"])) {
    return "verification";
  }
  if (includesAny(lowerPrompt, KEYWORD_GROUPS.research) || includesAny(lowerSession, ["research"])) {
    return "research";
  }
  if (includesAny(lowerPrompt, KEYWORD_GROUPS.coding) || includesAny(lowerSession, ["subagent", "code"])) {
    return "coding";
  }
  return "coding";
}

function loadOpenClawConfig(): OpenClawConfig {
  return readJsonFile<OpenClawConfig>(DEFAULT_CONFIG_PATH) || {};
}

function configuredModelRefs(): Set<string> {
  const cfg = loadOpenClawConfig();
  const refs = new Set<string>();
  const providers = cfg.models?.providers || {};
  for (const [providerId, providerCfg] of Object.entries(providers)) {
    for (const row of providerCfg.models || []) {
      const modelId = String(row?.id || "").trim();
      if (providerId && modelId) {
        refs.add(`${providerId}/${modelId}`);
      }
    }
  }
  const defaultRef = String(cfg.models?.default || "").trim();
  if (defaultRef) {
    refs.add(defaultRef);
  }
  return refs;
}

function configuredProviderIds(): Set<string> {
  const cfg = loadOpenClawConfig();
  const providers = new Set<string>();
  for (const providerId of Object.keys(cfg.models?.providers || {})) {
    if (providerId) {
      providers.add(providerId);
    }
  }
  for (const profile of Object.values(cfg.auth?.profiles || {})) {
    const providerId = String(profile?.provider || "").trim();
    if (providerId) {
      providers.add(providerId);
    }
  }
  return providers;
}

function getConfiguredSubagentLane(api: OpenClawPluginApi): string {
  const state = loadPolicyState(api);
  const baselineLane = String(state.baseline?.subagents || "").trim();
  if (baselineLane) {
    return baselineLane;
  }
  const cfg = loadOpenClawConfig();
  return String(cfg.agents?.defaults?.subagents?.model || "").trim();
}

function laneIsAvailable(lane: string): boolean {
  const refs = configuredModelRefs();
  if (refs.has(lane)) {
    return true;
  }
  const split = splitModelRef(lane);
  if (split && configuredProviderIds().has(split.provider)) {
    return true;
  }
  return false;
}

function resolveLaneForTask(api: OpenClawPluginApi, taskType: "coding" | "research" | "verification", routeMode: "primary" | "subagent"): { lane: string; row: PolicyRow; source: string } | null {
  const policy = loadPolicy(api);
  const row = policy[taskType] || {};
  const policyLane = String(row.lane || "").trim();
  const subagentLane = getConfiguredSubagentLane(api);

  if (routeMode === "subagent") {
    if (row.sidecar && policyLane) {
      return { lane: policyLane, row, source: "policy-sidecar" };
    }
    if (subagentLane) {
      return { lane: subagentLane, row, source: "subagent-default" };
    }
    if (policyLane) {
      return { lane: policyLane, row, source: "policy-fallback" };
    }
    return null;
  }

  if (policyLane) {
    return { lane: policyLane, row, source: "policy-primary" };
  }
  return null;
}

function audit(api: OpenClawPluginApi, row: AuditRow): void {
  appendJsonLine(resolveAuditPath(api), row);
}

export default function register(api: OpenClawPluginApi) {
  api.on(
    "before_tool_call",
    (event, ctx) => {
      if (event.toolName !== "sessions_spawn") {
        return;
      }

      const explicitModel = String(event.params?.model || "").trim();
      if (explicitModel) {
        audit(api, {
          ts: new Date().toISOString(),
          kind: "subagent.spawn.request",
          sessionKey: ctx.sessionKey,
          sessionId: ctx.sessionId,
          agentId: ctx.agentId,
          runId: ctx.runId,
          toolName: event.toolName,
          selectedLane: explicitModel,
          explicitOverride: true,
          outcome: "explicit-model",
        });
        return;
      }

      const task = String(event.params?.task || "").trim();
      const taskType = classifyTask(task, ctx.agentId, ctx.sessionKey);
      const resolution = resolveLaneForTask(api, taskType, "subagent");
      if (!resolution) {
        return;
      }
      if (Boolean(resolution.row.strict) && !laneIsAvailable(resolution.lane)) {
        audit(api, {
          ts: new Date().toISOString(),
          kind: "route.blocked",
          sessionKey: ctx.sessionKey,
          sessionId: ctx.sessionId,
          agentId: ctx.agentId,
          runId: ctx.runId,
          taskType,
          routeMode: "subagent",
          requestedLane: resolution.lane,
          selectedLane: resolution.lane,
          strict: true,
          sidecar: Boolean(resolution.row.sidecar),
          reason: "lane-not-configured",
          outcome: "blocked",
          toolName: event.toolName,
        });
        return {
          block: true,
          blockReason: `[runtime-governor] strict ${taskType} sub-agent lane is unavailable: ${resolution.lane}`,
        };
      }
      audit(api, {
        ts: new Date().toISOString(),
        kind: "subagent.spawn.request",
        sessionKey: ctx.sessionKey,
        sessionId: ctx.sessionId,
        agentId: ctx.agentId,
        runId: ctx.runId,
        toolName: event.toolName,
        taskType,
        routeMode: "subagent",
        requestedLane: resolution.lane,
        selectedLane: resolution.lane,
        strict: Boolean(resolution.row.strict),
        sidecar: Boolean(resolution.row.sidecar),
        outcome: resolution.source,
      });
      return {
        params: {
          ...event.params,
          model: resolution.lane,
        },
      };
    },
    { priority: 1000 },
  );

  api.on(
    "before_model_resolve",
    (event, ctx) => {
      if (isCodexbarSmokeSession(ctx.sessionId, ctx.sessionKey)) {
        audit(api, {
          ts: new Date().toISOString(),
          kind: "route.resolve",
          sessionKey: ctx.sessionKey,
          sessionId: ctx.sessionId,
          agentId: ctx.agentId,
          taskType: "verification",
          explicitOverride: true,
          outcome: "codexbar-smoke-bypass",
        });
        return;
      }

      const sessionEntry = loadSessionEntry(ctx.agentId, ctx.sessionKey);
      if (hasExplicitUserOverride(sessionEntry)) {
        audit(api, {
          ts: new Date().toISOString(),
          kind: "route.resolve",
          sessionKey: ctx.sessionKey,
          sessionId: ctx.sessionId,
          agentId: ctx.agentId,
          taskType: classifyTask(event.prompt, ctx.agentId, ctx.sessionKey),
          explicitOverride: true,
          outcome: "session-override",
        });
        return;
      }

      const taskType = classifyTask(event.prompt, ctx.agentId, ctx.sessionKey);
      const routeMode: "primary" | "subagent" = isSubagentSessionKey(ctx.sessionKey) ? "subagent" : "primary";
      const resolution = resolveLaneForTask(api, taskType, routeMode);
      if (!resolution) {
        return;
      }
      if (Boolean(resolution.row.strict) && !laneIsAvailable(resolution.lane)) {
        audit(api, {
          ts: new Date().toISOString(),
          kind: "route.blocked",
          sessionKey: ctx.sessionKey,
          sessionId: ctx.sessionId,
          agentId: ctx.agentId,
          taskType,
          routeMode,
          requestedLane: resolution.lane,
          selectedLane: resolution.lane,
          strict: true,
          sidecar: Boolean(resolution.row.sidecar),
          reason: "lane-not-configured",
          outcome: "blocked",
        });
        return;
      }
      const resolved = splitModelRef(resolution.lane);
      if (!resolved) {
        return;
      }

      audit(api, {
        ts: new Date().toISOString(),
        kind: "route.resolve",
        sessionKey: ctx.sessionKey,
        sessionId: ctx.sessionId,
        agentId: ctx.agentId,
        taskType,
        routeMode,
        requestedLane: resolution.lane,
        selectedLane: resolution.lane,
        provider: resolved.provider,
        model: resolved.model,
        strict: Boolean(resolution.row.strict),
        sidecar: Boolean(resolution.row.sidecar),
        outcome: resolution.source,
      });

      return {
        providerOverride: resolved.provider,
        modelOverride: resolved.model,
      };
    },
    { priority: 1000 },
  );

  api.on("llm_output", (event, ctx) => {
    audit(api, {
      ts: new Date().toISOString(),
      kind: "route.actual",
      sessionKey: ctx.sessionKey,
      sessionId: ctx.sessionId,
      agentId: ctx.agentId,
      runId: event.runId,
      provider: event.provider,
      model: event.model,
      outcome: "ok",
    });
  });

  api.on("agent_end", (event, ctx) => {
    audit(api, {
      ts: new Date().toISOString(),
      kind: "route.end",
      sessionKey: ctx.sessionKey,
      sessionId: ctx.sessionId,
      agentId: ctx.agentId,
      outcome: event.success ? "ok" : "error",
      reason: event.error,
    });
  });
}
