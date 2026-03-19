#!/usr/bin/python3
"""
CodexBar Linux — OpenClaw LLM control panel + status monitor.
Tabbed glass popup, DBus SNI tray icon, daemon via systemd.

Modes:
  codexbar-linux.py tray              — GTK3 tabbed popup + SNI tray (default)
  codexbar-linux.py waybar            — JSON stdout for Waybar
  codexbar-linux.py status            — one-shot status dump
  codexbar-linux.py cost --provider codex|openclaw --format json
"""
import json
import os
import sys
import time
import subprocess
import threading
import shlex
import re
import uuid
import shutil
from datetime import datetime, timedelta
from pathlib import Path

OPENCLAW_CONFIG = Path.home() / ".openclaw" / "openclaw.json"
OPENCLAW_CREDENTIALS = Path.home() / ".openclaw" / "credentials" / "env"
CODEXBAR_DAEMONS = Path.home() / ".openclaw" / "codexbar-daemons.json"
CODEXBAR_STATE = Path.home() / ".openclaw" / "codexbar-state.json"
CODEXBAR_USAGE_LEDGER_CACHE = Path.home() / ".openclaw" / "codexbar-usage-ledger-cache.json"
RUNTIME_GOVERNOR_AUDIT = Path.home() / ".openclaw" / "runtime-governor-audit.jsonl"
CODEXBAR_EVENTS = Path.home() / ".openclaw" / "codexbar-events.jsonl"
CODEXBAR_RESTORE_POINTS = Path.home() / ".openclaw" / "restore-points"
CODEXBAR_NOTES = Path.home() / ".openclaw" / "workspace" / "OPENCLAW_BUILD_NOTES.md"
USER_SYSTEMD_DIR = Path.home() / ".config" / "systemd" / "user"
PROVIDER_ALIASES = {
    "nvidia": "nim",
}
VIEW_MODES = [
    ("◉", "Overview", "overview"),
    ("⌘", "Models", "models"),
    ("🔑", "Accounts", "accounts"),
    ("⇆", "Daemons", "daemons"),
    ("$", "Spend", "spend"),
    ("⛭", "Ops", "ops"),
]

ACCOUNT_ACTIONS = [
    {
        "status_key": "codex",
        "title": "Codex",
        "action": "login",
        "button": "Login",
        "description": "Native Codex session used by OpenClaw onboarding.",
    },
    {
        "status_key": "openai",
        "title": "OpenAI API",
        "action": "env",
        "env_key": "OPENAI_API_KEY",
        "button": "Set Key",
        "placeholder": "sk-proj-...",
        "description": "Optional API key for router and direct OpenAI fallback paths.",
    },
    {
        "status_key": "nim",
        "title": "NVIDIA NIM",
        "action": "env",
        "env_key": "NVIDIA_API_KEY",
        "button": "Set Key",
        "placeholder": "nvapi-...",
        "description": "Required for Nemotron and other NIM-backed fallback lanes.",
    },
    {
        "status_key": "gemini",
        "title": "Gemini",
        "action": "env",
        "env_key": "GEMINI_API_KEY",
        "button": "Set Key",
        "placeholder": "AIza...",
        "description": "Required for Gemini fallback and heartbeat lanes.",
    },
]

ROUTING_PROFILES = {
    "Balanced": {
        "default": "nim/nvidia/nemotron-3-super-120b-a12b",
        "fallbacks": [
            "nim/minimaxai/minimax-m2.5",
            "google/gemini-3.1-pro-preview",
            "ollama/nemotron-mini:latest",
        ],
        "agents": {
            "main": "nim/nvidia/nemotron-3-super-120b-a12b",
            "code": "nim/qwen/qwen3-coder-480b-a35b-instruct",
            "simple": "google/gemini-3-flash-preview",
            "reasoning": "nim/deepseek-ai/deepseek-v3.1",
            "creative": "nim/moonshotai/kimi-k2.5",
            "local": "ollama/nemotron-mini:latest",
        },
        "subagents": "nim/mistralai/mistral-small-4-119b-2603",
        "description": "Strong default lane with lighter fallbacks and local safety net.",
    },
    "Cheap": {
        "default": "google/gemini-3-flash-preview",
        "fallbacks": [
            "ollama/nemotron-mini:latest",
            "nim/minimaxai/minimax-m2.5",
            "nim/nvidia/nemotron-3-super-120b-a12b",
        ],
        "agents": {
            "main": "google/gemini-3-flash-preview",
            "code": "ollama/nemotron-mini:latest",
            "simple": "google/gemini-3-flash-preview",
            "reasoning": "nim/minimaxai/minimax-m2.5",
            "creative": "google/gemini-3-flash-preview",
            "local": "ollama/nemotron-mini:latest",
        },
        "subagents": "ollama/nemotron-mini:latest",
        "description": "Prefer quota and local lanes before premium remote models.",
    },
    "Best Coding": {
        "default": "openai-codex/gpt-5.4",
        "fallbacks": [
            "nim/qwen/qwen3-coder-480b-a35b-instruct",
            "nim/nvidia/nemotron-3-super-120b-a12b",
            "ollama/nemotron-mini:latest",
        ],
        "agents": {
            "main": "openai-codex/gpt-5.4",
            "code": "openai-codex/gpt-5.4",
            "simple": "google/gemini-3-flash-preview",
            "reasoning": "nim/deepseek-ai/deepseek-v3.1",
            "creative": "nim/moonshotai/kimi-k2.5",
            "local": "ollama/nemotron-mini:latest",
        },
        "subagents": "nim/qwen/qwen3-coder-480b-a35b-instruct",
        "description": "Bias orchestration and code paths toward Codex with strong coding fallbacks.",
    },
    "Local First": {
        "default": "ollama/nemotron-mini:latest",
        "fallbacks": [
            "nim/nvidia/nemotron-3-super-120b-a12b",
            "google/gemini-3-flash-preview",
            "openai-codex/gpt-5.4",
        ],
        "agents": {
            "main": "ollama/nemotron-mini:latest",
            "code": "ollama/nemotron-mini:latest",
            "simple": "ollama/nemotron-mini:latest",
            "reasoning": "nim/deepseek-ai/deepseek-v3.1",
            "creative": "nim/moonshotai/kimi-k2.5",
            "local": "ollama/nemotron-mini:latest",
        },
        "subagents": "ollama/nemotron-mini:latest",
        "description": "Keep as much work local as possible and burst remote only when needed.",
    },
    "Research Heavy": {
        "default": "nim/deepseek-ai/deepseek-v3.1",
        "fallbacks": [
            "google/gemini-3.1-pro-preview",
            "nim/nvidia/nemotron-3-super-120b-a12b",
            "openai-codex/gpt-5.4",
        ],
        "agents": {
            "main": "nim/deepseek-ai/deepseek-v3.1",
            "code": "nim/qwen/qwen3-coder-480b-a35b-instruct",
            "simple": "google/gemini-3-flash-preview",
            "reasoning": "nim/deepseek-ai/deepseek-v3.1",
            "creative": "google/gemini-3.1-pro-preview",
            "local": "ollama/nemotron-mini:latest",
        },
        "subagents": "google/gemini-3-flash-preview",
        "description": "Use stronger reasoning and research lanes with cheaper sidecar workers.",
    },
}

DOCKER_DAEMON_ID = "docker_nemoclaw"
LOCAL_DAEMON_ID = "local_openclaw"
DEFAULT_DAEMON_CONFIG = {
    "daemons": {
        LOCAL_DAEMON_ID: {
            "label": "Local OpenClaw",
            "type": "systemd",
            "scope": "user",
            "unit": "openclaw-gateway.service",
            "url": "http://127.0.0.1:18789",
            "description": "Your local OpenClaw gateway managed by user systemd.",
        },
        DOCKER_DAEMON_ID: {
            "label": "NemoClaw Gateway",
            "type": "command",
            "service": "nemoclaw-daemon.service",
            "url": "https://127.0.0.1:8080",
            "open_cmd": "x-terminal-emulator -e openshell term -g nemoclaw",
            "cookie_env": "NEMOCLAW_COOKIE",
            "start_cmd": "status_output=$(openshell -g nemoclaw status 2>/dev/null); [[ \"$status_output\" == *Connected* ]] || openshell -g nemoclaw gateway start",
            "stop_cmd": "openshell -g nemoclaw gateway stop",
            "status_cmd": "openshell -g nemoclaw status",
            "environment": {},
            "description": "NVIDIA NemoClaw gateway managed through OpenShell. Add environment overrides only if your Docker runtime needs them.",
        },
    }
}
DEFAULT_COOKIE_ENV_BY_DAEMON = {
    DOCKER_DAEMON_ID: "NEMOCLAW_COOKIE",
}

DEFAULT_CODEXBAR_STATE = {
    "baseline": {},
    "provider_tests": {},
    "usage_snapshot": {},
    "alert_state": {
        "down_signature": [],
        "ack_signature": [],
        "lastNotifiedAt": 0,
        "mutedUntil": 0,
    },
    "event_filter": "all",
    "scheduled_smoke_tests": {
        "enabled": True,
        "lastRunAt": "",
        "summary": {},
    },
    "routing_policy": {
        "coding": {
            "lane": "openai-codex/gpt-5.4",
            "strict": True,
            "sidecar": False,
        },
        "research": {
            "lane": "google/gemini-3.1-pro-preview",
            "strict": False,
            "sidecar": True,
        },
        "verification": {
            "lane": "nim/deepseek-ai/deepseek-v3.1",
            "strict": True,
            "sidecar": True,
        },
        "override_always_wins": True,
    },
    "pricing_registry": {
        "models": {}
    },
}

ROUTING_MUTATION_LOCK = threading.Lock()
RUNTIME_CACHE = {}
EVENT_RETENTION_MAX_LINES = 2000
EVENT_RETENTION_KEEP_LINES = 1200
ALERT_NOTIFY_COOLDOWN_SECONDS = 900


# ── Credentials ────────────────────────────────────────────────────────

def load_env_file(path):
    env = {}
    try:
        for line in open(path):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

def save_env_file(path, updates):
    """Update specific keys in the credentials env file."""
    path = os.path.expanduser(path)
    lines = []
    try:
        with open(path) as f:
            lines = f.readlines()
    except FileNotFoundError:
        pass
    updated_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            k, _, _ = stripped.partition("=")
            k = k.strip()
            if k in updates:
                new_lines.append(f"{k}={updates[k]}\n")
                updated_keys.add(k)
                continue
        new_lines.append(line)
    for k, v in updates.items():
        if k not in updated_keys:
            new_lines.append(f"{k}={v}\n")
    with open(path, "w") as f:
        f.writelines(new_lines)

def load_json_file(path, default=None):
    path = Path(path).expanduser()
    if default is None:
        default = {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return json.loads(json.dumps(default))

def save_json_file(path, data):
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")

def merge_dict(base, override):
    result = json.loads(json.dumps(base))
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_dict(result[key], value)
        else:
            result[key] = value
    return result

def default_cookie_env_for_daemon(daemon_id):
    return DEFAULT_COOKIE_ENV_BY_DAEMON.get(daemon_id, "")

ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

def strip_ansi(text):
    return ANSI_RE.sub("", str(text or ""))

def normalize_cookie_env_name(daemon_id, raw_value):
    default_name = default_cookie_env_for_daemon(daemon_id)
    if not default_name:
        return ""
    value = str(raw_value or "").strip()
    if value and value.replace("_", "").isalnum() and value.upper() == value and not value[0].isdigit():
        return value if value == default_name else default_name
    return default_name

def repair_daemon_cookie_binding(cfg):
    changed = False
    env_updates = {}
    env_file = load_env_file(str(OPENCLAW_CREDENTIALS))
    for daemon_id, daemon_cfg in (cfg.get("daemons") or {}).items():
        if not isinstance(daemon_cfg, dict):
            continue
        raw_cookie_env = daemon_cfg.get("cookie_env")
        normalized_env = normalize_cookie_env_name(daemon_id, raw_cookie_env)
        raw_cookie_env = str(raw_cookie_env or "").strip()
        if not normalized_env and raw_cookie_env:
            daemon_cfg.pop("cookie_env", None)
            changed = True
            continue
        if normalized_env != raw_cookie_env:
            if raw_cookie_env and raw_cookie_env in env_file:
                env_updates[normalized_env] = env_file.get(raw_cookie_env, "")
            daemon_cfg["cookie_env"] = normalized_env
            changed = True
    if env_updates:
        save_env_file(str(OPENCLAW_CREDENTIALS), env_updates)
    return changed

def load_daemon_config():
    file_exists = CODEXBAR_DAEMONS.exists()
    existing = load_json_file(CODEXBAR_DAEMONS, DEFAULT_DAEMON_CONFIG)
    merged = merge_dict(DEFAULT_DAEMON_CONFIG, existing)
    if repair_daemon_cookie_binding(merged):
        pass
    if (not file_exists) or merged != existing:
        save_json_file(CODEXBAR_DAEMONS, merged)
    return merged

def save_daemon_config(cfg):
    save_json_file(CODEXBAR_DAEMONS, cfg)


def load_codexbar_state():
    existing = load_json_file(CODEXBAR_STATE, DEFAULT_CODEXBAR_STATE)
    merged = merge_dict(DEFAULT_CODEXBAR_STATE, existing)
    if merged != existing:
        save_json_file(CODEXBAR_STATE, merged)
    return merged


def save_codexbar_state(data):
    save_json_file(CODEXBAR_STATE, data)
    invalidate_cache("usage:")


def get_alert_state():
    state = load_codexbar_state()
    return merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], state.get("alert_state") or {})


def save_alert_state(alert_state):
    state = load_codexbar_state()
    state["alert_state"] = merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], alert_state or {})
    save_codexbar_state(state)


def get_event_filter():
    state = load_codexbar_state()
    value = str(state.get("event_filter") or "all").strip().lower()
    return value or "all"


def save_event_filter(value):
    state = load_codexbar_state()
    state["event_filter"] = str(value or "all").strip().lower() or "all"
    save_codexbar_state(state)


def get_routing_policy():
    return load_codexbar_state().get("routing_policy") or merge_dict({}, DEFAULT_CODEXBAR_STATE["routing_policy"])


def save_routing_policy(policy):
    create_restore_point("save-routing-policy", include_credentials=False)
    state = load_codexbar_state()
    state["routing_policy"] = merge_dict(DEFAULT_CODEXBAR_STATE["routing_policy"], policy or {})
    save_codexbar_state(state)
    append_event("policy.save", "Routing policy updated", "good")


def get_pricing_registry():
    state = load_codexbar_state()
    registry = state.get("pricing_registry") or {}
    return merge_dict(DEFAULT_CODEXBAR_STATE["pricing_registry"], registry)


def save_pricing_registry(registry):
    create_restore_point("save-pricing-registry", include_credentials=False)
    state = load_codexbar_state()
    state["pricing_registry"] = merge_dict(DEFAULT_CODEXBAR_STATE["pricing_registry"], registry or {})
    save_codexbar_state(state)
    append_event("pricing.save", "Pricing registry updated", "good")

def build_usage_snapshot(summary):
    summary = summary or {}
    sessions = []
    for entry in (summary.get("sessions") or [])[:8]:
        sessions.append({
            "agentId": entry.get("agentId"),
            "sessionId": entry.get("sessionId"),
            "lastSeenLabel": entry.get("lastSeenLabel"),
            "modelRef": entry.get("modelRef"),
            "provider": entry.get("provider"),
            "billingMode": entry.get("billingMode"),
            "account": entry.get("account"),
            "totalTokens": int(entry.get("totalTokens") or 0),
            "totalCostUSD": float(entry.get("totalCostUSD") or 0.0),
            "errors": int(entry.get("errors") or 0),
            "active": bool(entry.get("active")),
        })
    recent_days = []
    for row in (summary.get("recent_days") or [])[:7]:
        recent_days.append({
            "label": row.get("label"),
            "tokens": int(row.get("tokens") or 0),
            "cost": float(row.get("cost") or 0.0),
            "quotaTokens": int(row.get("quotaTokens") or 0),
            "meteredTokens": int(row.get("meteredTokens") or 0),
            "localTokens": int(row.get("localTokens") or 0),
            "unknownTokens": int(row.get("unknownTokens") or 0),
            "providers": row.get("providers") or [],
        })
    return {
        "today": dict(summary.get("today") or {}),
        "week": dict(summary.get("week") or {}),
        "month": dict(summary.get("month") or {}),
        "projection": float(summary.get("projection") or 0.0),
        "recent_days": recent_days,
        "sessions": sessions,
        "entries": [],
        "daily": [],
        "snapshotAt": time.strftime("%H:%M:%S"),
        "snapshotEpoch": int(time.time()),
    }


def load_usage_snapshot():
    state = load_codexbar_state()
    return state.get("usage_snapshot") or {}


def save_usage_snapshot(summary):
    state = load_codexbar_state()
    state["usage_snapshot"] = build_usage_snapshot(summary)
    save_codexbar_state(state)


def usage_snapshot_is_stale(snapshot, max_age_seconds=600):
    try:
        snap_epoch = int((snapshot or {}).get("snapshotEpoch") or 0)
    except Exception:
        snap_epoch = 0
    if snap_epoch <= 0:
        return True
    return (time.time() - snap_epoch) > int(max_age_seconds)

def load_runtime_governor_audit(limit=40):
    limit = max(1, int(limit or 1))
    def builder():
        rows = []
        try:
            with RUNTIME_GOVERNOR_AUDIT.open() as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
        except FileNotFoundError:
            return []
        return rows[-limit:]
    return get_cached_value(f"audit:runtime-governor:{limit}", 3.0, builder)


def get_runtime_governor_summary(limit=6):
    rows = list(reversed(load_runtime_governor_audit(limit)))
    summary = []
    for row in rows:
        kind = str(row.get("kind") or "").strip()
        if kind not in {"route.resolve", "route.blocked", "subagent.spawn.request", "route.actual"}:
            continue
        summary.append({
            "time": str(row.get("ts") or "").replace("T", " ").replace("Z", ""),
            "kind": kind,
            "agentId": str(row.get("agentId") or "unknown"),
            "taskType": str(row.get("taskType") or ""),
            "routeMode": str(row.get("routeMode") or ""),
            "lane": str(row.get("selectedLane") or ""),
            "provider": str(row.get("provider") or ""),
            "model": str(row.get("model") or ""),
            "outcome": str(row.get("outcome") or ""),
            "reason": str(row.get("reason") or ""),
            "explicitOverride": bool(row.get("explicitOverride")),
        })
        if len(summary) >= limit:
            break
    return summary

def append_build_note(line):
    CODEXBAR_NOTES.parent.mkdir(parents=True, exist_ok=True)
    if not CODEXBAR_NOTES.exists():
        CODEXBAR_NOTES.write_text("# OpenClaw Build Notes\n\n")
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    with CODEXBAR_NOTES.open("a") as handle:
        handle.write(f"- {stamp} {line}\n")


def compact_event_log():
    try:
        if not CODEXBAR_EVENTS.exists():
            return
        lines = CODEXBAR_EVENTS.read_text().splitlines()
        if len(lines) <= EVENT_RETENTION_MAX_LINES:
            return
        trimmed = lines[-EVENT_RETENTION_KEEP_LINES:]
        CODEXBAR_EVENTS.write_text("\n".join(trimmed) + ("\n" if trimmed else ""))
    except Exception:
        return


def append_event(kind, message, tone="good", **meta):
    CODEXBAR_EVENTS.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "kind": str(kind or "").strip() or "event",
        "message": str(message or "").strip(),
        "tone": str(tone or "good").strip(),
    }
    for key, value in meta.items():
        row[key] = value
    compact_event_log()
    with CODEXBAR_EVENTS.open("a") as handle:
        handle.write(json.dumps(row) + "\n")
    invalidate_cache("events:")


def load_events(limit=40):
    limit = max(1, int(limit or 1))
    def builder():
        rows = []
        try:
            with CODEXBAR_EVENTS.open() as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        continue
        except FileNotFoundError:
            return []
        return rows[-limit:]
    return get_cached_value(f"events:{limit}", 2.0, builder)


def create_restore_point(reason, include_credentials=False):
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    restore_id = f"{stamp}-{uuid.uuid4().hex[:6]}"
    target = CODEXBAR_RESTORE_POINTS / restore_id
    target.mkdir(parents=True, exist_ok=True)
    files = [
        ("openclaw.json", OPENCLAW_CONFIG),
        ("codexbar-state.json", CODEXBAR_STATE),
        ("codexbar-daemons.json", CODEXBAR_DAEMONS),
    ]
    if include_credentials:
        files.append(("credentials-env", OPENCLAW_CREDENTIALS))
    copied = []
    for name, src in files:
        try:
            if Path(src).exists():
                shutil.copy2(src, target / name)
                copied.append(name)
        except Exception:
            continue
    metadata = {
        "id": restore_id,
        "createdAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "reason": str(reason or "manual").strip(),
        "includeCredentials": bool(include_credentials),
        "files": copied,
    }
    (target / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    append_event("restore-point", f"Restore point created: {metadata['reason']}", "good", restoreId=restore_id)
    return metadata


def list_restore_points(limit=20):
    rows = []
    try:
        for path in sorted(CODEXBAR_RESTORE_POINTS.glob("*/metadata.json"), reverse=True):
            try:
                rows.append(json.loads(path.read_text()))
            except Exception:
                continue
    except FileNotFoundError:
        return []
    return rows[:limit]


def restore_restore_point(restore_id):
    target = CODEXBAR_RESTORE_POINTS / str(restore_id or "").strip()
    metadata_path = target / "metadata.json"
    if not metadata_path.exists():
        return False, "Restore point not found."
    files = {
        "openclaw.json": OPENCLAW_CONFIG,
        "codexbar-state.json": CODEXBAR_STATE,
        "codexbar-daemons.json": CODEXBAR_DAEMONS,
        "credentials-env": OPENCLAW_CREDENTIALS,
    }
    for name, dest in files.items():
        src = target / name
        if src.exists():
            Path(dest).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
    invalidate_cache()
    append_event("restore-point.restore", f"Restored config from {restore_id}", "warn", restoreId=restore_id)
    return True, restore_id


def get_recent_incidents(limit=8):
    rows = []
    for row in reversed(load_events(limit * 3)):
        tone = str(row.get("tone") or "good")
        if tone in {"bad", "warn"} or str(row.get("kind") or "").startswith("smoke."):
            rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def event_matches_filter(row, filter_key):
    kind = str((row or {}).get("kind") or "").strip().lower()
    if filter_key == "all":
        return True
    if filter_key == "alerts":
        return kind.startswith("alert.") or str((row or {}).get("tone") or "") in {"bad", "warn"}
    if filter_key == "smoke":
        return kind.startswith("smoke.")
    if filter_key == "restore":
        return kind.startswith("restore-point")
    if filter_key == "daemon":
        return "daemon" in kind or "nemoclaw" in str((row or {}).get("message") or "").lower() or "openclaw" in str((row or {}).get("message") or "").lower()
    if filter_key == "policy":
        return kind.startswith("policy.") or kind.startswith("routing.") or kind.startswith("baseline.")
    return True


def get_filtered_events(limit, filter_key):
    rows = []
    for row in reversed(load_events(limit * 5)):
        if event_matches_filter(row, filter_key):
            rows.append(row)
        if len(rows) >= limit:
            break
    return rows


def parse_command_daemon_status(label, text):
    clean = strip_ansi(text or "").strip()
    if not clean:
        return {
            "active": False,
            "detail": f"{label} — status probe returned no output",
            "statusText": "",
            "server": "",
            "version": "",
        }
    info = {"statusText": "", "server": "", "version": ""}
    for raw_line in clean.splitlines():
        line = raw_line.strip()
        if not line or ":" not in line:
            continue
        key, value = [part.strip() for part in line.split(":", 1)]
        key_lower = key.lower()
        if key_lower == "status":
            info["statusText"] = value
        elif key_lower == "server":
            info["server"] = value
        elif key_lower == "version":
            info["version"] = value
    status_text = info.get("statusText") or ""
    active = any(token in status_text.lower() for token in ("connected", "running", "ready", "online"))
    pieces = [label]
    if status_text:
        pieces.append(status_text)
    if info.get("server"):
        pieces.append(info["server"])
    if info.get("version"):
        pieces.append(f"v{info['version']}")
    return {
        "active": active,
        "detail": " — ".join(pieces),
        **info,
    }

def shell_command(cmd):
    return ["/bin/bash", "-lc", cmd]

def get_openclaw_bin():
    preferred = Path.home() / ".nvm" / "versions" / "node" / "v22.22.1" / "bin" / "openclaw"
    if preferred.exists():
        return str(preferred)
    return "openclaw"

def get_node_bin_dir():
    preferred = Path.home() / ".nvm" / "versions" / "node" / "v22.22.1" / "bin"
    if preferred.exists():
        return str(preferred)
    return ""

def run_command_capture(cmd, timeout=6):
    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()
    except Exception as exc:
        return 1, "", str(exc)


def get_cached_value(key, ttl_seconds, builder):
    now = time.time()
    cached = RUNTIME_CACHE.get(key)
    if cached and (now - cached.get("ts", 0.0)) < float(ttl_seconds):
        return cached.get("value")
    value = builder()
    RUNTIME_CACHE[key] = {"ts": now, "value": value}
    return value


def invalidate_cache(*prefixes):
    if not prefixes:
        RUNTIME_CACHE.clear()
        return
    drop = []
    for key in RUNTIME_CACHE:
        if any(str(key).startswith(prefix) for prefix in prefixes):
            drop.append(key)
    for key in drop:
        RUNTIME_CACHE.pop(key, None)

def daemon_unit_path(service_name):
    return USER_SYSTEMD_DIR / service_name

def write_command_daemon_unit(daemon_cfg):
    service_name = str(daemon_cfg.get("service") or "").strip()
    start_cmd = str(daemon_cfg.get("start_cmd") or "").strip()
    if not service_name or not start_cmd:
        return False
    stop_cmd = str(daemon_cfg.get("stop_cmd") or "").strip()
    lines = [
        "[Unit]",
        f"Description={daemon_cfg.get('label', 'Command daemon')} — managed by CodexBar",
        "After=network-online.target",
        "",
        "[Service]",
        "Type=oneshot",
        "RemainAfterExit=yes",
        f"EnvironmentFile={OPENCLAW_CREDENTIALS}",
    ]
    for key, value in (daemon_cfg.get("environment") or {}).items():
        lines.append(f"Environment={key}={value}")
    lines.append(f"ExecStart=/bin/bash -lc {shlex.quote(start_cmd)}")
    if stop_cmd:
        lines.append(f"ExecStop=/bin/bash -lc {shlex.quote(stop_cmd)}")
    lines.extend([
        "",
        "[Install]",
        "WantedBy=default.target",
        "",
    ])
    unit_path = daemon_unit_path(service_name)
    unit_path.parent.mkdir(parents=True, exist_ok=True)
    unit_path.write_text("\n".join(lines))
    run_command_capture(["systemctl", "--user", "daemon-reload"], timeout=12)
    return True

def control_daemon(daemon_id, action):
    daemons = load_daemon_config().get("daemons", {})
    daemon_cfg = daemons.get(daemon_id, {})
    if daemon_cfg.get("type") == "systemd":
        unit = str(daemon_cfg.get("unit") or "").strip()
        if not unit:
            return False, "No systemd unit configured."
        run_command_capture(["systemctl", "--user", "daemon-reload"], timeout=12)
        code, out, err = run_command_capture(["systemctl", "--user", action, unit], timeout=20)
        return code == 0, out or err or unit
    service_name = str(daemon_cfg.get("service") or "").strip()
    if not service_name:
        return False, "No daemon service configured."
    if not write_command_daemon_unit(daemon_cfg):
        return False, "Set a launch command first."
    code, out, err = run_command_capture(["systemctl", "--user", action, service_name], timeout=30)
    return code == 0, out or err or service_name

def set_daemon_enabled(daemon_id, enabled):
    daemons = load_daemon_config().get("daemons", {})
    daemon_cfg = daemons.get(daemon_id, {})
    unit = str(daemon_cfg.get("unit") or daemon_cfg.get("service") or "").strip()
    if daemon_cfg.get("type") == "command":
        if not write_command_daemon_unit(daemon_cfg):
            return False, "Set a launch command first."
    if not unit:
        return False, "No unit configured."
    action = "enable" if enabled else "disable"
    code, out, err = run_command_capture(["systemctl", "--user", action, unit], timeout=20)
    return code == 0, out or err or unit

def get_daemon_status(daemon_id):
    daemons = load_daemon_config().get("daemons", {})
    daemon_cfg = daemons.get(daemon_id, {})
    label = daemon_cfg.get("label", daemon_id)
    cookie_env = str(daemon_cfg.get("cookie_env") or "").strip()
    cookie_value = load_env_file(str(OPENCLAW_CREDENTIALS)).get(cookie_env, "") if cookie_env else ""
    cookie_saved = bool(cookie_value)
    if daemon_cfg.get("type") == "systemd":
        unit = str(daemon_cfg.get("unit") or "").strip()
        if not unit:
            return {
                "label": "CFG",
                "detail": f"{label} — no unit configured",
                "ok": False,
                "needs_setup": True,
                "cookieSaved": cookie_saved,
                "cookieEnv": cookie_env,
            }
        code, out, _err = run_command_capture(["systemctl", "--user", "is-active", unit])
        enabled_code, enabled_out, _ = run_command_capture(["systemctl", "--user", "is-enabled", unit])
        enabled = enabled_code == 0 and enabled_out.strip() == "enabled"
        active = code == 0 and out.strip() == "active"
        return {
            "label": "RUN" if active else "OFF",
            "detail": f"{label} — {'running' if active else 'stopped'} · {'autostart on' if enabled else 'autostart off'}",
            "ok": active,
            "enabled": enabled,
            "unit": unit,
            "url": daemon_cfg.get("url", ""),
            "cookieSaved": cookie_saved,
            "cookieEnv": cookie_env,
        }
    start_cmd = str(daemon_cfg.get("start_cmd") or "").strip()
    service_name = str(daemon_cfg.get("service") or "").strip()
    if not start_cmd:
        return {
            "label": "SETUP",
            "detail": f"{label} — launch command not configured",
            "ok": False,
            "needs_setup": True,
            "enabled": False,
            "url": daemon_cfg.get("url", ""),
            "cookieSaved": cookie_saved,
            "cookieEnv": cookie_env,
        }
    write_command_daemon_unit(daemon_cfg)
    enabled_code, enabled_out, _ = run_command_capture(["systemctl", "--user", "is-enabled", service_name])
    enabled = enabled_code == 0 and enabled_out.strip() == "enabled"
    status_cmd = str(daemon_cfg.get("status_cmd") or "").strip()
    if status_cmd:
        code, out, err = run_command_capture(shell_command(status_cmd), timeout=8)
        parsed = parse_command_daemon_status(label, out or err or "")
        active = code == 0 and parsed.get("active", False)
        detail = parsed.get("detail") or f"{label} — status probe {'ok' if active else 'failed'}"
        return {
            "label": "RUN" if active else "OFF",
            "detail": detail,
            "ok": active,
            "enabled": enabled,
            "url": daemon_cfg.get("url", ""),
            "cookieSaved": cookie_saved,
            "cookieEnv": cookie_env,
            "statusText": parsed.get("statusText", ""),
            "server": parsed.get("server", ""),
            "version": parsed.get("version", ""),
        }
    code, out, _err = run_command_capture(["systemctl", "--user", "is-active", service_name])
    active = code == 0 and out.strip() == "active"
    return {
        "label": "RUN" if active else "OFF",
        "detail": f"{label} — {'running' if active else 'stopped'} · {'autostart on' if enabled else 'autostart off'}",
        "ok": active,
        "enabled": enabled,
        "url": daemon_cfg.get("url", ""),
        "cookieSaved": cookie_saved,
        "cookieEnv": cookie_env,
    }

def get_keys():
    fe = load_env_file(str(OPENCLAW_CREDENTIALS))
    return {
        "NVIDIA":  os.environ.get("NVIDIA_API_KEY",  fe.get("NVIDIA_API_KEY",  "")),
        "OPENAI":  os.environ.get("OPENAI_API_KEY",  fe.get("OPENAI_API_KEY",  "")),
        "GEMINI":  os.environ.get("GEMINI_API_KEY",  fe.get("GEMINI_API_KEY",  "")),
    }


# ── Provider fetchers ──────────────────────────────────────────────────

def get_nvidia_status(key):
    if not key or "REPLACE" in key:
        return {"label": "NIM:NOKEY", "detail": "NVIDIA NIM — no API key", "ok": False, "nokey": True}
    try:
        import requests
        r = requests.get("https://integrate.api.nvidia.com/v1/models",
                         headers={"Authorization": f"Bearer {key}"}, timeout=6)
        if r.status_code == 200:
            n = len(r.json().get("data", []))
            return {"label": "NIM:OK", "detail": f"NVIDIA NIM — {n} models", "ok": True, "model_count": n}
        return {"label": f"NIM:{r.status_code}", "detail": f"NVIDIA NIM — HTTP {r.status_code}", "ok": False}
    except Exception as e:
        return {"label": "NIM:ERR", "detail": f"NVIDIA NIM — {e}", "ok": False}

def get_openai_status(key):
    if not key or "REPLACE" in key:
        return {"label": "OAI:NOKEY", "detail": "OpenAI — no API key", "ok": False, "nokey": True}
    try:
        import requests
        r = requests.get("https://api.openai.com/v1/models",
                         headers={"Authorization": f"Bearer {key}"}, timeout=6)
        if r.status_code == 200:
            return {"label": "OAI:OK", "detail": "OpenAI API — key valid", "ok": True}
        return {"label": f"OAI:{r.status_code}", "detail": f"OpenAI — HTTP {r.status_code}", "ok": False}
    except Exception as e:
        return {"label": "OAI:ERR", "detail": f"OpenAI — {e}", "ok": False}


def get_gemini_status(key):
    if not key or "REPLACE" in key:
        return {"label": "GEM:NOKEY", "detail": "Gemini — no API key", "ok": False, "nokey": True}
    try:
        import requests
        r = requests.get(
            f"https://generativelanguage.googleapis.com/v1beta/models?key={key}", timeout=6)
        if r.status_code == 200:
            n = len(r.json().get("models", []))
            return {"label": "GEM:OK", "detail": f"Gemini — {n} models", "ok": True, "model_count": n}
        return {"label": f"GEM:{r.status_code}", "detail": f"Gemini — HTTP {r.status_code}", "ok": False}
    except Exception as e:
        return {"label": "GEM:ERR", "detail": f"Gemini — {e}", "ok": False}

def get_ollama_status():
    try:
        import requests
        r = requests.get("http://localhost:11434/api/tags", timeout=3)
        if r.status_code == 200:
            models = [m["name"] for m in r.json().get("models", [])]
            if models:
                return {"label": "L:ON", "detail": f"Ollama — {', '.join(models)}", "ok": True, "models": models}
            return {"label": "L:NOMODEL", "detail": "Ollama — running, no models", "ok": False}
    except Exception:
        pass
    return {"label": "L:OFF", "detail": "Ollama — not running", "ok": False}

def get_codex_status():
    auth_path = Path.home() / ".codex" / "auth.json"
    try:
        data = json.loads(auth_path.read_text())
        tokens = data.get("tokens") or {}
        has_token = bool(tokens.get("access_token") or data.get("OPENAI_API_KEY"))
        if not has_token:
            return {"label": "CDX:NOKEY", "detail": "Codex — not logged in", "ok": False, "nokey": True}
        model = data.get("model", "gpt-5.4")
        # Get session count from sqlite
        try:
            import sqlite3
            conn = sqlite3.connect(str(Path.home() / ".codex" / "state_5.sqlite"))
            count = conn.execute("SELECT COUNT(*), SUM(tokens_used) FROM threads").fetchone()
            conn.close()
            sessions = count[0] or 0
            tokens_used = count[1] or 0
            return {"label": "CDX:OK",
                    "detail": f"Codex — logged in · {sessions} sessions · {tokens_used:,} tokens",
                    "ok": True, "sessions": sessions, "tokens_used": tokens_used, "model": model}
        except Exception:
            return {"label": "CDX:OK", "detail": f"Codex — logged in ({model})", "ok": True}
    except FileNotFoundError:
        return {"label": "CDX:NOKEY", "detail": "Codex — not logged in", "ok": False, "nokey": True}
    except Exception as e:
        return {"label": "CDX:ERR", "detail": f"Codex — {e}", "ok": False}

def get_openclaw_status():
    service_active = False
    try:
        svc = subprocess.run(
            ["systemctl", "--user", "is-active", "openclaw-gateway.service"],
            capture_output=True, text=True, timeout=3)
        service_active = svc.stdout.strip() == "active"
    except Exception:
        service_active = False
    try:
        import requests
        r = requests.get("http://127.0.0.1:18789/overview", timeout=3, allow_redirects=True)
        if r.status_code in (200, 304):
            return {"label": "OC:OK", "detail": "OpenClaw gateway — running", "ok": True}
        return {"label": f"OC:{r.status_code}", "detail": f"OpenClaw — HTTP {r.status_code}", "ok": False}
    except Exception:
        if service_active:
            return {"label": "OC:WARM", "detail": "OpenClaw gateway — service active, HTTP warming up", "ok": True}
        return {"label": "OC:OFF", "detail": "OpenClaw gateway — not running", "ok": False}

def get_gpu_info():
    try:
        r = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.used,memory.total,utilization.gpu",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=3)
        if r.returncode == 0:
            used, total, util = [int(x.strip()) for x in r.stdout.strip().split(",")]
            return {"used": used, "total": total, "util": util, "ok": True}
    except Exception:
        pass
    return {"ok": False}

def get_router_enabled():
    try:
        r = subprocess.run(
            ["systemctl", "--user", "is-active", "openclaw-router.service"],
            capture_output=True, text=True, timeout=3)
        return r.stdout.strip() == "active"
    except Exception:
        return False

def load_openclaw_config():
    def builder():
        try:
            return json.loads(OPENCLAW_CONFIG.read_text())
        except Exception:
            return {}
    return get_cached_value("config:openclaw", 1.0, builder)

def write_openclaw_config(cfg):
    OPENCLAW_CONFIG.write_text(json.dumps(cfg, indent=2) + "\n")
    invalidate_cache("config:", "usage:")

def get_default_model_ref():
    cfg = load_openclaw_config()
    model = (((cfg.get("agents") or {}).get("defaults") or {}).get("model") or {})
    if isinstance(model, dict):
        primary = model.get("primary")
        if isinstance(primary, str) and primary.strip():
            return primary.strip()
    if isinstance(model, str) and model.strip():
        return model.strip()
    return "openai-codex/gpt-5.4"

def set_default_model_ref(model_ref: str):
    cfg = load_openclaw_config()
    agents = cfg.setdefault("agents", {})
    defaults = agents.setdefault("defaults", {})
    current_model = defaults.get("model")
    current_fallbacks = []
    if isinstance(current_model, dict):
        raw_fallbacks = current_model.get("fallbacks") or []
        if isinstance(raw_fallbacks, list):
            current_fallbacks = [str(v).strip() for v in raw_fallbacks if str(v).strip()]
    defaults["model"] = {
        "primary": model_ref,
        "fallbacks": current_fallbacks,
    }
    defaults_models = defaults.setdefault("models", {})
    defaults_models.setdefault(model_ref, {})

    agent_list = agents.setdefault("list", [])
    main_agent = None
    for agent in agent_list:
        if not isinstance(agent, dict):
            continue
        if agent.get("id") == "main" or agent.get("default") is True:
            main_agent = agent
            break
    if main_agent is None:
        main_agent = {"id": "main", "default": True}
        agent_list.insert(0, main_agent)
    main_model = main_agent.get("model")
    main_fallbacks = current_fallbacks
    if isinstance(main_model, dict):
        raw_fallbacks = main_model.get("fallbacks") or []
        if isinstance(raw_fallbacks, list):
            main_fallbacks = [str(v).strip() for v in raw_fallbacks if str(v).strip()]
    main_agent["model"] = {
        "primary": model_ref,
        "fallbacks": main_fallbacks,
    }
    write_openclaw_config(cfg)

def restart_openclaw_stack():
    subprocess.Popen(
        ["systemctl", "--user", "restart", "openclaw-gateway.service"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if get_router_enabled():
        subprocess.Popen(
            ["systemctl", "--user", "restart", "openclaw-router.service"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def set_router_enabled(val: bool):
    action = "start" if val else "stop"
    subprocess.Popen(
        ["systemctl", "--user", action, "openclaw-router.service"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def get_all_status():
    keys = get_keys()
    return {
        "nim":      get_nvidia_status(keys["NVIDIA"]),
        "openai":   get_openai_status(keys["OPENAI"]),
        "gemini":   get_gemini_status(keys["GEMINI"]),
        "ollama":   get_ollama_status(),
        "codex":    get_codex_status(),
        "openclaw": get_openclaw_status(),
        "daemons": {
            LOCAL_DAEMON_ID: get_daemon_status(LOCAL_DAEMON_ID),
            DOCKER_DAEMON_ID: get_daemon_status(DOCKER_DAEMON_ID),
        },
    }


def estimate_codex_cost(tokens):
    return float(tokens or 0) * 0.0000075


def parse_iso_timestamp(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def coerce_event_datetime(value):
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value) / 1000.0)
        except Exception:
            return None
    return parse_iso_timestamp(value)


def format_age_short(dt):
    if not isinstance(dt, datetime):
        return "unknown"
    delta = datetime.now(dt.tzinfo) - dt
    seconds = max(0, int(delta.total_seconds()))
    if seconds < 60:
        return f"{seconds}s"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h"
    days = hours // 24
    return f"{days}d"


def short_model_name(model_ref):
    value = str(model_ref or "").strip()
    if not value:
        return "unknown"
    parts = value.split("/")
    if len(parts) >= 2:
        return parts[-1]
    return value


def normalize_model_ref(provider, model):
    p = PROVIDER_ALIASES.get(str(provider or "").strip().lower(), str(provider or "").strip().lower())
    m = str(model or "").strip()
    if not p and not m:
        return ""
    if not m:
        return p
    if "/" in m and m.lower().startswith(f"{p}/"):
        return m.lower()
    if p:
        return f"{p}/{m}".lower()
    return m.lower()


def nonzero_cost_config(cost_cfg):
    if not isinstance(cost_cfg, dict):
        return False
    for key in ("input", "output", "cacheRead", "cacheWrite"):
        try:
            if float(cost_cfg.get(key) or 0) > 0:
                return True
        except Exception:
            continue
    return False


def normalize_cost_dict(cost_cfg):
    base = {"input": 0.0, "output": 0.0, "cacheRead": 0.0, "cacheWrite": 0.0}
    if not isinstance(cost_cfg, dict):
        return base
    for key in tuple(base.keys()):
        try:
            base[key] = float(cost_cfg.get(key) or 0.0)
        except Exception:
            base[key] = 0.0
    return base


def estimate_cost_from_usage(usage, cost_cfg):
    if not isinstance(usage, dict) or not nonzero_cost_config(cost_cfg):
        return 0.0
    total = 0.0
    for usage_key, price_key in (
        ("input", "input"),
        ("output", "output"),
        ("cacheRead", "cacheRead"),
        ("cacheWrite", "cacheWrite"),
    ):
        try:
            tokens = float(usage.get(usage_key) or 0)
            price = float((cost_cfg or {}).get(price_key) or 0)
        except Exception:
            tokens = 0.0
            price = 0.0
        total += (tokens / 1_000_000.0) * price
    return total


def load_auth_profile_index():
    profiles = {}
    base = Path.home() / ".openclaw" / "agents"
    for path in base.glob("*/agent/auth-profiles.json"):
        try:
            data = json.loads(path.read_text())
        except Exception:
            continue
        for profile_id, profile in ((data.get("profiles") or {}).items()):
            if not isinstance(profile, dict):
                continue
            provider = str(profile.get("provider") or "").strip().lower()
            if not provider:
                continue
            profiles.setdefault(provider, []).append({
                "agentId": path.parent.parent.name,
                "profileId": profile_id,
                "type": str(profile.get("type") or "").strip().lower(),
                "accountId": str(profile.get("accountId") or "").strip(),
            })
    return profiles


def account_label_for_provider(provider, auth_index):
    lookup = PROVIDER_ALIASES.get(str(provider or "").strip().lower(), str(provider or "").strip().lower())
    options = auth_index.get(lookup) or []
    if not options:
        return "none"
    preferred = options[0]
    if preferred.get("accountId"):
        return preferred["accountId"]
    return preferred.get("profileId") or "configured"


def billing_mode_for(provider, model_ref, auth_index, cost_cfg):
    provider = PROVIDER_ALIASES.get(str(provider or "").strip().lower(), str(provider or "").strip().lower())
    model_ref = str(model_ref or "").strip().lower()
    profile_types = {entry.get("type") for entry in (auth_index.get(provider) or [])}
    if provider == "ollama":
        return "local_unmetered"
    if provider == "router":
        return "unknown"
    if "oauth" in profile_types or provider == "openai-codex":
        return "oauth_quota"
    if nonzero_cost_config(cost_cfg):
        return "api_key_billable"
    if provider in {"nim", "gemini", "google", "openai", "anthropic"}:
        return "api_key_unpriced"
    if model_ref.startswith("ollama/"):
        return "local_unmetered"
    return "unknown"


def load_model_cost_catalog():
    cfg = load_openclaw_config()
    providers = ((cfg.get("models") or {}).get("providers") or {})
    catalog = {}
    for provider, entry in providers.items():
        if not isinstance(entry, dict):
            continue
        for model in entry.get("models") or []:
            if not isinstance(model, dict):
                continue
            model_id = str(model.get("id") or "").strip()
            if not model_id:
                continue
            catalog[f"{str(provider).strip().lower()}/{model_id.lower()}"] = {
                "provider": str(provider).strip().lower(),
                "name": str(model.get("name") or model_id),
                "cost": normalize_cost_dict(model.get("cost") or {}),
            }
    registry_models = (get_pricing_registry().get("models") or {})
    for model_ref, row in registry_models.items():
        ref = str(model_ref or "").strip().lower()
        if not ref:
            continue
        entry = catalog.setdefault(ref, {
            "provider": provider_for_model_ref(ref),
            "name": short_model_name(ref),
            "cost": normalize_cost_dict({}),
        })
        entry["cost"] = normalize_cost_dict(row)
        entry["pricingSource"] = str((row or {}).get("source") or "manual").strip() or "manual"
        entry["lastVerifiedAt"] = str((row or {}).get("lastVerifiedAt") or "").strip()
    return catalog


def safe_int(value):
    try:
        return int(value or 0)
    except Exception:
        return 0


def iter_session_paths():
    root = Path.home() / ".openclaw" / "agents"
    yield from sorted(root.glob("*/sessions/*.jsonl*"))


def serialize_dt(value):
    if not value:
        return ""
    try:
        return value.isoformat()
    except Exception:
        return ""


def deserialize_dt(value):
    try:
        text = str(value or "").strip()
        return datetime.fromisoformat(text) if text else None
    except Exception:
        return None


def load_usage_ledger_disk_cache():
    cache = load_json_file(CODEXBAR_USAGE_LEDGER_CACHE, {"version": 1, "files": {}})
    if not isinstance(cache, dict):
        return {"version": 1, "files": {}}
    if int(cache.get("version") or 0) != 1:
        return {"version": 1, "files": {}}
    files = cache.get("files")
    if not isinstance(files, dict):
        cache["files"] = {}
    return cache


def save_usage_ledger_disk_cache(cache):
    save_json_file(CODEXBAR_USAGE_LEDGER_CACHE, cache)


def parse_session_ledger_file(path, catalog, auth_index):
    agent_id = path.parts[-3]
    session_id = path.name.split(".jsonl", 1)[0]
    session_total_tokens = 0
    session_total_cost = 0.0
    last_event_at = None
    last_model_ref = ""
    last_provider = ""
    last_billing_mode = "unknown"
    session_errors = 0
    message_count = 0
    account_label = "none"
    entries = []

    try:
        lines = path.read_text().splitlines()
    except Exception:
        return {"entries": [], "session": None}

    for line in lines:
        try:
            event = json.loads(line)
        except Exception:
            continue
        if not isinstance(event, dict):
            continue
        event_dt = coerce_event_datetime(event.get("timestamp"))
        if event.get("type") != "message":
            continue
        message = event.get("message") or {}
        if not isinstance(message, dict):
            continue
        provider = str(message.get("provider") or "").strip().lower()
        normalized_provider = PROVIDER_ALIASES.get(provider, provider)
        model = str(message.get("model") or "").strip()
        model_ref = normalize_model_ref(normalized_provider, model)
        usage = message.get("usage") or {}
        if not isinstance(usage, dict):
            usage = {}
        usage_cost = usage.get("cost") or {}
        actual_cost = 0.0
        if isinstance(usage_cost, dict):
            try:
                actual_cost = float(usage_cost.get("total") or 0.0)
            except Exception:
                actual_cost = 0.0
        input_tokens = safe_int(usage.get("input"))
        output_tokens = safe_int(usage.get("output"))
        cache_read_tokens = safe_int(usage.get("cacheRead"))
        cache_write_tokens = safe_int(usage.get("cacheWrite"))
        token_total = safe_int(usage.get("totalTokens"))
        stop_reason = str(message.get("stopReason") or "").strip().lower()
        error_message = message.get("errorMessage")
        if (
            not normalized_provider
            and not model_ref
            and token_total <= 0
            and input_tokens <= 0
            and output_tokens <= 0
            and cache_read_tokens <= 0
            and cache_write_tokens <= 0
            and actual_cost <= 0
            and not error_message
        ):
            continue
        if (
            normalized_provider in {"router", "openclaw"}
            and token_total <= 0
            and input_tokens <= 0
            and output_tokens <= 0
            and cache_read_tokens <= 0
            and cache_write_tokens <= 0
            and actual_cost <= 0
            and not error_message
        ):
            continue
        cost_cfg = (catalog.get(model_ref) or {}).get("cost") or {}
        estimated_cost = actual_cost if actual_cost > 0 else estimate_cost_from_usage(usage, cost_cfg)
        billing_mode = billing_mode_for(normalized_provider, model_ref, auth_index, cost_cfg)
        account_label = account_label_for_provider(normalized_provider, auth_index)
        if stop_reason == "error" or error_message:
            session_errors += 1
        if event_dt is not None:
            last_event_at = event_dt
        last_model_ref = model_ref or last_model_ref
        last_provider = normalized_provider or last_provider
        last_billing_mode = billing_mode or last_billing_mode
        session_total_tokens += token_total
        session_total_cost += estimated_cost
        message_count += 1

        day_key = event_dt.strftime("%Y-%m-%d") if event_dt else "unknown"
        entries.append({
            "agentId": agent_id,
            "sessionId": session_id,
            "provider": normalized_provider,
            "modelRef": model_ref,
            "account": account_label,
            "billingMode": billing_mode,
            "timestamp": serialize_dt(event_dt),
            "date": day_key,
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "cacheReadTokens": cache_read_tokens,
            "cacheWriteTokens": cache_write_tokens,
            "totalTokens": token_total,
            "costUSD": estimated_cost,
            "estimated": actual_cost <= 0 and estimated_cost > 0,
            "path": str(path),
            "stopReason": stop_reason or "unknown",
        })

    session = None
    if message_count:
        session = {
            "agentId": agent_id,
            "sessionId": session_id,
            "path": str(path),
            "lastSeen": serialize_dt(last_event_at),
            "modelRef": last_model_ref,
            "provider": last_provider,
            "billingMode": last_billing_mode,
            "account": account_label,
            "totalTokens": session_total_tokens,
            "totalCostUSD": session_total_cost,
            "errors": session_errors,
            "messageCount": message_count,
        }
    return {"entries": entries, "session": session}


def inflate_cached_ledger_record(record):
    entries = []
    for entry in (record.get("entries") or []):
        row = dict(entry)
        row["timestamp"] = deserialize_dt(entry.get("timestamp"))
        entries.append(row)
    session = None
    if isinstance(record.get("session"), dict):
        session = dict(record["session"])
        session["lastSeen"] = deserialize_dt(session.get("lastSeen"))
        last_seen = session.get("lastSeen")
        session["lastSeenLabel"] = format_age_short(last_seen) + " ago" if last_seen else "unknown"
        session["active"] = bool(last_seen and (datetime.now(last_seen.tzinfo) - last_seen).total_seconds() < 6 * 3600)
    return entries, session


def build_usage_ledger():
    def builder():
        catalog = load_model_cost_catalog()
        auth_index = load_auth_profile_index()
        entries = []
        sessions = []
        daily = {}
        disk_cache = load_usage_ledger_disk_cache()
        cached_files = (disk_cache.get("files") or {})
        next_cached_files = {}

        for path in iter_session_paths():
            path_key = str(path)
            try:
                stat = path.stat()
            except Exception:
                continue
            fingerprint = {
                "size": int(stat.st_size),
                "mtime_ns": int(getattr(stat, "st_mtime_ns", int(stat.st_mtime * 1_000_000_000))),
            }
            cached = cached_files.get(path_key) if isinstance(cached_files, dict) else None
            if (
                isinstance(cached, dict)
                and int(cached.get("size") or -1) == fingerprint["size"]
                and int(cached.get("mtime_ns") or -1) == fingerprint["mtime_ns"]
            ):
                record = cached.get("record") or {}
            else:
                record = parse_session_ledger_file(path, catalog, auth_index)
            next_cached_files[path_key] = {
                **fingerprint,
                "record": record,
            }
            record_entries, record_session = inflate_cached_ledger_record(record)
            entries.extend(record_entries)
            if record_session:
                sessions.append(record_session)

        save_usage_ledger_disk_cache({
            "version": 1,
            "files": next_cached_files,
        })

        for entry in entries:
            event_dt = entry.get("timestamp")
            day_key = str(entry.get("date") or (event_dt.strftime("%Y-%m-%d") if event_dt else "unknown"))
            model_ref = str(entry.get("modelRef") or "").strip()
            normalized_provider = str(entry.get("provider") or "").strip()
            billing_mode = str(entry.get("billingMode") or "unknown").strip() or "unknown"
            input_tokens = int(entry.get("inputTokens") or 0)
            output_tokens = int(entry.get("outputTokens") or 0)
            cache_read_tokens = int(entry.get("cacheReadTokens") or 0)
            cache_write_tokens = int(entry.get("cacheWriteTokens") or 0)
            token_total = int(entry.get("totalTokens") or 0)
            estimated_cost = float(entry.get("costUSD") or 0.0)
            bucket = daily.setdefault(day_key, {
                "date": day_key,
                "inputTokens": 0,
                "outputTokens": 0,
                "cacheReadTokens": 0,
                "cacheWriteTokens": 0,
                "totalTokens": 0,
                "totalCost": 0.0,
                "modelsUsed": set(),
                "modelMap": {},
                "providers": set(),
                "billingModes": set(),
            })
            bucket["inputTokens"] += input_tokens
            bucket["outputTokens"] += output_tokens
            bucket["cacheReadTokens"] += cache_read_tokens
            bucket["cacheWriteTokens"] += cache_write_tokens
            bucket["totalTokens"] += token_total
            bucket["totalCost"] += estimated_cost
            if model_ref:
                bucket["modelsUsed"].add(model_ref)
                if normalized_provider:
                    bucket["providers"].add(normalized_provider)
                bucket["billingModes"].add(billing_mode)
                model_bucket = bucket["modelMap"].setdefault(model_ref, {
                    "modelName": model_ref,
                    "cost": 0.0,
                    "tokens": 0,
                    "count": 0,
                    "provider": normalized_provider,
                    "billingMode": billing_mode,
                })
                model_bucket["cost"] += estimated_cost
                model_bucket["tokens"] += token_total
                model_bucket["count"] += 1

        daily_rows = []
        for day_key in sorted(daily.keys()):
            row = daily[day_key]
            daily_rows.append({
                "date": row["date"],
                "inputTokens": row["inputTokens"],
                "outputTokens": row["outputTokens"],
                "cacheReadTokens": row["cacheReadTokens"],
                "cacheWriteTokens": row["cacheWriteTokens"],
                "totalTokens": row["totalTokens"],
                "totalCost": round(row["totalCost"], 6),
                "modelsUsed": sorted(row["modelsUsed"]),
                "providers": sorted(row["providers"]),
                "billingModes": sorted(row["billingModes"]),
                "modelBreakdowns": [
                    {
                        "modelName": item["modelName"],
                        "cost": round(item["cost"], 6),
                        "tokens": item["tokens"],
                        "count": item["count"],
                        "provider": item["provider"],
                        "billingMode": item["billingMode"],
                    }
                    for item in sorted(row["modelMap"].values(), key=lambda value: (-value["tokens"], value["modelName"]))
                ],
            })

        sessions.sort(key=lambda value: value["lastSeen"] or datetime.min, reverse=True)
        return {
            "entries": entries,
            "sessions": sessions,
            "daily": daily_rows,
            "authIndex": auth_index,
        }
    return get_cached_value("usage:ledger", 5.0, builder)


def format_tokens_short(value):
    value = int(value or 0)
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    return str(value)


def format_cost_short(value):
    return f"${float(value or 0):.2f}"


def format_usage_metric_value(metric):
    metric = metric or {}
    cost = float(metric.get("cost") or 0.0)
    quota_tokens = int(metric.get("quotaTokens") or 0)
    unknown_tokens = int(metric.get("unknownTokens") or 0)
    metered_tokens = int(metric.get("meteredTokens") or 0)
    local_tokens = int(metric.get("localTokens") or 0)
    if cost > 0:
        return format_cost_short(cost)
    if quota_tokens > 0:
        return "Quota"
    if metered_tokens > 0:
        return "Metered"
    if unknown_tokens > 0:
        return "Unknown"
    if local_tokens > 0:
        return "Local"
    return "$0.00"


def format_usage_metric_detail(title, metric):
    metric = metric or {}
    tokens = format_tokens_short(metric.get("tokens") or 0)
    extras = []
    if int(metric.get("quotaTokens") or 0) > 0:
        extras.append(f"quota {format_tokens_short(metric['quotaTokens'])}")
    if int(metric.get("meteredTokens") or 0) > 0:
        extras.append(f"metered {format_tokens_short(metric['meteredTokens'])}")
    if int(metric.get("unknownTokens") or 0) > 0:
        extras.append(f"unknown {format_tokens_short(metric['unknownTokens'])}")
    if int(metric.get("localTokens") or 0) > 0:
        extras.append(f"local {format_tokens_short(metric['localTokens'])}")
    if extras:
        return f"{title} · {tokens} tok · " + " · ".join(extras)
    return f"{title} · {tokens} tok"


def usage_mode_tone(session_entry):
    mode = str((session_entry or {}).get("billingMode") or "unknown")
    if mode == "api_key_billable":
        return "warn"
    if mode == "api_key_unpriced":
        return "warn"
    if mode == "oauth_quota":
        return "good"
    if mode == "local_unmetered":
        return "good"
    return "bad"


def usage_mode_label(value):
    mode = str(value or "unknown").strip()
    labels = {
        "api_key_billable": "billable",
        "api_key_unpriced": "metered",
        "oauth_quota": "quota",
        "local_unmetered": "local",
    }
    return labels.get(mode, mode.replace("_", " "))


def summarize_usage_metrics(entries):
    billable_cost = sum(float(entry.get("costUSD") or 0.0) for entry in entries if entry.get("billingMode") == "api_key_billable")
    quota_tokens = sum(int(entry.get("totalTokens") or 0) for entry in entries if entry.get("billingMode") == "oauth_quota")
    metered_tokens = sum(int(entry.get("totalTokens") or 0) for entry in entries if entry.get("billingMode") == "api_key_unpriced")
    local_tokens = sum(int(entry.get("totalTokens") or 0) for entry in entries if entry.get("billingMode") == "local_unmetered")
    unknown_tokens = sum(int(entry.get("totalTokens") or 0) for entry in entries if entry.get("billingMode") == "unknown")
    total_tokens = sum(int(entry.get("totalTokens") or 0) for entry in entries)
    return {
        "tokens": total_tokens,
        "cost": billable_cost,
        "quotaTokens": quota_tokens,
        "meteredTokens": metered_tokens,
        "localTokens": local_tokens,
        "unknownTokens": unknown_tokens,
    }


def get_usage_summary():
    def builder():
        ledger = build_usage_ledger()
        entries = ledger["entries"]
        today = datetime.now().date()
        gateway_cost = get_gateway_usage_cost(30) or {}
        gateway_daily = {
            str(row.get("date")): float(row.get("totalCost") or 0.0)
            for row in (gateway_cost.get("daily") or [])
            if isinstance(row, dict)
        }
        gateway_total_cost = float(((gateway_cost.get("totals") or {}).get("totalCost") or 0.0))

        entries_by_day = {}
        billable_active_dates = set()
        for entry in entries:
            timestamp = entry.get("timestamp")
            if not timestamp:
                continue
            day = timestamp.date()
            entries_by_day.setdefault(day, []).append(entry)
            if entry.get("billingMode") == "api_key_billable" and float(entry.get("costUSD") or 0.0) > 0:
                billable_active_dates.add(day)

        def window(days):
            metric_entries = []
            cost_total = 0.0
            for idx in range(days):
                day = today - timedelta(days=idx)
                metric_entries.extend(entries_by_day.get(day, []))
                cost_total += gateway_daily.get(day.strftime("%Y-%m-%d"), 0.0)
            metric = summarize_usage_metrics(metric_entries)
            if cost_total > 0:
                metric["cost"] = cost_total
            return metric

        today_roll = window(1)
        week_roll = window(7)
        month_roll = window(30)
        billable_days = max(1, len(billable_active_dates))
        if gateway_total_cost > 0:
            month_roll["cost"] = gateway_total_cost
        billable_days = max(1, len([row for row in (gateway_cost.get("daily") or []) if float(row.get("totalCost") or 0.0) > 0])) if gateway_total_cost > 0 else billable_days
        projected = (month_roll["cost"] / billable_days) * 30 if billable_days else 0.0
        recent_days = []
        daily_index = {row["date"]: row for row in ledger["daily"]}
        for idx in range(6, -1, -1):
            day = today - timedelta(days=idx)
            key = day.strftime("%Y-%m-%d")
            row = daily_index.get(key) or {}
            metrics = summarize_usage_metrics(entries_by_day.get(day, []))
            recent_days.append({
                "label": day.strftime("%a"),
                "tokens": metrics["tokens"],
                "cost": gateway_daily.get(key, metrics["cost"]),
                "quotaTokens": metrics["quotaTokens"],
                "meteredTokens": metrics["meteredTokens"],
                "localTokens": metrics["localTokens"],
                "unknownTokens": metrics["unknownTokens"],
                "providers": row.get("providers") or [],
            })
        return {
            "today": today_roll,
            "week": week_roll,
            "month": month_roll,
            "projection": projected,
            "recent_days": recent_days,
            "active_days": billable_days,
            "sessions": ledger["sessions"],
            "entries": entries,
            "daily": ledger["daily"],
        }
    return get_cached_value("usage:summary", 5.0, builder)


def summarize_rollups(entries, key_name):
    buckets = {}
    for entry in entries:
        key = str(entry.get(key_name) or "unknown").strip() or "unknown"
        bucket = buckets.setdefault(key, {
            "name": key,
            "tokens": 0,
            "cost": 0.0,
            "quotaTokens": 0,
            "meteredTokens": 0,
            "localTokens": 0,
            "unknownTokens": 0,
            "count": 0,
        })
        bucket["tokens"] += int(entry.get("totalTokens") or 0)
        bucket["count"] += 1
        mode = entry.get("billingMode")
        if mode == "api_key_billable":
            bucket["cost"] += float(entry.get("costUSD") or 0.0)
        elif mode == "api_key_unpriced":
            bucket["meteredTokens"] += int(entry.get("totalTokens") or 0)
        elif mode == "oauth_quota":
            bucket["quotaTokens"] += int(entry.get("totalTokens") or 0)
        elif mode == "local_unmetered":
            bucket["localTokens"] += int(entry.get("totalTokens") or 0)
        else:
            bucket["unknownTokens"] += int(entry.get("totalTokens") or 0)
    return sorted(buckets.values(), key=lambda row: (-row["tokens"], row["name"]))


def get_gateway_usage_cost(days=30):
    def builder():
        quoted_env = shlex.quote(str(OPENCLAW_CREDENTIALS))
        quoted_openclaw = shlex.quote(get_openclaw_bin())
        node_bin = get_node_bin_dir()
        path_prefix = f"{node_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}" if node_bin else os.environ.get("PATH", "/usr/bin:/bin")
        quoted_path = shlex.quote(path_prefix)
        cmd = (
            f"export PATH={quoted_path}; "
            f"set -a; "
            f"[ -f {quoted_env} ] && source {quoted_env}; "
            f"set +a; "
            f"{quoted_openclaw} gateway usage-cost --json --days {int(days)}"
        )
        code, out, _err = run_command_capture(shell_command(cmd), timeout=12)
        if code != 0 or not out.strip():
            return None
        try:
            return json.loads(out)
        except Exception:
            return None

    return get_cached_value(f"usage:gateway-cost:{int(days)}", 60.0, builder)


def get_defaults_config():
    cfg = load_openclaw_config()
    return ((cfg.get("agents") or {}).get("defaults") or {})


def get_configured_model_refs():
    cfg = load_openclaw_config()
    defaults = get_defaults_config()
    models = defaults.get("models") or {}
    refs = set()
    if isinstance(models, dict):
        refs.update(str(key).strip() for key in models.keys() if str(key).strip())
    providers = ((cfg.get("models") or {}).get("providers") or {})
    for provider, entry in providers.items():
        if not isinstance(entry, dict):
            continue
        for model in entry.get("models") or []:
            if not isinstance(model, dict):
                continue
            model_id = str(model.get("id") or "").strip()
            if not model_id:
                continue
            refs.add(normalize_model_ref(provider, model_id))
    current = get_default_model_ref()
    if current:
        refs.add(current)
    for fallback in get_fallback_chain():
        if fallback:
            refs.add(fallback)
    for agent_model in get_agent_model_map().values():
        agent_model = str(agent_model or "").strip()
        if agent_model and agent_model != "inherit":
            refs.add(agent_model)
    if not any(ref.startswith("router/") for ref in get_active_provider_refs()):
        refs = {ref for ref in refs if not ref.startswith("router/")}
    return sorted(refs, key=lambda value: value.lower())


def provider_for_model_ref(model_ref):
    ref = str(model_ref or "").strip()
    if not ref:
        return ""
    provider = ref.split("/", 1)[0]
    if provider == "google":
        return "gemini"
    return provider


def policy_target_for_agent(agent_id):
    agent = str(agent_id or "").strip().lower()
    if agent == "code":
        return "coding"
    if agent in {"simple", "research"}:
        return "research"
    if agent in {"reasoning", "verify", "verification"}:
        return "verification"
    return "coding"


def routing_policy_status(session_entry, policy=None):
    policy = policy or get_routing_policy()
    target_key = policy_target_for_agent((session_entry or {}).get("agentId"))
    target = (policy.get(target_key) or {})
    expected_lane = str(target.get("lane") or "").strip()
    actual_lane = str((session_entry or {}).get("modelRef") or "").strip()
    if not expected_lane or not actual_lane:
        return {"target": target_key, "expected": expected_lane, "actual": actual_lane, "status": "unknown"}
    if expected_lane == actual_lane:
        return {"target": target_key, "expected": expected_lane, "actual": actual_lane, "status": "match"}
    same_provider = provider_for_model_ref(expected_lane) == provider_for_model_ref(actual_lane)
    if same_provider and not bool(target.get("strict")):
        return {"target": target_key, "expected": expected_lane, "actual": actual_lane, "status": "soft-match"}
    return {"target": target_key, "expected": expected_lane, "actual": actual_lane, "status": "mismatch"}


def build_route_timeline(sessions, limit=8):
    policy = get_routing_policy()
    rows = []
    for session in (sessions or [])[:limit]:
        policy_info = routing_policy_status(session, policy)
        rows.append({
            "sessionId": session.get("sessionId"),
            "agentId": session.get("agentId"),
            "modelRef": session.get("modelRef"),
            "provider": session.get("provider"),
            "account": session.get("account"),
            "billingMode": session.get("billingMode"),
            "tokens": int(session.get("totalTokens") or 0),
            "costUSD": float(session.get("totalCostUSD") or 0.0),
            "errors": int(session.get("errors") or 0),
            "lastSeenLabel": session.get("lastSeenLabel") or "unknown",
            "policyTarget": policy_info["target"],
            "policyExpected": policy_info["expected"],
            "policyStatus": policy_info["status"],
        })
    return rows


def get_pricing_registry_rows(entries, limit=8):
    catalog = load_model_cost_catalog()
    usage_by_model = {row["name"]: row for row in summarize_rollups(entries or [], "modelRef")}
    rows = []
    refs = sorted(set(list(catalog.keys()) + list(usage_by_model.keys())))
    for model_ref in refs:
        catalog_row = catalog.get(model_ref) or {}
        usage_row = usage_by_model.get(model_ref) or {}
        cost = normalize_cost_dict(catalog_row.get("cost") or {})
        rows.append({
            "modelRef": model_ref,
            "provider": provider_for_model_ref(model_ref),
            "tokens": int(usage_row.get("tokens") or 0),
            "cost": cost,
            "priced": nonzero_cost_config(cost),
            "source": str(catalog_row.get("pricingSource") or ("config" if model_ref in catalog else "manual")).strip() or "manual",
            "lastVerifiedAt": str(catalog_row.get("lastVerifiedAt") or "").strip(),
        })
    rows.sort(key=lambda row: (-row["tokens"], row["modelRef"]))
    return rows[:limit]


def get_provider_drilldown(entries, provider_tests, limit_models=4):
    catalog = load_model_cost_catalog()
    providers = {}
    for entry in entries or []:
        provider = provider_for_model_ref(entry.get("modelRef") or "") or str(entry.get("provider") or "").strip() or "unknown"
        bucket = providers.setdefault(provider, {
            "provider": provider,
            "tokens": 0,
            "cost": 0.0,
            "quotaTokens": 0,
            "meteredTokens": 0,
            "localTokens": 0,
            "unknownTokens": 0,
            "models": {},
            "accounts": set(),
            "lastSeen": entry.get("timestamp"),
        })
        bucket["tokens"] += int(entry.get("totalTokens") or 0)
        bucket["cost"] += float(entry.get("costUSD") or 0.0) if entry.get("billingMode") == "api_key_billable" else 0.0
        mode = entry.get("billingMode")
        if mode == "oauth_quota":
            bucket["quotaTokens"] += int(entry.get("totalTokens") or 0)
        elif mode == "api_key_unpriced":
            bucket["meteredTokens"] += int(entry.get("totalTokens") or 0)
        elif mode == "local_unmetered":
            bucket["localTokens"] += int(entry.get("totalTokens") or 0)
        elif mode == "unknown":
            bucket["unknownTokens"] += int(entry.get("totalTokens") or 0)
        account = str(entry.get("account") or "none").strip() or "none"
        bucket["accounts"].add(account)
        model_ref = str(entry.get("modelRef") or "").strip()
        if model_ref:
            model_bucket = bucket["models"].setdefault(model_ref, 0)
            bucket["models"][model_ref] = model_bucket + int(entry.get("totalTokens") or 0)
        ts = entry.get("timestamp")
        if ts and (bucket["lastSeen"] is None or ts > bucket["lastSeen"]):
            bucket["lastSeen"] = ts
    rows = []
    for provider, bucket in providers.items():
        smoke = provider_tests.get(provider if provider != "gemini" else "gemini", {}) or {}
        top_models = sorted(bucket["models"].items(), key=lambda item: (-item[1], item[0]))[:limit_models]
        priced_models = 0
        for model_ref in bucket["models"].keys():
            if nonzero_cost_config((catalog.get(model_ref) or {}).get("cost") or {}):
                priced_models += 1
        rows.append({
            "provider": provider,
            "tokens": bucket["tokens"],
            "cost": bucket["cost"],
            "quotaTokens": bucket["quotaTokens"],
            "meteredTokens": bucket["meteredTokens"],
            "localTokens": bucket["localTokens"],
            "unknownTokens": bucket["unknownTokens"],
            "accounts": sorted(bucket["accounts"]),
            "topModels": top_models,
            "lastSeenLabel": format_age_short(bucket["lastSeen"]) + " ago" if bucket["lastSeen"] else "unknown",
            "smoke": smoke,
            "pricedModels": priced_models,
        })
    rows.sort(key=lambda row: (-row["tokens"], row["provider"]))
    return rows


def get_account_drilldown(entries):
    rows = summarize_rollups(entries or [], "account")
    normalized = []
    for row in rows:
        normalized.append({
            "account": row["name"],
            "tokens": row["tokens"],
            "cost": row["cost"],
            "quotaTokens": row["quotaTokens"],
            "meteredTokens": row["meteredTokens"],
            "localTokens": row["localTokens"],
            "unknownTokens": row["unknownTokens"],
            "count": row["count"],
        })
    return normalized


def get_active_provider_refs():
    refs = [get_default_model_ref(), *get_fallback_chain()]
    refs.extend(
        model_ref for model_ref in get_agent_model_map().values()
        if str(model_ref or "").strip() and str(model_ref).strip() != "inherit"
    )
    return [str(ref).strip() for ref in refs if str(ref).strip()]


def show_openai_api_surface():
    active_refs = get_active_provider_refs()
    active_providers = {provider_for_model_ref(ref) for ref in active_refs}
    return any(provider in active_providers for provider in ("openai", "router"))


def provider_relevance_rank(status_key):
    provider_aliases = {
        "codex": {"openai-codex"},
        "openai": {"openai", "router"},
        "nim": {"nim"},
        "gemini": {"gemini"},
        "ollama": {"ollama"},
    }
    primary_ref = get_default_model_ref()
    fallback_refs = set(get_fallback_chain())
    aliases = provider_aliases.get(status_key, {status_key})
    refs = get_active_provider_refs()
    if provider_for_model_ref(primary_ref) in aliases:
        return 0
    if any(provider_for_model_ref(ref) in aliases for ref in fallback_refs):
        return 1
    if any(provider_for_model_ref(ref) in aliases for ref in refs if ref != primary_ref and ref not in fallback_refs):
        return 2
    return 3


def provider_relevance_label(status_key):
    return {
        0: "primary",
        1: "fallback",
        2: "agent",
        3: "unused",
    }.get(provider_relevance_rank(status_key), "unused")


def get_fallback_chain():
    model = get_defaults_config().get("model") or {}
    if isinstance(model, dict):
        values = model.get("fallbacks") or []
        return [str(v).strip() for v in values if str(v).strip()]
    return []


def get_agent_model_map():
    cfg = load_openclaw_config()
    agents = ((cfg.get("agents") or {}).get("list") or [])
    mapping = {}
    for agent in agents:
        if not isinstance(agent, dict):
            continue
        agent_id = str(agent.get("id") or "").strip()
        if not agent_id:
            continue
        model = agent.get("model") or {}
        if isinstance(model, dict):
            primary = str(model.get("primary") or "").strip()
        else:
            primary = str(model or "").strip()
        mapping[agent_id] = primary or "inherit"
    return mapping


def set_agent_model_ref(agent_id, model_ref):
    cfg = load_openclaw_config()
    agents = cfg.setdefault("agents", {}).setdefault("list", [])
    target = None
    for agent in agents:
        if isinstance(agent, dict) and str(agent.get("id") or "").strip() == agent_id:
            target = agent
            break
    if target is None:
        target = {"id": agent_id}
        agents.append(target)
    current = target.get("model") or {}
    fallbacks = []
    if isinstance(current, dict):
        raw = current.get("fallbacks") or []
        if isinstance(raw, list):
            fallbacks = [str(v).strip() for v in raw if str(v).strip()]
    target["model"] = {"primary": model_ref, "fallbacks": fallbacks}
    write_openclaw_config(cfg)


def set_fallback_chain(fallbacks):
    cfg = load_openclaw_config()
    agents = cfg.setdefault("agents", {})
    defaults = agents.setdefault("defaults", {})
    model_cfg = defaults.get("model") or {}
    primary = get_default_model_ref()
    if isinstance(model_cfg, dict):
        primary = str(model_cfg.get("primary") or primary).strip() or primary
    normalized = []
    for fallback in fallbacks:
        fallback = str(fallback or "").strip()
        if not fallback or fallback == primary or fallback in normalized:
            continue
        normalized.append(fallback)
    defaults["model"] = {
        "primary": primary,
        "fallbacks": normalized,
    }
    write_openclaw_config(cfg)


def set_subagent_model_ref(model_ref):
    cfg = load_openclaw_config()
    defaults = cfg.setdefault("agents", {}).setdefault("defaults", {})
    subagents = defaults.setdefault("subagents", {})
    subagents["model"] = str(model_ref or "").strip()
    write_openclaw_config(cfg)


def get_subagent_model_ref():
    defaults = get_defaults_config()
    subagents = defaults.get("subagents") or {}
    if isinstance(subagents, dict):
        return str(subagents.get("model") or "").strip()
    return ""


def snapshot_current_routing():
    return {
        "default": get_default_model_ref(),
        "fallbacks": get_fallback_chain(),
        "agents": get_agent_model_map(),
        "subagents": get_subagent_model_ref(),
    }


def save_current_baseline():
    state = load_codexbar_state()
    state["baseline"] = snapshot_current_routing()
    save_codexbar_state(state)
    append_event("baseline.save", f"Saved baseline {short_model_name(state['baseline'].get('default'))}", "good")
    return state["baseline"]


def restore_saved_baseline():
    state = load_codexbar_state()
    baseline = state.get("baseline") or {}
    default = str(baseline.get("default") or "").strip()
    if not default:
        return False
    create_restore_point("restore-baseline", include_credentials=False)
    set_default_model_ref(default)
    set_fallback_chain(baseline.get("fallbacks") or [])
    for agent_id, model_ref in (baseline.get("agents") or {}).items():
        if str(model_ref or "").strip():
            set_agent_model_ref(agent_id, str(model_ref).strip())
    if str(baseline.get("subagents") or "").strip():
        set_subagent_model_ref(str(baseline["subagents"]).strip())
    append_event("baseline.restore", f"Restored baseline {short_model_name(default)}", "warn", model=default)
    return True


def set_focus_models(default_model, code_model=None, create_snapshot=True):
    if create_snapshot:
        create_restore_point("set-focus-models", include_credentials=False)
    set_default_model_ref(default_model)
    set_agent_model_ref("main", default_model)
    set_agent_model_ref("code", code_model or default_model)
    if create_snapshot:
        append_event("routing.focus", f"Focused routing on {short_model_name(default_model)}", "good", model=default_model)


def apply_routing_profile(profile_name):
    profile = ROUTING_PROFILES.get(profile_name)
    if not profile:
        return False
    create_restore_point(f"apply-profile:{profile_name}", include_credentials=False)
    set_default_model_ref(profile["default"])
    set_fallback_chain(profile.get("fallbacks") or [])
    for agent_id, model_ref in (profile.get("agents") or {}).items():
        set_agent_model_ref(agent_id, model_ref)
    if profile.get("subagents"):
        set_subagent_model_ref(profile["subagents"])
    append_event("routing.profile", f"Applied profile {profile_name}", "good", profile=profile_name, model=profile["default"])
    return True


def detect_active_routing_profile():
    current_default = get_default_model_ref()
    current_fallbacks = get_fallback_chain()
    current_agents = get_agent_model_map()
    current_subagents = get_subagent_model_ref()
    for name, profile in ROUTING_PROFILES.items():
        if current_default != profile.get("default"):
            continue
        if current_fallbacks != list(profile.get("fallbacks") or []):
            continue
        profile_agents = profile.get("agents") or {}
        if any(current_agents.get(agent_id) != model_ref for agent_id, model_ref in profile_agents.items()):
            continue
        if profile.get("subagents") and current_subagents != profile.get("subagents"):
            continue
        return name
    return ""


def derive_route_mode():
    default_model = get_default_model_ref()
    if default_model.startswith("openai-codex/"):
        return "Native Codex"
    if default_model.startswith("router/"):
        return "Legacy Router"
    if default_model.startswith("ollama/"):
        return "Local Only"
    return "Direct Provider"


def build_overview_snapshot(statuses):
    default_model = get_default_model_ref()
    route_mode = derive_route_mode()
    fallbacks = get_fallback_chain()
    issues = []
    if not statuses.get("openclaw", {}).get("ok"):
        issues.append("gateway")
    if get_router_enabled() and not statuses.get("codex", {}).get("ok"):
        issues.append("codex")
    if not statuses.get("ollama", {}).get("ok"):
        issues.append("ollama")
    health = "Ready" if not issues else f"Degraded: {', '.join(issues)}"
    return {
        "default_model": default_model,
        "route_mode": route_mode,
        "fallbacks": fallbacks,
        "health": health,
    }


def save_config_snapshot():
    try:
        return OPENCLAW_CONFIG.read_text()
    except Exception:
        return ""


def restore_config_snapshot(snapshot):
    if snapshot is None:
        return False
    try:
        OPENCLAW_CONFIG.write_text(snapshot)
        return True
    except Exception:
        return False


def get_provider_model_choices():
    refs = get_configured_model_refs()
    grouped = {}
    for ref in refs:
        provider = provider_for_model_ref(ref)
        grouped.setdefault(provider, []).append(ref)
    for provider in grouped:
        grouped[provider] = sorted(grouped[provider])
    return grouped


def pick_provider_smoke_model(provider_key):
    choices = get_provider_model_choices()
    preferred = {
        "nim": [
            "nim/nvidia/nemotron-3-super-120b-a12b",
            "nim/qwen/qwen3-coder-480b-a35b-instruct",
        ],
        "gemini": [
            "google/gemini-3.1-pro-preview",
            "google/gemini-3-flash-preview",
        ],
        "ollama": [
            "ollama/nemotron-mini:latest",
        ],
        "openai-codex": [
            "openai-codex/gpt-5.4",
        ],
    }.get(provider_key, [])
    provider_refs = choices.get(provider_key, [])
    for ref in preferred:
        if ref in provider_refs:
            return ref
    return provider_refs[0] if provider_refs else ""


def build_openclaw_agent_command(message, session_id):
    quoted_env = shlex.quote(str(OPENCLAW_CREDENTIALS))
    quoted_openclaw = shlex.quote(get_openclaw_bin())
    node_bin = get_node_bin_dir()
    path_prefix = f"{node_bin}:{os.environ.get('PATH', '/usr/bin:/bin')}" if node_bin else os.environ.get("PATH", "/usr/bin:/bin")
    quoted_path = shlex.quote(path_prefix)
    return (
        f"export PATH={quoted_path}; "
        f"set -a; "
        f"[ -f {quoted_env} ] && source {quoted_env}; "
        f"set +a; "
        f"{quoted_openclaw} agent --json --session-id {shlex.quote(session_id)} "
        f"-m {shlex.quote(message)}"
    )


def normalize_smoke_result_output(payload):
    if isinstance(payload, dict):
        payloads = payload.get("payloads")
        if isinstance(payloads, list):
            texts = [str(item.get("text") or "").strip() for item in payloads if isinstance(item, dict) and str(item.get("text") or "").strip()]
            if texts:
                return "\n".join(texts).strip()
        for key in ("response", "content", "text", "output", "message"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        data = payload.get("data")
        if isinstance(data, dict):
            return normalize_smoke_result_output(data)
        result = payload.get("result")
        if isinstance(result, dict):
            return normalize_smoke_result_output(result)
    return ""


def extract_provider_model_from_payload(payload):
    if isinstance(payload, dict):
        if isinstance(payload.get("agentMeta"), dict):
            provider = str(payload["agentMeta"].get("provider") or "").strip().lower()
            model = str(payload["agentMeta"].get("model") or "").strip()
            if provider or model:
                return provider, normalize_model_ref(provider, model)
        provider = str(payload.get("provider") or "").strip().lower()
        model = str(payload.get("model") or "").strip()
        if provider or model:
            return provider, normalize_model_ref(provider, model)
        for value in payload.values():
            found_provider, found_model = extract_provider_model_from_payload(value)
            if found_provider or found_model:
                return found_provider, found_model
    elif isinstance(payload, list):
        for value in payload:
            found_provider, found_model = extract_provider_model_from_payload(value)
            if found_provider or found_model:
                return found_provider, found_model
    return "", ""


def run_provider_smoke_test(provider_key):
    model_ref = pick_provider_smoke_model(provider_key)
    if not model_ref:
        return {"ok": False, "provider": provider_key, "detail": "No configured model for provider.", "model": ""}
    session_id = f"codexbar-smoke-{provider_key}-{uuid.uuid4().hex[:8]}"
    with ROUTING_MUTATION_LOCK:
        baseline = save_config_snapshot()
        try:
            set_focus_models(model_ref, create_snapshot=False)
            restart_openclaw_stack()
            time.sleep(3)
            cmd = build_openclaw_agent_command("Reply with exactly OK", session_id)
            code, out, err = run_command_capture(shell_command(cmd), timeout=90)
            if code != 0:
                return {"ok": False, "provider": provider_key, "detail": err or out or "Agent run failed.", "model": model_ref}
            try:
                payload = json.loads(out)
            except Exception:
                payload = {}
            text = normalize_smoke_result_output(payload) or out.strip()
            actual_provider, actual_model = extract_provider_model_from_payload(payload)
            ok = text.strip() == "OK" or '"OK"' in text or '"response":"OK"' in text.replace(" ", "")
            detail = text.strip() or "No output."
            normalized_actual_provider = provider_for_model_ref(f"{actual_provider}/{payload.get('model') or ''}") if actual_provider else ""
            if normalized_actual_provider and normalized_actual_provider != provider_key:
                ok = False
                detail = f"Resolved to {normalized_actual_provider} instead of {provider_key}. Output: {detail}"
            return {
                "ok": ok,
                "provider": provider_key,
                "detail": detail,
                "model": actual_model or model_ref,
                "sessionId": session_id,
            }
        finally:
            restore_config_snapshot(baseline)
            restart_openclaw_stack()


def persist_provider_test_result(provider_key, result):
    state = load_codexbar_state()
    state.setdefault("provider_tests", {})[provider_key] = {
        "ok": result.get("ok"),
        "detail": str(result.get("detail") or "")[:800],
        "model": result.get("model") or "",
        "sessionId": result.get("sessionId") or "",
        "checkedAt": datetime.now().strftime("%H:%M:%S"),
    }
    save_codexbar_state(state)
    append_event(
        "smoke.pass" if result.get("ok") else "smoke.fail",
        f"{provider_key} smoke {'passed' if result.get('ok') else 'failed'}",
        "good" if result.get("ok") else "bad",
        provider=provider_key,
        model=result.get("model") or "",
        detail=str(result.get("detail") or "")[:240],
    )


def run_scheduled_smoke_tests():
    providers = ("nim", "gemini", "openai-codex", "ollama")
    summary = {}
    append_event("smoke.batch.start", "Scheduled smoke tests started", "warn")
    for provider_key in providers:
        result = run_provider_smoke_test(provider_key)
        persist_provider_test_result(provider_key, result)
        summary[provider_key] = {
            "ok": bool(result.get("ok")),
            "model": result.get("model") or "",
            "detail": str(result.get("detail") or "")[:200],
            "checkedAt": datetime.now().strftime("%H:%M:%S"),
        }
    state = load_codexbar_state()
    state.setdefault("scheduled_smoke_tests", {})
    state["scheduled_smoke_tests"]["enabled"] = True
    state["scheduled_smoke_tests"]["lastRunAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state["scheduled_smoke_tests"]["summary"] = summary
    save_codexbar_state(state)
    ok_count = len([row for row in summary.values() if row.get("ok")])
    append_event("smoke.batch.finish", f"Scheduled smoke tests finished {ok_count}/{len(providers)} passing", "good" if ok_count == len(providers) else "warn")
    return summary


def cleanup_legacy_router_refs():
    cfg = load_openclaw_config()
    changed = False
    defaults = cfg.setdefault("agents", {}).setdefault("defaults", {})
    model_cfg = defaults.get("model") or {}
    legacy_ref = "router/codex/gpt-5.4"

    def clean_model_block(block):
        local_changed = False
        if isinstance(block, dict):
            if str(block.get("primary") or "").strip() == legacy_ref:
                block["primary"] = "openai-codex/gpt-5.4"
                local_changed = True
            fallbacks = [str(v).strip() for v in (block.get("fallbacks") or []) if str(v).strip()]
            cleaned = [("openai-codex/gpt-5.4" if value == legacy_ref else value) for value in fallbacks]
            deduped = []
            for value in cleaned:
                if value and value not in deduped and value != block.get("primary"):
                    deduped.append(value)
            if deduped != fallbacks:
                block["fallbacks"] = deduped
                local_changed = True
        return local_changed

    if clean_model_block(model_cfg):
        defaults["model"] = model_cfg
        changed = True

    defaults_models = defaults.get("models") or {}
    if legacy_ref in defaults_models:
        defaults_models.pop(legacy_ref, None)
        changed = True
    defaults_models.setdefault("openai-codex/gpt-5.4", {})
    defaults["models"] = defaults_models

    for agent in cfg.setdefault("agents", {}).setdefault("list", []):
        if not isinstance(agent, dict):
            continue
        agent_model = agent.get("model") or {}
        if clean_model_block(agent_model):
            agent["model"] = agent_model
            changed = True

    if changed:
        write_openclaw_config(cfg)
    return changed


def move_fallback_entry(slot, direction):
    fallbacks = get_fallback_chain()
    if slot < 0 or slot >= len(fallbacks):
        return False
    target = slot + direction
    if target < 0 or target >= len(fallbacks):
        return False
    fallbacks[slot], fallbacks[target] = fallbacks[target], fallbacks[slot]
    set_fallback_chain(fallbacks)
    return True


def clear_fallback_entry(slot):
    fallbacks = get_fallback_chain()
    if slot < 0 or slot >= len(fallbacks):
        return False
    fallbacks.pop(slot)
    set_fallback_chain(fallbacks)
    return True


def build_repair_actions(statuses):
    active_models = [get_default_model_ref(), *get_fallback_chain()]
    active_providers = {provider_for_model_ref(model) for model in active_models if model}
    actions = []
    if not statuses.get("openclaw", {}).get("ok"):
        actions.append(("Restart Gateway", "gateway"))
    if "nim" in active_providers and statuses.get("nim", {}).get("nokey"):
        actions.append(("Set NVIDIA Key", "nim_key"))
    if "gemini" in active_providers and statuses.get("gemini", {}).get("nokey"):
        actions.append(("Set Gemini Key", "gemini_key"))
    if "openai-codex" in active_providers and statuses.get("codex", {}).get("nokey"):
        actions.append(("Codex Login", "codex_login"))
    if any(provider in active_providers for provider in ("openai", "router")) and statuses.get("openai", {}).get("nokey"):
        actions.append(("Set OpenAI Key", "openai_key"))
    if not statuses.get("ollama", {}).get("ok"):
        actions.append(("Open Control UI", "open_ui"))
    return actions[:3]


# ── cost subcommand ────────────────────────────────────────────────────

def cmd_usage_snapshot(_args):
    summary = get_usage_summary()
    save_usage_snapshot(summary)
    payload = build_usage_snapshot(summary)
    print(json.dumps({
        "ok": True,
        "snapshotAt": payload.get("snapshotAt"),
        "snapshotEpoch": payload.get("snapshotEpoch"),
        "todayCost": round(float(((payload.get("today") or {}).get("cost") or 0.0)), 6),
        "todayTokens": int(((payload.get("today") or {}).get("tokens") or 0)),
    }, indent=2))


def cmd_scheduled_smoke_tests(_args):
    summary = run_scheduled_smoke_tests()
    passing = len([row for row in summary.values() if row.get("ok")])
    print(json.dumps({
        "ok": True,
        "passing": passing,
        "total": len(summary),
        "summary": summary,
    }, indent=2))


def cmd_cost(args):
    """Output cost JSON for model-usage skill."""
    provider = "codex"
    fmt = "text"
    i = 2
    while i < len(args):
        if args[i] == "--provider" and i+1 < len(args):
            provider = args[i+1]; i += 2
        elif args[i] == "--format" and i+1 < len(args):
            fmt = args[i+1]; i += 2
        else:
            i += 1

    aliases = {
        "codex": {"openai-codex", "router"},
        "claude": {"anthropic"},
        "openclaw": None,
    }
    selected = aliases.get(provider, {provider})
    ledger = build_usage_ledger()
    entries = ledger["entries"]
    daily_rows = ledger["daily"]

    def matches(entry):
        if selected is None:
            return True
        entry_provider = str(entry.get("provider") or "").strip().lower()
        if entry_provider in selected:
            return True
        if provider == "codex" and "codex" in str(entry.get("modelRef") or "").lower():
            return True
        return False

    filtered_entries = [entry for entry in entries if matches(entry)]
    gateway_cost = get_gateway_usage_cost(30) if provider == "openclaw" else None
    gateway_daily = {
        str(row.get("date")): row
        for row in ((gateway_cost or {}).get("daily") or [])
        if isinstance(row, dict)
    }
    filtered_daily = []
    for row in daily_rows:
        row_entries = [entry for entry in filtered_entries if entry.get("date") == row.get("date")]
        if not row_entries:
            continue
        metrics = summarize_usage_metrics(row_entries)
        gateway_row = gateway_daily.get(str(row.get("date"))) or {}
        filtered_daily.append({
            "date": row["date"],
            "inputTokens": sum(int(entry.get("inputTokens") or 0) for entry in row_entries),
            "outputTokens": sum(int(entry.get("outputTokens") or 0) for entry in row_entries),
            "cacheReadTokens": sum(int(entry.get("cacheReadTokens") or 0) for entry in row_entries),
            "cacheWriteTokens": sum(int(entry.get("cacheWriteTokens") or 0) for entry in row_entries),
            "totalTokens": metrics["tokens"],
            "totalCost": round(float(gateway_row.get("totalCost") or metrics["cost"]), 6),
            "modelsUsed": sorted({entry.get("modelRef") for entry in row_entries if entry.get("modelRef")}),
            "modelBreakdowns": [
                {
                    "modelName": model_ref,
                    "cost": round(sum(float(entry.get("costUSD") or 0.0) for entry in row_entries if entry.get("modelRef") == model_ref), 6),
                    "tokens": sum(int(entry.get("totalTokens") or 0) for entry in row_entries if entry.get("modelRef") == model_ref),
                    "billingMode": next((entry.get("billingMode") for entry in row_entries if entry.get("modelRef") == model_ref), "unknown"),
                }
                for model_ref in sorted({entry.get("modelRef") for entry in row_entries if entry.get("modelRef")})
            ],
        })

    totals = summarize_usage_metrics(filtered_entries)
    if gateway_cost and provider == "openclaw":
        totals["cost"] = float(((gateway_cost.get("totals") or {}).get("totalCost") or totals["cost"]))

    payload = [{
        "provider": provider,
        "source": "gateway-usage-cost" if provider == "openclaw" and gateway_cost else "session-ledger",
        "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sessionTokens": totals["tokens"],
        "sessionCostUSD": round(totals["cost"], 4),
        "last30DaysTokens": totals["tokens"],
        "last30DaysCostUSD": round(totals["cost"], 4),
        "daily": filtered_daily,
        "totals": {
            "totalTokens": totals["tokens"],
            "totalCost": round(totals["cost"], 4),
            "quotaTokens": totals["quotaTokens"],
            "localTokens": totals["localTokens"],
            "unknownTokens": totals["unknownTokens"],
        },
    }]
    print(json.dumps(payload, indent=2))


# ── Waybar mode ────────────────────────────────────────────────────────

def waybar_output():
    while True:
        try:
            s = get_all_status()
            text = "  ".join(s[k]["label"] for k in ("nim", "gemini", "codex", "ollama"))
            tooltip = "\n".join([
                "── OpenClaw Status ──",
                *[s[k]["detail"] for k in ("nim", "gemini", "openai", "ollama", "codex", "openclaw")],
                "", f"Updated: {time.strftime('%H:%M:%S')}",
            ])
            ok = all(v["ok"] for k, v in s.items() if not v.get("nokey"))
            print(json.dumps({"text": f" {text}", "tooltip": tooltip,
                              "class": "llm-ok" if ok else "llm-warn"}), flush=True)
        except Exception as e:
            print(json.dumps({"text": " LLM:ERR", "tooltip": str(e), "class": "llm-err"}), flush=True)
        time.sleep(300)


# ── CSS ────────────────────────────────────────────────────────────────

CSS_DARK = b"""
window { background: transparent; }
.glass {
    background:
        linear-gradient(180deg, rgba(20, 20, 30, 0.94), rgba(9, 10, 18, 0.92));
    border-radius: 18px;
    border: 1px solid rgba(255,255,255,0.11);
}
.ttl { color: rgba(160,160,200,0.55); font-size: 9px; font-weight: bold; letter-spacing: 3px; }
.subttl { color: rgba(214, 211, 255, 0.88); font-size: 15px; font-weight: 800; }
.topbar {
    background: rgba(255,255,255,0.035);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 14px;
    padding: 10px 12px;
}
.topbar-meta { color: rgba(170,180,220,0.72); font-size: 10px; }
.hero {
    background: linear-gradient(135deg, rgba(93, 63, 211, 0.24), rgba(35, 190, 255, 0.12));
    border: 1px solid rgba(182, 162, 255, 0.22);
    border-radius: 14px;
    padding: 12px;
}
.card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 12px;
    padding: 10px;
}
.tab-bar {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px;
    padding: 4px;
}
.tab-btn {
    background: transparent;
    border: none;
    border-radius: 8px;
    color: rgba(168,174,208,0.82);
    padding: 6px 12px;
    font-size: 11px;
    font-weight: bold;
}
.tab-btn:hover { background: rgba(255,255,255,0.09); color: rgba(232,236,255,0.96); }
.tab-active {
    background: linear-gradient(180deg, rgba(73,126,255,0.35), rgba(89,74,214,0.26));
    border-radius: 8px;
    color: #dfe5ff;
    padding: 6px 12px;
    font-size: 11px;
    font-weight: bold;
    border: 1px solid rgba(140,160,255,0.35);
}
.pkey { color: rgba(140,140,185,0.75); font-size: 11px; font-family: monospace; font-weight: bold; }
.pval { font-size: 11px; font-family: monospace; }
.hero-title { color: #ffffff; font-size: 16px; font-weight: 800; }
.hero-sub { color: rgba(225,230,255,0.82); font-size: 11px; }
.metric-value { color: #ffffff; font-size: 16px; font-weight: 800; }
.metric-label { color: rgba(165,170,210,0.72); font-size: 10px; letter-spacing: 1px; }
.pill {
    background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 999px;
    color: rgba(225,228,255,0.92);
    padding: 3px 8px;
    font-size: 10px;
    font-weight: bold;
}
.pill-good {
    background: rgba(0,230,118,0.12);
    border: 1px solid rgba(0,230,118,0.26);
    color: #8cf7ba;
}
.pill-warn {
    background: rgba(255,145,0,0.12);
    border: 1px solid rgba(255,145,0,0.26);
    color: #ffcb85;
}
.pill-bad {
    background: rgba(255,82,82,0.12);
    border: 1px solid rgba(255,82,82,0.26);
    color: #ffb1b1;
}
.ok   { color: #00e676; }
.warn { color: #ff9100; }
.err  { color: #ff5252; }
.nokey { color: rgba(120,120,160,0.6); }
.upd  { color: rgba(100,100,135,0.55); font-size: 9px; }
.subdued { color: rgba(135,145,185,0.78); font-size: 10px; }
.btn {
    background: rgba(255,255,255,0.07);
    border: 1px solid rgba(255,255,255,0.11);
    border-radius: 8px;
    color: rgba(190,190,225,0.9);
    padding: 4px 10px;
    font-size: 11px;
}
.btn:hover { background: rgba(255,255,255,0.13); }
.btn-primary {
    background: linear-gradient(180deg, rgba(76,106,255,0.35), rgba(94,79,224,0.28));
    border: 1px solid rgba(140,160,255,0.35);
    color: #f7f9ff;
    font-weight: bold;
}
.btn-good {
    background: rgba(0,230,118,0.14);
    border: 1px solid rgba(0,230,118,0.26);
    color: #b7ffd7;
    font-weight: bold;
}
.btn-warn {
    background: rgba(255,145,0,0.14);
    border: 1px solid rgba(255,145,0,0.26);
    color: #ffd6a1;
    font-weight: bold;
}
.btn-bad {
    background: rgba(255,82,82,0.14);
    border: 1px solid rgba(255,82,82,0.28);
    color: #ffc0c0;
    font-weight: bold;
}
.btn-ghost {
    background: rgba(255,255,255,0.035);
    border: 1px solid rgba(255,255,255,0.08);
    color: rgba(190,196,230,0.78);
}
.btn-ghost:hover { background: rgba(255,255,255,0.08); }
.theme-btn {
    min-width: 44px;
    min-height: 32px;
}
.btn-router-on {
    background: rgba(0,230,118,0.15);
    border: 1px solid rgba(0,230,118,0.4);
    border-radius: 8px;
    color: #00e676;
    padding: 4px 10px;
    font-size: 11px;
    font-weight: bold;
}
.btn-router-off {
    background: rgba(255,81,82,0.12);
    border: 1px solid rgba(255,81,82,0.35);
    border-radius: 8px;
    color: #ff5252;
    padding: 4px 10px;
    font-size: 11px;
    font-weight: bold;
}
separator { background: rgba(255,255,255,0.07); min-height: 1px; margin: 2px 0; }
progressbar trough {
    background: rgba(255,255,255,0.08);
    border-radius: 4px; min-height: 5px; min-width: 140px;
}
progressbar progress { background: #00e676; border-radius: 4px; }
progressbar.warn progress { background: #ff9100; }
progressbar.crit progress { background: #ff5252; }
progressbar.blue progress { background: #40c4ff; }
"""

CSS_LIGHT = b"""
window { background: transparent; }
.glass {
    background: rgba(245,245,252,0.94);
    border-radius: 18px;
    border: 1px solid rgba(0,0,0,0.08);
}
.ttl  { color: rgba(60,60,100,0.55); font-size: 9px; font-weight: bold; letter-spacing: 3px; }
.subttl { color: rgba(40,40,70,0.92); font-size: 15px; font-weight: 800; }
.topbar {
    background: rgba(0,0,0,0.025);
    border: 1px solid rgba(0,0,0,0.07);
    border-radius: 14px;
    padding: 10px 12px;
}
.topbar-meta { color: rgba(70,80,120,0.72); font-size: 10px; }
.hero {
    background: linear-gradient(135deg, rgba(123, 108, 255, 0.18), rgba(78, 181, 240, 0.14));
    border: 1px solid rgba(100, 120, 220, 0.18);
    border-radius: 14px;
    padding: 12px;
}
.card {
    background: rgba(0,0,0,0.03);
    border: 1px solid rgba(0,0,0,0.06);
    border-radius: 12px;
    padding: 10px;
}
.tab-bar {
    background: rgba(0,0,0,0.04);
    border: 1px solid rgba(0,0,0,0.07);
    border-radius: 12px;
    padding: 4px;
}
.tab-btn {
    background: transparent; border: none; border-radius: 8px;
    color: rgba(68,74,112,0.82); padding: 6px 12px; font-size: 11px; font-weight: bold;
}
.tab-btn:hover { background: rgba(0,0,0,0.07); color: rgba(26,38,94,0.96); }
.tab-active {
    background: rgba(84,111,243,0.14); border-radius: 8px; color: #2746b6;
    padding: 6px 12px; font-size: 11px; font-weight: bold; border: 1px solid rgba(84,111,243,0.28);
}
.pkey { color: rgba(50,50,90,0.8); font-size: 11px; font-family: monospace; font-weight: bold; }
.pval { font-size: 11px; font-family: monospace; }
.hero-title { color: rgba(22,22,36,0.96); font-size: 16px; font-weight: 800; }
.hero-sub { color: rgba(40,50,95,0.74); font-size: 11px; }
.metric-value { color: rgba(22,22,36,0.96); font-size: 16px; font-weight: 800; }
.metric-label { color: rgba(80,80,120,0.66); font-size: 10px; letter-spacing: 1px; }
.pill {
    background: rgba(0,0,0,0.05);
    border: 1px solid rgba(0,0,0,0.08);
    border-radius: 999px;
    color: rgba(30,30,70,0.92);
    padding: 3px 8px;
    font-size: 10px;
    font-weight: bold;
}
.pill-good {
    background: rgba(0,132,61,0.10);
    border: 1px solid rgba(0,132,61,0.20);
    color: #00843d;
}
.pill-warn {
    background: rgba(184,92,0,0.10);
    border: 1px solid rgba(184,92,0,0.20);
    color: #b85c00;
}
.pill-bad {
    background: rgba(183,28,28,0.10);
    border: 1px solid rgba(183,28,28,0.18);
    color: #b71c1c;
}
.ok   { color: #00843d; }
.warn { color: #b85c00; }
.err  { color: #b71c1c; }
.nokey { color: rgba(100,100,140,0.55); }
.upd  { color: rgba(90,90,120,0.55); font-size: 9px; }
.subdued { color: rgba(85,95,125,0.86); font-size: 10px; }
.btn {
    background: rgba(0,0,0,0.06); border: 1px solid rgba(0,0,0,0.10);
    border-radius: 8px; color: rgba(30,30,70,0.9); padding: 4px 10px; font-size: 11px;
}
.btn:hover { background: rgba(0,0,0,0.11); }
.btn-primary {
    background: rgba(84,111,243,0.14);
    border: 1px solid rgba(84,111,243,0.24);
    color: #2746b6;
    font-weight: bold;
}
.btn-good {
    background: rgba(0,132,61,0.10);
    border: 1px solid rgba(0,132,61,0.20);
    color: #00843d;
    font-weight: bold;
}
.btn-warn {
    background: rgba(184,92,0,0.10);
    border: 1px solid rgba(184,92,0,0.20);
    color: #b85c00;
    font-weight: bold;
}
.btn-bad {
    background: rgba(183,28,28,0.10);
    border: 1px solid rgba(183,28,28,0.18);
    color: #b71c1c;
    font-weight: bold;
}
.btn-ghost {
    background: rgba(0,0,0,0.04);
    border: 1px solid rgba(0,0,0,0.08);
    color: rgba(55,55,95,0.84);
}
.btn-ghost:hover { background: rgba(0,0,0,0.08); }
.theme-btn {
    min-width: 44px;
    min-height: 32px;
}
.btn-router-on {
    background: rgba(0,132,61,0.12); border: 1px solid rgba(0,132,61,0.35);
    border-radius: 8px; color: #00843d; padding: 4px 10px; font-size: 11px; font-weight: bold;
}
.btn-router-off {
    background: rgba(183,28,28,0.10); border: 1px solid rgba(183,28,28,0.30);
    border-radius: 8px; color: #b71c1c; padding: 4px 10px; font-size: 11px; font-weight: bold;
}
separator { background: rgba(0,0,0,0.07); min-height: 1px; margin: 2px 0; }
progressbar trough { background: rgba(0,0,0,0.08); border-radius: 4px; min-height: 5px; min-width: 140px; }
progressbar progress { background: #00843d; border-radius: 4px; }
progressbar.warn progress { background: #b85c00; }
progressbar.crit progress { background: #b71c1c; }
progressbar.blue progress { background: #0288d1; }
"""


# ── SNI icon ───────────────────────────────────────────────────────────

def _make_sni_icon(all_ok, any_ok):
    import dbus
    from PIL import Image, ImageDraw
    SIZE = 22
    img  = Image.new("RGBA", (SIZE, SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    if all_ok:
        ring = (0, 200, 100, 80);  fill = (0, 210, 110, 240)
    elif any_ok:
        ring = (255, 140, 0, 80);  fill = (255, 150, 0, 240)
    else:
        ring = (210, 50, 50, 80);  fill = (220, 60, 60, 240)
    draw.ellipse([0, 0, SIZE-1, SIZE-1], fill=ring)
    draw.ellipse([2, 2, SIZE-3, SIZE-3], fill=fill)
    draw.ellipse([8, 5, 13, 9], fill=(255, 255, 255, 180))
    pixels = list(img.convert("RGBA").getdata())
    argb = bytearray()
    for r, g, b, a in pixels:
        argb += bytes([a, r, g, b])
    return dbus.Array(
        [dbus.Struct((dbus.Int32(SIZE), dbus.Int32(SIZE),
             dbus.Array(list(argb), signature="y")), signature="iiay")],
        signature="(iiay)")


# ── GTK3 glass tray ────────────────────────────────────────────────────

def gnome_tray():
    import gi
    gi.require_version("Gtk", "3.0")
    gi.require_version("Gdk", "3.0")
    import dbus
    import dbus.service
    import dbus.mainloop.glib
    from gi.repository import Gtk, Gdk, GLib, Pango

    dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)

    persisted_alert_state = get_alert_state()
    state = {
        "data": {}, "gpu": {}, "updated": "never",
        "dark": True, "visible": False, "tab": "overview",
        "refreshing": False,
        "usage_refreshing": False,
        "live_usage": None,
        "last_down_signature": (),
        "alert_state": persisted_alert_state,
        "flash": "",
        "flash_tone": "good",
        "action_log": [],
        "event_filter": get_event_filter(),
        "event_page": 0,
        "provider_tests": load_codexbar_state().get("provider_tests", {}),
        "usage_snapshot": load_usage_snapshot(),
        "testing_providers": set(),
    }

    css_provider = Gtk.CssProvider()

    def apply_css():
        css_provider.load_from_data(CSS_DARK if state["dark"] else CSS_LIGHT)
        Gtk.StyleContext.add_provider_for_screen(
            Gdk.Screen.get_default(), css_provider,
            Gtk.STYLE_PROVIDER_PRIORITY_APPLICATION)

    def do_refresh(notify=False):
        data = get_all_status()
        gpu  = get_gpu_info()
        state["data"]    = data
        state["gpu"]     = gpu
        state["updated"] = time.strftime("%H:%M:%S")

    def collect_down_components(data):
        down = []
        for key, value in (data or {}).items():
            if key == "daemons" and isinstance(value, dict):
                for daemon_id, daemon_status in value.items():
                    if not isinstance(daemon_status, dict):
                        continue
                    if daemon_status.get("ok") is False and not daemon_status.get("needs_setup"):
                        down.append(str((daemon_status.get("detail") or daemon_id)).split("—", 1)[0].strip() or daemon_id)
                continue
            if not isinstance(value, dict):
                continue
            if value.get("ok") is False and not value.get("nokey"):
                down.append(key.upper())
        return tuple(sorted(set(down)))

    def maybe_notify_down(data):
        down = collect_down_components(data)
        previous = tuple((state.get("alert_state") or {}).get("down_signature") or state.get("last_down_signature") or ())
        state["last_down_signature"] = down
        alert_state = merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], state.get("alert_state") or {})
        now = int(time.time())
        if int(alert_state.get("mutedUntil") or 0) > now:
            alert_state["down_signature"] = list(down)
            state["alert_state"] = alert_state
            save_alert_state(alert_state)
            return
        if not down:
            if previous:
                append_event("alert.recovered", f"Recovered: {', '.join(previous)}", "good", components=list(previous))
            alert_state["down_signature"] = []
            alert_state["ack_signature"] = []
            alert_state["lastNotifiedAt"] = 0
            state["alert_state"] = alert_state
            save_alert_state(alert_state)
            return
        if down == previous and (now - int(alert_state.get("lastNotifiedAt") or 0)) < ALERT_NOTIFY_COOLDOWN_SECONDS:
            return
        if list(down) == list(alert_state.get("ack_signature") or []) and (now - int(alert_state.get("lastNotifiedAt") or 0)) < ALERT_NOTIFY_COOLDOWN_SECONDS:
            return
        if down != previous:
            append_event("alert.down", f"Down: {', '.join(down)}", "bad", components=list(down))
        subprocess.Popen([
            "notify-send", "-a", "CodexBar", "⚠ OpenClaw Alert",
            f"Down: {', '.join(down)}",
            "-u", "normal", "-t", "5000",
        ], stderr=subprocess.DEVNULL)
        alert_state["down_signature"] = list(down)
        alert_state["ack_signature"] = list(down)
        alert_state["lastNotifiedAt"] = now
        state["alert_state"] = alert_state
        save_alert_state(alert_state)

    def acknowledge_current_alerts():
        alert_state = merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], state.get("alert_state") or {})
        alert_state["ack_signature"] = list(alert_state.get("down_signature") or [])
        alert_state["lastNotifiedAt"] = int(time.time())
        state["alert_state"] = alert_state
        save_alert_state(alert_state)
        set_flash("Current alerts acknowledged", "good", timeout=4)

    def mute_alerts_for(seconds):
        alert_state = merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], state.get("alert_state") or {})
        alert_state["mutedUntil"] = int(time.time()) + int(seconds)
        alert_state["lastNotifiedAt"] = int(time.time())
        state["alert_state"] = alert_state
        save_alert_state(alert_state)
        set_flash(f"Alerts muted for {int(seconds // 60)}m", "warn", timeout=4)

    def clear_alert_mute():
        alert_state = merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], state.get("alert_state") or {})
        alert_state["mutedUntil"] = 0
        state["alert_state"] = alert_state
        save_alert_state(alert_state)
        set_flash("Alert mute cleared", "good", timeout=4)

    def publish_refresh():
        menu = state.get("menu")
        if menu is not None:
            menu._rev += 1
            menu.LayoutUpdated(menu._rev, 0)
        sni = state.get("sni")
        if sni is not None:
            sni.NewTitle()
            sni.NewIcon()

    def set_flash(message, tone="good", timeout=5):
        state["flash"] = str(message or "").strip()
        state["flash_tone"] = tone
        if state["visible"]:
            rebuild()
            win.show_all()
            reposition()
        if timeout:
            def clear_flash():
                if state.get("flash") == message:
                    state["flash"] = ""
                    if state["visible"]:
                        rebuild()
                        win.show_all()
                        reposition()
                return False
            GLib.timeout_add_seconds(timeout, clear_flash)

    def record_action(message, tone="good"):
        entry = {
            "message": str(message or "").strip(),
            "tone": tone,
            "time": time.strftime("%H:%M:%S"),
        }
        state["action_log"] = [entry, *(state.get("action_log") or [])][:8]
        append_event("ui.action", entry["message"], tone, time=entry["time"])
        if state["visible"]:
            rebuild()
            win.show_all()
            reposition()

    def compact_detail_text(text, limit=72):
        value = re.sub(r"\s+", " ", str(text or "").strip())
        if len(value) <= limit:
            return value
        return value[: max(0, limit - 1)].rstrip() + "…"

    def sync_route_ui():
        if state["visible"]:
            rebuild()
            win.show_all()
            reposition()

    def run_provider_smoke_test_async(provider_key):
        if provider_key in state["testing_providers"]:
            set_flash(f"{provider_key} smoke test already running", "warn", timeout=3)
            return
        state["testing_providers"].add(provider_key)
        state["provider_tests"][provider_key] = {
            "ok": None,
            "detail": "Running smoke test…",
            "model": pick_provider_smoke_model(provider_key),
            "checkedAt": time.strftime("%H:%M:%S"),
        }
        set_flash(f"Testing {provider_key}…", "warn", timeout=3)
        sync_route_ui()

        def worker():
            result = run_provider_smoke_test(provider_key)

            def finish():
                state["testing_providers"].discard(provider_key)
                state["provider_tests"][provider_key] = {
                    **result,
                    "checkedAt": time.strftime("%H:%M:%S"),
                }
                persist_provider_test_result(provider_key, state["provider_tests"][provider_key])
                tone = "good" if result.get("ok") else "bad"
                model_name = short_model_name(result.get("model") or provider_key)
                set_flash(f"{provider_key} smoke {'passed' if result.get('ok') else 'failed'}", tone, timeout=4)
                record_action(f"{provider_key}: {model_name} {'OK' if result.get('ok') else 'FAILED'}", tone)
                request_refresh(False, rebuild_after=False)
                sync_route_ui()
                return False

            GLib.idle_add(finish)

        threading.Thread(target=worker, daemon=True).start()

    def run_all_provider_smoke_tests_async():
        providers = ("nim", "gemini", "openai-codex", "ollama")
        if any(provider in state["testing_providers"] for provider in providers):
            set_flash("Smoke tests already running", "warn", timeout=3)
            return

        def worker():
            for provider_key in providers:
                GLib.idle_add(lambda p=provider_key: state["testing_providers"].add(p) or state["provider_tests"].update({
                    p: {
                        "ok": None,
                        "detail": "Running smoke test…",
                        "model": pick_provider_smoke_model(p),
                        "checkedAt": time.strftime("%H:%M:%S"),
                    }
                }) or sync_route_ui() or False)
                result = run_provider_smoke_test(provider_key)

                def finish_one(p=provider_key, res=result):
                    state["testing_providers"].discard(p)
                    state["provider_tests"][p] = {
                        **res,
                        "checkedAt": time.strftime("%H:%M:%S"),
                    }
                    persist_provider_test_result(p, state["provider_tests"][p])
                    tone = "good" if res.get("ok") else "bad"
                    record_action(f"{p}: {short_model_name(res.get('model') or p)} {'OK' if res.get('ok') else 'FAILED'}", tone)
                    sync_route_ui()
                    return False

                GLib.idle_add(finish_one)
            GLib.idle_add(lambda: set_flash("All smoke tests finished", "good", timeout=4) or False)

        threading.Thread(target=worker, daemon=True).start()

    def cleanup_legacy_router_async():
        changed = cleanup_legacy_router_refs()
        if changed:
            restart_openclaw_stack()
            set_flash("Removed legacy router Codex refs", "good", timeout=4)
            record_action("Legacy router refs cleaned", "good")
            GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
        else:
            set_flash("No legacy router refs found", "warn", timeout=3)
            record_action("Legacy router refs already clean", "warn")
        sync_route_ui()

    def save_baseline_async():
        baseline = save_current_baseline()
        set_flash(f"Saved baseline: {short_model_name(baseline.get('default'))}", "good", timeout=4)
        record_action(f"Baseline saved: {short_model_name(baseline.get('default'))}", "good")
        sync_route_ui()

    def restore_baseline_async():
        if not restore_saved_baseline():
            set_flash("No saved baseline to restore", "warn", timeout=4)
            record_action("Baseline restore failed", "bad")
            sync_route_ui()
            return
        restart_openclaw_stack()
        set_flash("Baseline restored", "good", timeout=4)
        record_action("Baseline restored", "good")
        sync_route_ui()
        GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])

    def create_restore_point_async():
        meta = create_restore_point("manual-ui", include_credentials=False)
        set_flash(f"Restore point created {meta['id']}", "good", timeout=4)
        record_action(f"Restore point {meta['id']} created", "good")
        sync_route_ui()

    def restore_latest_restore_point_async():
        points = list_restore_points(1)
        if not points:
            set_flash("No restore points available", "warn", timeout=4)
            record_action("Restore point restore failed", "bad")
            sync_route_ui()
            return
        ok, detail = restore_restore_point(points[0]["id"])
        if not ok:
            set_flash(detail, "bad", timeout=5)
            record_action("Restore point restore failed", "bad")
            sync_route_ui()
            return
        restart_openclaw_stack()
        set_flash(f"Restored {points[0]['id']}", "warn", timeout=4)
        record_action(f"Restored {points[0]['id']}", "warn")
        sync_route_ui()
        GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])

    def request_refresh(notify=False, rebuild_after=True):
        if state["refreshing"]:
            return
        state["refreshing"] = True
        state["updated"] = "refreshing…"
        set_flash("Refreshing runtime state…", "warn", timeout=2)
        if rebuild_after and state["visible"]:
            rebuild()
            win.show_all()
            reposition()

        def worker():
            data = get_all_status()
            gpu = get_gpu_info()

            def finish():
                state["data"] = data
                state["gpu"] = gpu
                state["updated"] = time.strftime("%H:%M:%S")
                state["refreshing"] = False
                set_flash("Refresh complete", "good", timeout=3)
                if notify:
                    maybe_notify_down(data)
                publish_refresh()
                if rebuild_after and state["visible"]:
                    rebuild()
                    win.show_all()
                    reposition()
                return False

            GLib.idle_add(finish)

        threading.Thread(target=worker, daemon=True).start()

    def request_full_refresh(notify=False, rebuild_after=True):
        request_refresh(notify=notify, rebuild_after=rebuild_after)
        request_usage_refresh(rebuild_after=rebuild_after and state.get("tab") in {"accounts", "spend"})

    def request_usage_refresh(rebuild_after=True):
        if state.get("usage_refreshing"):
            return
        state["usage_refreshing"] = True

        def worker():
            usage_summary = None
            try:
                usage_summary = get_usage_summary()
            except Exception:
                usage_summary = None

            def finish():
                if usage_summary:
                    state["live_usage"] = usage_summary
                    state["usage_snapshot"] = build_usage_snapshot(usage_summary)
                    save_usage_snapshot(usage_summary)
                state["usage_refreshing"] = False
                if rebuild_after and state["visible"]:
                    rebuild()
                    win.show_all()
                    reposition()
                return False

            GLib.idle_add(finish)

        threading.Thread(target=worker, daemon=True).start()

    # ── Window ──────────────────────────────────────────────────────────
    win = Gtk.Window()
    win.set_decorated(False)
    win.set_keep_above(True)
    win.set_skip_taskbar_hint(True)
    win.set_skip_pager_hint(True)
    win.set_type_hint(Gdk.WindowTypeHint.UTILITY)
    win.set_resizable(False)
    screen = win.get_screen()
    visual = screen.get_rgba_visual()
    if visual and screen.is_composited():
        win.set_visual(visual)

    def hide_popup():
        win.hide()
        state["visible"] = False

    def on_focus_out(_widget, _event):
        # Tray-style popups were closing before click handlers and dialogs
        # completed on some desktops. Keep the panel open and let explicit
        # actions (Esc, Close, tray toggle) dismiss it reliably.
        return False

    def on_key_press(_widget, event):
        if event.keyval == Gdk.KEY_Escape:
            hide_popup()
            return True
        return False

    win.connect("focus-out-event", on_focus_out)
    win.connect("key-press-event", on_key_press)

    frame = Gtk.Box(orientation=Gtk.Orientation.VERTICAL)
    frame.get_style_context().add_class("glass")
    frame.set_margin_top(6); frame.set_margin_bottom(6)
    frame.set_margin_start(6); frame.set_margin_end(6)
    win.add(frame)

    scroller = Gtk.ScrolledWindow()
    scroller.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
    scroller.set_overlay_scrolling(True)
    if hasattr(scroller, "set_propagate_natural_width"):
        scroller.set_propagate_natural_width(True)
    if hasattr(scroller, "set_propagate_natural_height"):
        scroller.set_propagate_natural_height(False)
    frame.pack_start(scroller, True, True, 0)

    viewport = Gtk.Viewport()
    viewport.set_shadow_type(Gtk.ShadowType.NONE)
    scroller.add(viewport)

    body = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=5)
    body.set_margin_top(12); body.set_margin_bottom(12)
    body.set_margin_start(16); body.set_margin_end(16)
    viewport.add(body)

    def sc(widget, *classes):
        for c in classes: widget.get_style_context().add_class(c)
        return widget

    def lbl(text, *classes, xalign=0.0, wrap=True, max_width=None, ellipsize=False):
        w = Gtk.Label(label=text); w.set_xalign(xalign)
        w.set_line_wrap(bool(wrap))
        if max_width is not None:
            w.set_max_width_chars(int(max_width))
        if ellipsize:
            w.set_ellipsize(Pango.EllipsizeMode.END)
            w.set_single_line_mode(True)
        return sc(w, *classes)

    def section_card():
        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        box.set_margin_top(2); box.set_margin_bottom(2)
        box.set_margin_start(2); box.set_margin_end(2)
        return sc(box, "card")

    def action_button(text, handler, primary=False, tone=None):
        classes = ["btn-primary" if primary else "btn"]
        if tone:
            classes.append(f"btn-{tone}")
        btn = sc(Gtk.Button(label=text), *classes)
        btn.set_relief(Gtk.ReliefStyle.NONE)
        btn.connect("clicked", handler)
        return btn

    def pill(text, tone="neutral"):
        tone_class = {
            "good": "pill-good",
            "warn": "pill-warn",
            "bad": "pill-bad",
        }.get(tone, "")
        return lbl(text, "pill", tone_class)

    def show_notice_dialog(title, message):
        dlg = Gtk.MessageDialog(
            transient_for=win,
            modal=True,
            destroy_with_parent=True,
            message_type=Gtk.MessageType.INFO,
            buttons=Gtk.ButtonsType.OK,
            text=title,
        )
        dlg.format_secondary_text(message)
        dlg.run()
        dlg.destroy()

    def show_text_value_dialog(title, message, initial_value="", placeholder="", secret=False):
        dlg = Gtk.Dialog(title=title, transient_for=win,
                         modal=True, destroy_with_parent=True)
        dlg.add_buttons("Cancel", Gtk.ResponseType.CANCEL,
                        "Save", Gtk.ResponseType.OK)
        dlg.set_default_response(Gtk.ResponseType.OK)
        content = dlg.get_content_area()
        content.set_spacing(10)
        content.set_margin_top(16); content.set_margin_bottom(8)
        content.set_margin_start(16); content.set_margin_end(16)
        content.add(lbl(message, "pval"))
        entry = Gtk.Entry()
        entry.set_visibility(not secret)
        entry.set_placeholder_text(placeholder)
        entry.set_activates_default(True)
        if initial_value:
            entry.set_text(initial_value)
        content.add(entry)
        dlg.show_all()
        resp = dlg.run()
        value = entry.get_text().strip() if resp == Gtk.ResponseType.OK else None
        dlg.destroy()
        return value

    def choose_model_dialog(title, message, options, current_value=""):
        dlg = Gtk.Dialog(title=title, transient_for=win, modal=True, destroy_with_parent=True)
        dlg.add_buttons("Cancel", Gtk.ResponseType.CANCEL, "Select", Gtk.ResponseType.OK)
        dlg.set_default_response(Gtk.ResponseType.OK)
        content = dlg.get_content_area()
        content.set_spacing(10)
        content.set_margin_top(16); content.set_margin_bottom(8)
        content.set_margin_start(16); content.set_margin_end(16)
        content.add(lbl(message, "pval"))
        combo = Gtk.ComboBoxText()
        combo.append("", "None")
        for option in options:
            combo.append(option, option)
        combo.set_active_id(current_value if current_value in options else "")
        content.add(combo)
        dlg.show_all()
        resp = dlg.run()
        value = combo.get_active_id() if resp == Gtk.ResponseType.OK else None
        dlg.destroy()
        return value

    def show_policy_dialog(policy_key, options):
        policy = get_routing_policy()
        current = merge_dict({}, policy.get(policy_key) or {})
        dlg = Gtk.Dialog(title=f"Edit {policy_key.title()} policy", transient_for=win, modal=True, destroy_with_parent=True)
        dlg.add_buttons("Cancel", Gtk.ResponseType.CANCEL, "Save", Gtk.ResponseType.OK)
        dlg.set_default_response(Gtk.ResponseType.OK)
        content = dlg.get_content_area()
        content.set_spacing(10)
        content.set_margin_top(16); content.set_margin_bottom(8)
        content.set_margin_start(16); content.set_margin_end(16)
        content.add(lbl(f"Set the preferred lane and enforcement rules for {policy_key} tasks.", "pval"))
        grid = Gtk.Grid(column_spacing=10, row_spacing=8)
        grid.attach(lbl("Preferred lane", "pkey"), 0, 0, 1, 1)
        combo = Gtk.ComboBoxText()
        for option in options:
            combo.append(option, option)
        combo.set_active_id(str(current.get("lane") or ""))
        grid.attach(combo, 1, 0, 1, 1)
        strict_btn = Gtk.CheckButton(label="Strict routing")
        strict_btn.set_active(bool(current.get("strict")))
        grid.attach(strict_btn, 0, 1, 2, 1)
        sidecar_btn = Gtk.CheckButton(label="Allow sidecar / helper lanes")
        sidecar_btn.set_active(bool(current.get("sidecar")))
        grid.attach(sidecar_btn, 0, 2, 2, 1)
        content.add(grid)
        dlg.show_all()
        resp = dlg.run()
        if resp == Gtk.ResponseType.OK and combo.get_active_id():
            policy[policy_key] = {
                "lane": combo.get_active_id(),
                "strict": strict_btn.get_active(),
                "sidecar": sidecar_btn.get_active(),
            }
            save_routing_policy(policy)
            set_flash(f"{policy_key.title()} policy updated", "good", timeout=4)
            record_action(f"Policy: {policy_key} -> {short_model_name(combo.get_active_id())}", "good")
            sync_route_ui()
        dlg.destroy()

    def show_pricing_dialog(model_ref):
        registry = get_pricing_registry()
        row = merge_dict({}, (registry.get("models") or {}).get(model_ref) or {})
        dlg = Gtk.Dialog(title=f"Edit pricing: {short_model_name(model_ref)}", transient_for=win, modal=True, destroy_with_parent=True)
        dlg.add_buttons("Cancel", Gtk.ResponseType.CANCEL, "Save", Gtk.ResponseType.OK)
        dlg.set_default_response(Gtk.ResponseType.OK)
        content = dlg.get_content_area()
        content.set_spacing(10)
        content.set_margin_top(16); content.set_margin_bottom(8)
        content.set_margin_start(16); content.set_margin_end(16)
        content.add(lbl("Per-1M-token pricing registry for metered models. Leave zero to keep as metered/unpriced.", "pval"))
        grid = Gtk.Grid(column_spacing=10, row_spacing=8)
        entries = {}
        fields = [
            ("Input", "input"),
            ("Output", "output"),
            ("Cache Read", "cacheRead"),
            ("Cache Write", "cacheWrite"),
            ("Source URL / note", "source"),
            ("Last verified", "lastVerifiedAt"),
        ]
        for idx, (title, key) in enumerate(fields):
            grid.attach(lbl(title, "pkey"), 0, idx, 1, 1)
            entry = Gtk.Entry()
            entry.set_text(str(row.get(key) or ""))
            grid.attach(entry, 1, idx, 1, 1)
            entries[key] = entry
        content.add(grid)
        dlg.show_all()
        resp = dlg.run()
        if resp == Gtk.ResponseType.OK:
            registry.setdefault("models", {})[model_ref] = {
                "input": entries["input"].get_text().strip() or "0",
                "output": entries["output"].get_text().strip() or "0",
                "cacheRead": entries["cacheRead"].get_text().strip() or "0",
                "cacheWrite": entries["cacheWrite"].get_text().strip() or "0",
                "source": entries["source"].get_text().strip() or "manual",
                "lastVerifiedAt": entries["lastVerifiedAt"].get_text().strip() or datetime.now().strftime("%Y-%m-%d"),
            }
            save_pricing_registry(registry)
            set_flash(f"Pricing saved for {short_model_name(model_ref)}", "good", timeout=4)
            record_action(f"Pricing updated: {short_model_name(model_ref)}", "good")
            sync_route_ui()
        dlg.destroy()

    def show_env_key_dialog(env_key, title, placeholder, initial_message=None, on_saved=None):
        dlg = Gtk.Dialog(title=title, transient_for=win,
                         modal=True, destroy_with_parent=True)
        dlg.add_buttons("Cancel", Gtk.ResponseType.CANCEL,
                        "Save & Restart", Gtk.ResponseType.OK)
        dlg.set_default_response(Gtk.ResponseType.OK)
        content = dlg.get_content_area()
        content.set_spacing(10)
        content.set_margin_top(16); content.set_margin_bottom(8)
        content.set_margin_start(16); content.set_margin_end(16)
        default_messages = {
            "OPENAI_API_KEY": "Enter your OpenAI API key.\nSaved to ~/.openclaw/credentials/env\nGateway restarts automatically.",
            "NVIDIA_API_KEY": "Enter your NVIDIA NIM API key.\nSaved to ~/.openclaw/credentials/env\nGateway restarts automatically.",
            "GEMINI_API_KEY": "Enter your Gemini API key.\nSaved to ~/.openclaw/credentials/env\nGateway restarts automatically.",
        }
        message = initial_message or default_messages.get(
            env_key,
            "Enter the provider key.\nSaved to ~/.openclaw/credentials/env\nGateway restarts automatically.",
        )
        content.add(lbl(message, "pval"))
        entry = Gtk.Entry()
        entry.set_visibility(False)
        entry.set_placeholder_text(placeholder)
        entry.set_activates_default(True)
        key_lookup = {
            "OPENAI_API_KEY": "OPENAI",
            "NVIDIA_API_KEY": "NVIDIA",
            "GEMINI_API_KEY": "GEMINI",
        }
        existing = get_keys().get(key_lookup.get(env_key, ""), "")
        if existing and "REPLACE" not in existing:
            entry.set_text(existing)
        content.add(entry)
        dlg.show_all()
        resp = dlg.run()
        if resp == Gtk.ResponseType.OK:
            key = entry.get_text().strip()
            if key:
                create_restore_point(f"credential:{env_key}", include_credentials=True)
                save_env_file("~/.openclaw/credentials/env", {env_key: key})
                append_event("credential.save", f"Saved {env_key}", "warn", envKey=env_key)
                restart_openclaw_stack()
                if on_saved:
                    on_saved()
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
        dlg.destroy()

    def show_openai_key_dialog(initial_message=None, on_saved=None):
        show_env_key_dialog(
            "OPENAI_API_KEY",
            "Set OpenAI API Key",
            "sk-proj-...",
            initial_message=initial_message,
            on_saved=on_saved,
        )

    def handle_repair_action(action_key):
        if action_key == "gateway":
            subprocess.Popen(
                ["systemctl", "--user", "restart", "openclaw-gateway.service"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            set_flash("Restarting OpenClaw gateway…", "warn", timeout=4)
            GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
        elif action_key == "nim_key":
            show_env_key_dialog("NVIDIA_API_KEY", "Set NVIDIA NIM API Key", "nvapi-...")
        elif action_key == "gemini_key":
            show_env_key_dialog("GEMINI_API_KEY", "Set Gemini API Key", "AIza...")
        elif action_key == "openai_key":
            show_openai_key_dialog()
        elif action_key == "codex_login":
            subprocess.Popen(["x-terminal-emulator", "-e", "codex login"], stderr=subprocess.DEVNULL)
            set_flash("Opening Codex login in terminal…", "good", timeout=4)
        elif action_key == "open_ui":
            subprocess.Popen(["xdg-open", "http://127.0.0.1:18789"], stderr=subprocess.DEVNULL)
            set_flash("Opening OpenClaw control UI…", "good", timeout=4)

    def show_daemon_cookie_dialog(daemon_id):
        daemon_cfg = load_daemon_config().get("daemons", {}).get(daemon_id, {})
        env_key = str(daemon_cfg.get("cookie_env") or "").strip()
        if not env_key:
            show_notice_dialog("No token field", "This daemon does not have a configured cookie or token variable.")
            return
        current = load_env_file(str(OPENCLAW_CREDENTIALS)).get(env_key, "")
        value = show_text_value_dialog(
            f"Set {daemon_cfg.get('label', daemon_id)} Cookie",
            f"Paste the value to save as {env_key} in ~/.openclaw/credentials/env.",
            initial_value=current,
            placeholder="cookie or bearer token",
            secret=True,
        )
        if value:
            create_restore_point(f"daemon-cookie:{daemon_id}", include_credentials=True)
            save_env_file(str(OPENCLAW_CREDENTIALS), {env_key: value})
            append_build_note(f"Saved {env_key} for {daemon_cfg.get('label', daemon_id)} from CodexBar.")
            set_flash(f"Saved {env_key} for {daemon_cfg.get('label', daemon_id)}", "good", timeout=4)
            record_action(f"{daemon_cfg.get('label', daemon_id)} token saved", "good")
            show_notice_dialog("Saved", f"{env_key} saved. The daemon service will read it on next launch.")
            GLib.timeout_add_seconds(1, lambda: (request_full_refresh(False), False)[1])

    def show_daemon_config_dialog(daemon_id):
        daemon_bundle = load_daemon_config()
        daemon_cfg = daemon_bundle.get("daemons", {}).get(daemon_id, {})
        if not daemon_cfg:
            show_notice_dialog("Missing daemon", f"No daemon config found for {daemon_id}.")
            return
        if daemon_cfg.get("type") == "systemd":
            show_notice_dialog(
                daemon_cfg.get("label", daemon_id),
                "This daemon is managed by a fixed user systemd unit. Use the Start/Stop/Open buttons directly.",
            )
            return
        dlg = Gtk.Dialog(title=f"Configure {daemon_cfg.get('label', daemon_id)}", transient_for=win,
                         modal=True, destroy_with_parent=True)
        dlg.add_buttons("Cancel", Gtk.ResponseType.CANCEL,
                        "Save", Gtk.ResponseType.OK)
        dlg.set_default_response(Gtk.ResponseType.OK)
        content = dlg.get_content_area()
        content.set_spacing(10)
        content.set_margin_top(16); content.set_margin_bottom(8)
        content.set_margin_start(16); content.set_margin_end(16)
        content.add(lbl("Set the commands CodexBar should use. Detached commands like `docker compose up -d` work best.", "pval"))
        grid = Gtk.Grid(column_spacing=10, row_spacing=8)
        fields = [
            ("Start command", "start_cmd", "docker compose -f ~/path/compose.yml up -d"),
            ("Stop command", "stop_cmd", "docker compose -f ~/path/compose.yml down"),
            ("Status command", "status_cmd", "docker ps | rg nemoclaw"),
            ("Open URL", "url", "http://127.0.0.1:3000"),
        ]
        entries = {}
        for row, (label_text, key, placeholder) in enumerate(fields):
            grid.attach(lbl(label_text, "pkey"), 0, row, 1, 1)
            entry = Gtk.Entry()
            entry.set_placeholder_text(placeholder)
            entry.set_text(str(daemon_cfg.get(key) or ""))
            grid.attach(entry, 1, row, 1, 1)
            entries[key] = entry
        content.add(grid)
        dlg.show_all()
        resp = dlg.run()
        if resp == Gtk.ResponseType.OK:
            create_restore_point(f"daemon-config:{daemon_id}", include_credentials=False)
            for key, entry in entries.items():
                daemon_cfg[key] = entry.get_text().strip()
            daemon_bundle.setdefault("daemons", {})[daemon_id] = daemon_cfg
            save_daemon_config(daemon_bundle)
            if daemon_cfg.get("start_cmd"):
                write_command_daemon_unit(daemon_cfg)
            append_build_note(f"Updated daemon config for {daemon_cfg.get('label', daemon_id)} in CodexBar.")
            set_flash(f"Saved daemon config for {daemon_cfg.get('label', daemon_id)}", "good", timeout=4)
            record_action(f"{daemon_cfg.get('label', daemon_id)} config saved", "good")
            GLib.timeout_add_seconds(1, lambda: (request_full_refresh(False), False)[1])
        dlg.destroy()

    def trigger_daemon_action(daemon_id, action):
        ok, detail = control_daemon(daemon_id, action)
        if not ok:
            set_flash(f"{daemon_id} {action} failed", "bad", timeout=5)
            record_action(f"{daemon_id} {action} failed", "bad")
            show_notice_dialog("Daemon action failed", detail)
            return
        append_build_note(f"Ran `{action}` for {daemon_id} from CodexBar.")
        pretty = load_daemon_config().get("daemons", {}).get(daemon_id, {}).get("label", daemon_id)
        set_flash(f"{pretty}: {action} requested", "good", timeout=4)
        record_action(f"{pretty}: {action}", "good")
        GLib.timeout_add_seconds(2, lambda: (request_full_refresh(False), False)[1])

    def open_daemon_surface(daemon_id):
        daemon_cfg = load_daemon_config().get("daemons", {}).get(daemon_id, {})
        open_cmd = str(daemon_cfg.get("open_cmd") or "").strip()
        if open_cmd:
            subprocess.Popen(shell_command(open_cmd), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            set_flash(f"Opening {daemon_cfg.get('label', daemon_id)}…", "good", timeout=4)
            record_action(f"{daemon_cfg.get('label', daemon_id)} opened", "good")
            return
        url = str(daemon_cfg.get("url") or "").strip()
        if url:
            subprocess.Popen(["xdg-open", url], stderr=subprocess.DEVNULL)
            set_flash(f"Opening {daemon_cfg.get('label', daemon_id)} URL…", "good", timeout=4)
            record_action(f"{daemon_cfg.get('label', daemon_id)} URL opened", "good")
            return
        show_notice_dialog("No open action", f"{daemon_cfg.get('label', daemon_id)} does not have a configured surface to open.")

    def toggle_daemon_autostart(daemon_id, enabled):
        create_restore_point(f"daemon-autostart:{daemon_id}", include_credentials=False)
        ok, detail = set_daemon_enabled(daemon_id, enabled)
        if not ok:
            set_flash(f"{daemon_id} autostart change failed", "bad", timeout=5)
            record_action(f"{daemon_id} autostart failed", "bad")
            show_notice_dialog("Autostart change failed", detail)
            return
        append_build_note(f"{'Enabled' if enabled else 'Disabled'} autostart for {daemon_id} from CodexBar.")
        pretty = load_daemon_config().get("daemons", {}).get(daemon_id, {}).get("label", daemon_id)
        set_flash(f"{pretty}: autostart {'enabled' if enabled else 'disabled'}", "good", timeout=4)
        record_action(f"{pretty}: autostart {'enabled' if enabled else 'disabled'}", "good")
        GLib.timeout_add_seconds(1, lambda: (request_full_refresh(False), False)[1])

    def rebuild():
        for child in body.get_children():
            body.remove(child)

        data    = state["data"]
        gpu     = state["gpu"]
        updated = state["updated"]
        dark    = state["dark"]
        tab     = state["tab"]
        router_on = get_router_enabled()
        current_default_model = get_default_model_ref()
        overview = build_overview_snapshot(data)
        repair_actions = build_repair_actions(data)
        usage_fallback = state.get("usage_snapshot") or load_usage_snapshot() or {
            "today": {},
            "week": {},
            "month": {},
            "projection": 0.0,
            "recent_days": [],
            "sessions": [],
            "entries": [],
            "daily": [],
        }
        if tab in {"accounts", "spend"}:
            usage = state.get("live_usage") or usage_fallback
            if state["visible"] and not state.get("live_usage") and not state.get("usage_refreshing"):
                request_usage_refresh(rebuild_after=True)
        else:
            usage = usage_fallback
        agent_models = get_agent_model_map()
        daemon_data = data.get("daemons", {})
        provider_tests = state.get("provider_tests") or {}
        scheduled_smoke = load_codexbar_state().get("scheduled_smoke_tests") or {}
        event_filter = state.get("event_filter") or get_event_filter()
        all_filtered_events = get_filtered_events(60, event_filter)
        page_size = 8
        max_page = max(0, (len(all_filtered_events) - 1) // page_size) if all_filtered_events else 0
        event_page = max(0, min(int(state.get("event_page") or 0), max_page))
        state["event_page"] = event_page
        recent_events = all_filtered_events[event_page * page_size:(event_page + 1) * page_size]
        restore_points = list_restore_points(6)
        all_ok = data.get("openclaw", {}).get("ok") and data.get("codex", {}).get("ok")
        degraded = not all_ok or not data.get("ollama", {}).get("ok")
        current_alert_state = merge_dict(DEFAULT_CODEXBAR_STATE["alert_state"], state.get("alert_state") or {})
        muted_until = int(current_alert_state.get("mutedUntil") or 0)
        alerts_muted = muted_until > int(time.time())

        # ── Title row ───────────────────────────────────────────────────
        hdr = sc(Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=10), "topbar")
        title_wrap = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        title_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=1)
        title_box.pack_start(lbl("CODEXBAR · OPENCLAW", "ttl"), False, False, 0)
        title_box.pack_start(lbl("Realtime lane control for your local stack", "subttl"), False, False, 0)
        title_wrap.pack_start(title_box, False, False, 0)
        meta_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        meta_row.pack_start(pill("Healthy" if not degraded else "Attention", "good" if not degraded else "warn"), False, False, 0)
        meta_row.pack_start(pill(tab.title(), "good" if tab == "overview" else "warn"), False, False, 0)
        meta_row.pack_start(lbl(f"Updated {state['updated']}", "topbar-meta"), False, False, 0)
        title_wrap.pack_start(meta_row, False, False, 0)
        hdr.pack_start(title_wrap, True, True, 0)
        tbtn = sc(Gtk.Button(label="☀" if dark else "☾"), "btn", "theme-btn")
        tbtn.set_relief(Gtk.ReliefStyle.NONE)
        def toggle_theme(b):
            state["dark"] = not state["dark"]; apply_css(); rebuild()
        tbtn.connect("clicked", toggle_theme)
        hdr.pack_end(tbtn, False, False, 0)
        body.pack_start(hdr, False, False, 0)

        # ── Tab bar ─────────────────────────────────────────────────────
        tab_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=4)
        tab_row.get_style_context().add_class("tab-bar")
        for icon, name, key in VIEW_MODES:
            is_active = (tab == key)
            btn = Gtk.Button(label=f"{icon} {name}")
            btn.set_relief(Gtk.ReliefStyle.NONE)
            btn.set_hexpand(True)
            if is_active:
                btn.get_style_context().add_class("tab-active")
            else:
                btn.get_style_context().add_class("tab-btn")
            def on_tab(b, k=key):
                state["tab"] = k
                rebuild()
                win.show_all()
                reposition()
                if k in {"accounts", "spend"}:
                    request_usage_refresh(rebuild_after=True)
            btn.connect("clicked", on_tab)
            tab_row.pack_start(btn, True, True, 0)
        body.pack_start(tab_row, False, False, 2)
        body.pack_start(Gtk.Separator(), False, False, 0)

        # ── Hero card ────────────────────────────────────────────────────
        hero = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=6)
        hero.get_style_context().add_class("hero")
        hero.pack_start(lbl(overview["route_mode"], "hero-title"), False, False, 0)
        hero.pack_start(lbl(overview["default_model"], "hero-sub"), False, False, 0)
        hero_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        hero_row.pack_start(
            pill(overview["health"], "good" if not degraded else "warn"),
            False,
            False,
            0,
        )
        hero_row.pack_start(
            pill("Router Armed" if router_on else "Router Idle", "good" if router_on else "bad"),
            False,
            False,
            0,
        )
        hero.pack_start(hero_row, False, False, 0)
        fallback_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        fallback_row.pack_start(pill("Primary", "good"), False, False, 0)
        fallback_row.pack_start(pill(short_model_name(current_default_model), "good"), False, False, 0)
        for fallback in get_fallback_chain()[:3]:
            fallback_row.pack_start(pill(short_model_name(fallback), "warn"), False, False, 0)
        hero.pack_start(fallback_row, False, False, 0)
        subagent_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
        subagent_row.pack_start(pill("Sub-agent", "good"), False, False, 0)
        subagent_row.pack_start(pill(short_model_name(get_subagent_model_ref() or current_default_model), "warn"), False, False, 0)
        hero.pack_start(subagent_row, False, False, 0)
        if state.get("flash"):
            hero.pack_start(pill(state["flash"], state.get("flash_tone", "good")), False, False, 0)
        if state["refreshing"]:
            hero.pack_start(lbl("Refreshing provider state…", "upd"), False, False, 0)
        body.pack_start(hero, False, False, 0)

        recent_actions = state.get("action_log") or []
        if recent_actions and tab in {"overview", "daemons", "ops"}:
            action_card = section_card()
            action_card.pack_start(lbl("Recent actions", "pkey"), False, False, 0)
            for entry in recent_actions[:4]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(entry["message"], "upd"), True, True, 0)
                row.pack_end(pill(entry["time"], entry.get("tone", "good")), False, False, 0)
                action_card.pack_start(row, False, False, 0)
            body.pack_start(action_card, False, False, 0)

        # ── Mode content ────────────────────────────────────────────────
        if tab == "overview":
            metrics = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
            for title, metric in [
                ("Today", usage["today"]),
                ("7D", usage["week"]),
                ("30D", usage["month"]),
            ]:
                card = section_card()
                card.pack_start(lbl(format_usage_metric_value(metric), "metric-value"), False, False, 0)
                card.pack_start(lbl(format_usage_metric_detail(title, metric), "metric-label"), False, False, 0)
                metrics.pack_start(card, True, True, 0)
            body.pack_start(metrics, False, False, 0)

            subagent_card = section_card()
            subagent_card.pack_start(lbl("Sub-agent runtime", "pkey"), False, False, 0)
            subagent_card.pack_start(lbl(f"Dedicated child lane: {short_model_name(get_subagent_model_ref() or current_default_model)}", "pval"), False, False, 0)
            subagent_card.pack_start(lbl("Spawned workers do not just inherit silently. The runtime governor writes the child lane choice into the audit log.", "upd", wrap=False, max_width=92, ellipsize=True), False, False, 0)
            body.pack_start(subagent_card, False, False, 0)

            route_card = section_card()
            route_card.pack_start(lbl("Live route timeline", "pkey"), False, False, 0)
            route_card.pack_start(lbl("Recent sessions with actual lane, policy target, and billing mode.", "upd"), False, False, 0)
            for row_data in build_route_timeline(usage["sessions"], limit=3):
                row = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
                head = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                head.pack_start(lbl(f"{row_data['agentId']} · {short_model_name(row_data['modelRef'])}", "pval"), False, False, 0)
                policy_tone = "good" if row_data["policyStatus"] == "match" else ("warn" if row_data["policyStatus"] == "soft-match" else "bad")
                head.pack_end(pill(usage_mode_label(row_data["billingMode"]), usage_mode_tone(row_data)), False, False, 0)
                head.pack_end(pill(f"{row_data['policyTarget']}:{row_data['policyStatus']}", policy_tone), False, False, 0)
                row.pack_start(head, False, False, 0)
                row.pack_start(lbl(f"{row_data['provider']} · {row_data['account']} · expected {short_model_name(row_data['policyExpected']) or 'none'} · {row_data['lastSeenLabel']}", "upd", wrap=False, max_width=90, ellipsize=True), False, False, 0)
                route_card.pack_start(row, False, False, 0)
            body.pack_start(route_card, False, False, 0)

            governor_card = section_card()
            governor_card.pack_start(lbl("Governor audit", "pkey"), False, False, 0)
            governor_card.pack_start(lbl("Recent runtime-governor decisions from live OpenClaw hooks.", "upd"), False, False, 0)
            governor_rows = get_runtime_governor_summary(limit=3)
            if governor_rows:
                for row_data in governor_rows:
                    row = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
                    head = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                    label = row_data["kind"].replace("route.", "").replace("subagent.", "subagent ")
                    tone = "good"
                    if row_data["kind"] == "route.blocked":
                        tone = "bad"
                    elif row_data["explicitOverride"] or row_data["outcome"] in {"session-override", "explicit-model"}:
                        tone = "warn"
                    head.pack_start(lbl(f"{row_data['agentId']} · {label}", "pval"), False, False, 0)
                    pill_text = short_model_name(row_data["lane"]) or short_model_name(f"{row_data['provider']}/{row_data['model']}" if row_data["provider"] and row_data["model"] else "")
                    if pill_text:
                        head.pack_end(pill(pill_text, tone), False, False, 0)
                    row.pack_start(head, False, False, 0)
                    detail = row_data["taskType"] or row_data["routeMode"] or row_data["outcome"] or "decision"
                    if row_data["reason"]:
                        detail = f"{detail} · {row_data['reason']}"
                    row.pack_start(lbl(detail, "upd", wrap=False, max_width=90, ellipsize=True), False, False, 0)
                    governor_card.pack_start(row, False, False, 0)
            else:
                governor_card.pack_start(lbl("No governor audit rows yet.", "upd"), False, False, 0)
            body.pack_start(governor_card, False, False, 0)

            live_card = section_card()
            live_card.pack_start(lbl("Live agents", "pkey"), False, False, 0)
            active_sessions = [entry for entry in usage["sessions"] if entry.get("active")]
            if active_sessions:
                top_tokens = max(1, max(int(entry.get("totalTokens") or 0) for entry in active_sessions[:2]))
                for entry in active_sessions[:2]:
                    row = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=4)
                    head = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                    head.pack_start(lbl(f"{entry['agentId']} · {short_model_name(entry['modelRef'])}", "pval"), False, False, 0)
                    head.pack_end(pill(usage_mode_label(entry.get("billingMode")), usage_mode_tone(entry)), False, False, 0)
                    row.pack_start(head, False, False, 0)
                    row.pack_start(lbl(f"{entry['provider']} · {entry['account']} · {entry['lastSeenLabel']}", "upd"), False, False, 0)
                    bar = Gtk.ProgressBar()
                    bar.set_fraction(min(1.0, float(entry.get("totalTokens") or 0) / float(top_tokens)))
                    bar.set_show_text(True)
                    cost_text = format_cost_short(entry.get("totalCostUSD") or 0.0) if float(entry.get("totalCostUSD") or 0.0) > 0 else usage_mode_label(entry.get("billingMode"))
                    bar.set_text(f"{format_tokens_short(entry.get('totalTokens') or 0)} tok · {cost_text}")
                    sc(bar, "blue")
                    row.pack_start(bar, False, False, 0)
                    live_card.pack_start(row, False, False, 0)
            else:
                live_card.pack_start(lbl("No active agent sessions in the last 6h.", "upd"), False, False, 0)
            body.pack_start(live_card, False, False, 0)

            recent_card = section_card()
            recent_card.pack_start(lbl("Recent sessions", "pkey"), False, False, 0)
            recent_sessions = usage["sessions"][:3]
            if recent_sessions:
                for entry in recent_sessions:
                    row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                    row.pack_start(lbl(f"{entry['agentId']} · {short_model_name(entry['modelRef'])}", "pval"), False, False, 0)
                    row.pack_start(lbl(f"{entry['lastSeenLabel']} · {format_tokens_short(entry.get('totalTokens') or 0)} tok", "upd"), True, True, 0)
                    tone = usage_mode_tone(entry)
                    if entry.get("errors"):
                        tone = "bad"
                    right = format_cost_short(entry.get("totalCostUSD") or 0.0) if float(entry.get("totalCostUSD") or 0.0) > 0 else usage_mode_label(entry.get("billingMode"))
                    row.pack_end(pill(right, tone), False, False, 0)
                    recent_card.pack_start(row, False, False, 0)
            else:
                recent_card.pack_start(lbl("No session history found in ~/.openclaw/agents yet.", "upd"), False, False, 0)
            body.pack_start(recent_card, False, False, 0)

            health_card = section_card()
            health_card.pack_start(lbl("System health", "pkey"), False, False, 0)
            health_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            for key, title in [
                ("openclaw", "Gateway"),
                ("codex", "Codex"),
                ("nim", "NIM"),
                ("gemini", "Gemini"),
                ("ollama", "Ollama"),
            ]:
                tone = "good" if data.get(key, {}).get("ok") else ("warn" if data.get(key, {}).get("nokey") else "bad")
                health_row.pack_start(pill(f"{title}:{data.get(key, {}).get('label', 'n/a')}", tone), False, False, 0)
            health_card.pack_start(health_row, False, False, 0)
            body.pack_start(health_card, False, False, 0)

            actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            actions.pack_start(action_button("Open Control UI", lambda _b: subprocess.Popen(["xdg-open", "http://127.0.0.1:18789"], stderr=subprocess.DEVNULL), True), True, True, 0)
            actions.pack_start(action_button("Refresh", lambda _b: request_full_refresh(True)), True, True, 0)
            body.pack_start(actions, False, False, 0)

            if repair_actions:
                repair_card = section_card()
                repair_card.pack_start(lbl("Quick repairs", "pkey"), False, False, 0)
                repair_card.pack_start(lbl("Show the most relevant fixes for the current degraded state.", "upd"), False, False, 0)
                repair_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
                for title, action_key in repair_actions:
                    repair_row.pack_start(action_button(title, lambda _b, k=action_key: handle_repair_action(k)), True, True, 0)
                repair_card.pack_start(repair_row, False, False, 0)
                body.pack_start(repair_card, False, False, 0)

            rationale_card = section_card()
            rationale_card.pack_start(lbl("Route rationale", "pkey"), False, False, 0)
            rationale_card.pack_start(lbl(f"Primary {short_model_name(current_default_model)} · Fallbacks {' → '.join(short_model_name(item) for item in get_fallback_chain()) if get_fallback_chain() else 'none'}", "upd", wrap=False, max_width=92, ellipsize=True), False, False, 0)
            body.pack_start(rationale_card, False, False, 0)

            incident_card = section_card()
            incident_card.pack_start(lbl("Event center", "pkey"), False, False, 0)
            incident_card.pack_start(lbl("Recent incidents, smoke results, and restore points.", "upd"), False, False, 0)
            for row_data in recent_events[:5]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
                row.pack_start(pill(str(row_data.get("kind") or "event").replace(".", " "), row_data.get("tone") or "good"), False, False, 0)
                row.pack_start(lbl(str(row_data.get("message") or ""), "subdued", wrap=False, max_width=64, ellipsize=True), True, True, 0)
                row.pack_end(lbl(str(row_data.get("ts") or "").split("T")[-1], "upd"), False, False, 0)
                incident_card.pack_start(row, False, False, 0)
            body.pack_start(incident_card, False, False, 0)

        elif tab == "models":
            lane_card = section_card()
            lane_card.pack_start(lbl("Primary lane", "pkey"), False, False, 0)
            configured_model_refs = get_configured_model_refs()
            switch_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
            current_model_btn = action_button(short_model_name(current_default_model), lambda _b: None, True)
            current_model_btn.set_sensitive(False)
            def choose_primary_model(_b):
                next_model = choose_model_dialog(
                    "Primary lane",
                    "Choose the default model lane.",
                    configured_model_refs,
                    current_default_model,
                )
                if not next_model or next_model == current_default_model:
                    return
                set_focus_models(next_model)
                restart_openclaw_stack()
                set_flash(f"Primary lane set to {next_model}", "good", timeout=4)
                record_action(f"Primary lane: {short_model_name(next_model)}", "good")
                sync_route_ui()
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
            switch_row.pack_start(current_model_btn, True, True, 0)
            switch_row.pack_start(action_button("Choose Model", choose_primary_model, True), False, False, 0)
            switch_row.pack_start(pill(overview["route_mode"], "good" if "Codex" in overview["route_mode"] else "warn"), False, False, 0)
            lane_card.pack_start(switch_row, False, False, 0)
            lane_card.pack_start(lbl(f"{len(configured_model_refs)} configured models available from your OpenClaw catalog.", "upd"), False, False, 0)
            lane_meta = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            lane_meta.pack_start(pill(f"Primary {short_model_name(current_default_model)}", "good"), False, False, 0)
            for fallback in get_fallback_chain()[:2]:
                lane_meta.pack_start(pill(f"Fallback {short_model_name(fallback)}", "warn"), False, False, 0)
            lane_card.pack_start(lane_meta, False, False, 0)
            body.pack_start(lane_card, False, False, 0)

            profile_card = section_card()
            profile_card.pack_start(lbl("Routing presets", "pkey"), False, False, 0)
            profile_card.pack_start(lbl("Apply a full routing profile for default, fallbacks, agent lanes, and sub-agents.", "upd"), False, False, 0)
            profile_grid = Gtk.Grid(column_spacing=6, row_spacing=6)
            active_profile = detect_active_routing_profile()
            for idx, profile_name in enumerate(ROUTING_PROFILES.keys()):
                def on_profile(_b, name=profile_name):
                    if apply_routing_profile(name):
                        restart_openclaw_stack()
                        set_flash(f"Applied {name} profile", "good", timeout=4)
                        record_action(f"Profile applied: {name}", "good")
                        sync_route_ui()
                        GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
                profile_grid.attach(action_button(profile_name, on_profile, profile_name == active_profile), idx % 3, idx // 3, 1, 1)
            profile_card.pack_start(profile_grid, False, False, 0)
            profile_desc = ROUTING_PROFILES.get(active_profile or "Balanced", ROUTING_PROFILES["Balanced"])["description"]
            profile_card.pack_start(lbl(profile_desc, "upd"), False, False, 0)
            baseline_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            baseline_row.pack_start(action_button("Save Baseline", lambda _b: save_baseline_async(), True), True, True, 0)
            baseline_row.pack_start(action_button("Restore Baseline", lambda _b: restore_baseline_async(), True), True, True, 0)
            profile_card.pack_start(baseline_row, False, False, 0)
            body.pack_start(profile_card, False, False, 0)

            quick = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            def force_native_codex(_b):
                set_focus_models("openai-codex/gpt-5.4")
                restart_openclaw_stack()
                set_flash("Forced Native Codex lane", "good", timeout=4)
                record_action("Profile action: Force Native Codex", "good")
                sync_route_ui()
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
            def safe_mode(_b):
                set_focus_models("nim/nvidia/nemotron-3-super-120b-a12b", "nim/qwen/qwen3-coder-480b-a35b-instruct")
                restart_openclaw_stack()
                set_flash("Safe mode routing applied", "good", timeout=4)
                record_action("Profile action: Safe Mode", "good")
                sync_route_ui()
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
            quick.pack_start(action_button("Force Native Codex", force_native_codex, True), True, True, 0)
            quick.pack_start(action_button("Safe Mode", safe_mode, True), True, True, 0)
            body.pack_start(quick, False, False, 0)

            fallback_card = section_card()
            fallback_card.pack_start(lbl("Fallback chain", "pkey"), False, False, 0)
            fallback_card.pack_start(lbl("Change fallback order directly from the tray. Duplicate and primary entries are stripped automatically.", "upd"), False, False, 0)
            current_fallbacks = get_fallback_chain()
            fallback_grid = Gtk.Grid(column_spacing=8, row_spacing=8)
            for idx in range(3):
                fallback_grid.attach(lbl(f"Fallback {idx + 1}", "pkey"), 0, idx, 1, 1)
                current_fallback = current_fallbacks[idx] if idx < len(current_fallbacks) else ""
                model_label = short_model_name(current_fallback) if current_fallback else "None"
                def choose_fallback(_b, slot=idx, current_value=current_fallback):
                    next_value = choose_model_dialog(
                        f"Fallback {slot + 1}",
                        "Choose a fallback model or None.",
                        configured_model_refs,
                        current_value,
                    )
                    fallbacks = get_fallback_chain()
                    while len(fallbacks) <= slot:
                        fallbacks.append("")
                    fallbacks[slot] = next_value or ""
                    set_fallback_chain(fallbacks)
                    restart_openclaw_stack()
                    set_flash(f"Fallback {slot + 1} updated", "good", timeout=4)
                    record_action(f"Fallback {slot + 1}: {short_model_name(next_value) if next_value else 'None'}", "good")
                    sync_route_ui()
                    GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
                fallback_grid.attach(action_button(model_label, choose_fallback, True), 1, idx, 1, 1)
                move_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=4)
                def move_up(_b, slot=idx):
                    if move_fallback_entry(slot, -1):
                        restart_openclaw_stack()
                        set_flash(f"Moved fallback {slot + 1} up", "good", timeout=3)
                        record_action(f"Fallback {slot + 1} moved up", "good")
                        sync_route_ui()
                        GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
                def move_down(_b, slot=idx):
                    if move_fallback_entry(slot, 1):
                        restart_openclaw_stack()
                        set_flash(f"Moved fallback {slot + 1} down", "good", timeout=3)
                        record_action(f"Fallback {slot + 1} moved down", "good")
                        sync_route_ui()
                        GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
                def clear_slot(_b, slot=idx):
                    if clear_fallback_entry(slot):
                        restart_openclaw_stack()
                        set_flash(f"Cleared fallback {slot + 1}", "good", timeout=3)
                        record_action(f"Fallback {slot + 1} cleared", "good")
                        sync_route_ui()
                        GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
                move_row.pack_start(action_button("↑", move_up), False, False, 0)
                move_row.pack_start(action_button("↓", move_down), False, False, 0)
                move_row.pack_start(action_button("Clear", clear_slot), False, False, 0)
                fallback_grid.attach(move_row, 2, idx, 1, 1)
            fallback_card.pack_start(fallback_grid, False, False, 0)
            body.pack_start(fallback_card, False, False, 0)

            agents_card = section_card()
            agents_card.pack_start(lbl("Agent lanes", "pkey"), False, False, 0)
            agents_card.pack_start(lbl("Override individual agent lanes without leaving the tray.", "upd"), False, False, 0)
            agent_grid = Gtk.Grid(column_spacing=8, row_spacing=8)
            managed_agents = ("main", "code", "simple", "reasoning", "creative", "local")
            for row_idx, agent_id in enumerate(managed_agents):
                agent_grid.attach(lbl(agent_id, "pkey"), 0, row_idx, 1, 1)
                current_agent_model = agent_models.get(agent_id, current_default_model)
                current_agent_model = current_agent_model if current_agent_model != "inherit" else current_default_model
                def choose_agent_model(_b, target_agent=agent_id, current_value=current_agent_model):
                    next_model = choose_model_dialog(
                        f"{target_agent} lane",
                        f"Choose the model for agent `{target_agent}`.",
                        configured_model_refs,
                        current_value,
                    )
                    if not next_model:
                        return
                    set_agent_model_ref(target_agent, next_model)
                    restart_openclaw_stack()
                    set_flash(f"{target_agent} lane updated", "good", timeout=4)
                    record_action(f"{target_agent}: {short_model_name(next_model)}", "good")
                    sync_route_ui()
                    GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
                agent_grid.attach(action_button(short_model_name(current_agent_model), choose_agent_model, True), 1, row_idx, 1, 1)
            subagent_row = len(managed_agents)
            agent_grid.attach(lbl("subagents", "pkey"), 0, subagent_row, 1, 1)
            current_subagent = get_subagent_model_ref() or current_default_model
            def choose_subagent_model(_b):
                next_subagent = choose_model_dialog(
                    "Sub-agent lane",
                    "Choose the model for delegated sub-agents.",
                    configured_model_refs,
                    current_subagent,
                )
                if not next_subagent:
                    return
                set_subagent_model_ref(next_subagent)
                restart_openclaw_stack()
                set_flash("Sub-agent lane updated", "good", timeout=4)
                record_action(f"subagents: {short_model_name(next_subagent)}", "good")
                sync_route_ui()
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
            agent_grid.attach(action_button(short_model_name(current_subagent), choose_subagent_model, True), 1, subagent_row, 1, 1)
            agents_card.pack_start(agent_grid, False, False, 0)
            body.pack_start(agents_card, False, False, 0)

            policy = get_routing_policy()
            policy_card = section_card()
            policy_card.pack_start(lbl("Policy engine", "pkey"), False, False, 0)
            policy_card.pack_start(lbl("Define which lane each task type should prefer and whether deviation is allowed.", "upd"), False, False, 0)
            policy_grid = Gtk.Grid(column_spacing=8, row_spacing=8)
            for row_idx, policy_key in enumerate(("coding", "research", "verification")):
                row_policy = policy.get(policy_key) or {}
                policy_grid.attach(lbl(policy_key.title(), "pkey"), 0, row_idx, 1, 1)
                policy_grid.attach(pill(short_model_name(row_policy.get("lane")), "good"), 1, row_idx, 1, 1)
                policy_grid.attach(pill("strict" if row_policy.get("strict") else "flex", "warn" if row_policy.get("strict") else "good"), 2, row_idx, 1, 1)
                policy_grid.attach(pill("sidecar" if row_policy.get("sidecar") else "solo", "good" if row_policy.get("sidecar") else "bad"), 3, row_idx, 1, 1)
                policy_grid.attach(action_button("Edit", lambda _b, key=policy_key, opts=configured_model_refs: show_policy_dialog(key, opts)), 4, row_idx, 1, 1)
            policy_card.pack_start(policy_grid, False, False, 0)
            policy_card.pack_start(lbl("User override always wins. Runtime governor is now enforcing these lanes before model resolution and on sessions_spawn.", "upd"), False, False, 0)
            body.pack_start(policy_card, False, False, 0)

        elif tab == "accounts":
            account_card = section_card()
            account_card.pack_start(lbl("Accounts & credentials", "pkey"), False, False, 0)
            account_card.pack_start(lbl("Keep auth aligned with the providers actually in your fallback chain.", "upd"), False, False, 0)
            sorted_accounts = sorted(ACCOUNT_ACTIONS, key=lambda account: (provider_relevance_rank(account["status_key"]), account["title"]))
            for account in sorted_accounts:
                if account["status_key"] == "openai" and not show_openai_api_surface():
                    continue
                row_card = section_card()
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                left = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=3)
                left.pack_start(lbl(account["title"], "pval"), False, False, 0)
                left.pack_start(lbl(account["description"], "subdued"), False, False, 0)
                row.pack_start(left, True, True, 0)
                status = data.get(account["status_key"], {})
                tone = "good" if status.get("ok") else ("warn" if status.get("nokey") else "bad")
                meta = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
                meta.pack_start(pill(status.get("label", "n/a"), tone), False, False, 0)
                meta.pack_start(pill(provider_relevance_label(account["status_key"]), "good" if provider_relevance_rank(account["status_key"]) == 0 else ("warn" if provider_relevance_rank(account["status_key"]) == 1 else "bad")), False, False, 0)
                row.pack_start(meta, False, False, 0)
                if account["action"] == "login":
                    btn = action_button(account["button"], lambda _b: subprocess.Popen(["x-terminal-emulator", "-e", "codex login"], stderr=subprocess.DEVNULL), tone="good" if status.get("ok") else "primary")
                else:
                    btn = action_button(
                        account["button"],
                        lambda _b, env_key=account["env_key"], title=account["title"], placeholder=account["placeholder"]:
                            show_env_key_dialog(env_key, f"Set {title} Key", placeholder),
                        tone="good" if status.get("ok") else ("warn" if status.get("nokey") else "primary"),
                    )
                row.pack_end(btn, False, False, 0)
                row_card.pack_start(row, False, False, 0)
                account_card.pack_start(row_card, False, False, 0)
            local_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
            local_info = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=3)
            local_info.pack_start(lbl("Ollama", "pval"), False, False, 0)
            local_info.pack_start(lbl("Local runtime only. No external credential required.", "subdued"), False, False, 0)
            local_row.pack_start(local_info, True, True, 0)
            local_row.pack_end(pill(data.get("ollama", {}).get("label", "L:OFF"), "good" if data.get("ollama", {}).get("ok") else "bad"), False, False, 0)
            account_card.pack_start(local_row, False, False, 0)
            body.pack_start(account_card, False, False, 0)

            provider_drilldown_card = section_card()
            provider_drilldown_card.pack_start(lbl("Provider drilldown", "pkey"), False, False, 0)
            provider_drilldown_card.pack_start(lbl("Usage, smoke status, account spread, and top models per provider.", "upd"), False, False, 0)
            for row_data in get_provider_drilldown(usage["entries"], provider_tests)[:5]:
                row_card = section_card()
                head = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                head.pack_start(lbl(row_data["provider"], "pval"), False, False, 0)
                smoke = row_data.get("smoke") or {}
                if smoke.get("ok") is True:
                    smoke_text, smoke_tone = "smoke ok", "good"
                elif smoke.get("ok") is False:
                    smoke_text, smoke_tone = "smoke failed", "bad"
                else:
                    smoke_text, smoke_tone = "smoke idle", "warn"
                head.pack_end(pill(smoke_text, smoke_tone), False, False, 0)
                head.pack_end(pill(format_usage_metric_value(row_data), "good" if float(row_data.get("cost") or 0.0) > 0 else "warn"), False, False, 0)
                row_card.pack_start(head, False, False, 0)
                row_card.pack_start(lbl(f"{format_tokens_short(row_data['tokens'])} tok · accounts {', '.join(row_data['accounts'])} · last {row_data['lastSeenLabel']}", "upd", wrap=False, max_width=90, ellipsize=True), False, False, 0)
                top_models_text = ", ".join(f"{short_model_name(model)} {format_tokens_short(tokens)}" for model, tokens in row_data["topModels"]) or "No model data"
                row_card.pack_start(lbl(f"Top models: {top_models_text}", "upd", wrap=False, max_width=90, ellipsize=True), False, False, 0)
                row_card.pack_start(lbl(f"Priced models: {row_data['pricedModels']} · quota {format_tokens_short(row_data['quotaTokens'])} · metered {format_tokens_short(row_data['meteredTokens'])}", "upd", wrap=False, max_width=90, ellipsize=True), False, False, 0)
                provider_drilldown_card.pack_start(row_card, False, False, 0)
            body.pack_start(provider_drilldown_card, False, False, 0)

            account_rollup_card = section_card()
            account_rollup_card.pack_start(lbl("Account drilldown", "pkey"), False, False, 0)
            for row_data in get_account_drilldown(usage["entries"])[:5]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(row_data["account"], "pval"), False, False, 0)
                row.pack_start(lbl(f"{format_tokens_short(row_data['tokens'])} tok · {row_data['count']} runs", "upd"), True, True, 0)
                row.pack_end(lbl(format_usage_metric_value(row_data), "pval"), False, False, 0)
                account_rollup_card.pack_start(row, False, False, 0)
            body.pack_start(account_rollup_card, False, False, 0)

            account_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            account_actions.pack_start(action_button("Refresh Accounts", lambda _b: request_full_refresh(False), True), True, True, 0)
            account_actions.pack_start(action_button("Open Control UI", lambda _b: subprocess.Popen(["xdg-open", "http://127.0.0.1:18789"], stderr=subprocess.DEVNULL)), True, True, 0)
            body.pack_start(account_actions, False, False, 0)

        elif tab == "daemons":
            daemon_card = section_card()
            daemon_card.pack_start(lbl("Daemon control", "pkey"), False, False, 0)
            daemon_card.pack_start(lbl("Launch both local OpenClaw and your Docker or NemoClaw stack from the tray.", "upd"), False, False, 0)
            for daemon_id in (LOCAL_DAEMON_ID, DOCKER_DAEMON_ID):
                daemon_cfg = load_daemon_config().get("daemons", {}).get(daemon_id, {})
                status = daemon_data.get(daemon_id, {})
                row_card = section_card()
                head = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                head.pack_start(lbl(daemon_cfg.get("label", daemon_id), "pval"), False, False, 0)
                tone = "good" if status.get("ok") else ("warn" if status.get("needs_setup") else "bad")
                head.pack_end(pill(status.get("label", "OFF"), tone), False, False, 0)
                row_card.pack_start(head, False, False, 0)
                row_card.pack_start(lbl(status.get("detail", daemon_cfg.get("description", "No details.")), "subdued", wrap=False, max_width=92, ellipsize=True), False, False, 0)
                if daemon_cfg.get("description"):
                    row_card.pack_start(lbl(daemon_cfg["description"], "upd", wrap=False, max_width=92, ellipsize=True), False, False, 0)
                cookie_env = status.get("cookieEnv")
                if cookie_env:
                    cookie_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                    cookie_row.pack_start(lbl(f"Auth token: {cookie_env}", "subdued"), False, False, 0)
                    cookie_row.pack_end(
                        pill("saved" if status.get("cookieSaved") else "missing", "good" if status.get("cookieSaved") else "warn"),
                        False,
                        False,
                        0,
                    )
                    row_card.pack_start(cookie_row, False, False, 0)
                actions = Gtk.Grid(column_spacing=6, row_spacing=6)
                action_col = 0
                action_row = 0
                def attach_action(widget):
                    nonlocal action_col, action_row
                    actions.attach(widget, action_col, action_row, 1, 1)
                    action_col += 1
                    if action_col >= 4:
                        action_col = 0
                        action_row += 1
                if daemon_cfg.get("type") == "systemd":
                    attach_action(action_button("Start", lambda _b, d=daemon_id: trigger_daemon_action(d, "start"), True, tone="good"))
                    attach_action(action_button("Restart", lambda _b, d=daemon_id: trigger_daemon_action(d, "restart"), tone="ghost"))
                    attach_action(action_button("Stop", lambda _b, d=daemon_id: trigger_daemon_action(d, "stop"), tone="bad"))
                else:
                    attach_action(action_button("Launch", lambda _b, d=daemon_id: trigger_daemon_action(d, "start"), True, tone="good"))
                    attach_action(action_button("Stop", lambda _b, d=daemon_id: trigger_daemon_action(d, "stop"), tone="bad"))
                    attach_action(action_button("Config", lambda _b, d=daemon_id: show_daemon_config_dialog(d), tone="ghost"))
                    attach_action(action_button("Cookie", lambda _b, d=daemon_id: show_daemon_cookie_dialog(d), tone="ghost"))
                if daemon_cfg.get("open_cmd") or status.get("url"):
                    attach_action(action_button("Open", lambda _b, d=daemon_id: open_daemon_surface(d), tone="primary"))
                row_card.pack_start(actions, False, False, 0)
                auto_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
                auto_row.pack_start(pill(f"Autostart {'on' if status.get('enabled') else 'off'}", "good" if status.get("enabled") else "warn"), False, False, 0)
                auto_row.pack_start(action_button(
                    "Turn Off" if status.get("enabled") else "Turn On",
                    lambda _b, d=daemon_id, enabled=not status.get("enabled"): toggle_daemon_autostart(d, enabled),
                    tone="bad" if status.get("enabled") else "good",
                ), False, False, 0)
                row_card.pack_start(auto_row, False, False, 0)
                daemon_card.pack_start(row_card, False, False, 0)
            body.pack_start(daemon_card, False, False, 0)

            notes_card = section_card()
            notes_card.pack_start(lbl("Build notes", "pkey"), False, False, 0)
            notes_card.pack_start(lbl(str(CODEXBAR_NOTES), "upd"), False, False, 0)
            notes_card.pack_start(lbl("CodexBar now appends daemon and auth changes here so the system build stays documented.", "upd"), False, False, 0)
            body.pack_start(notes_card, False, False, 0)

        elif tab == "spend":
            spend_top = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
            for title, metric in [
                ("Today", usage["today"]),
                ("7 Day", usage["week"]),
                ("30 Day", usage["month"]),
                ("Projection", {"cost": usage["projection"], "tokens": usage["month"]["tokens"]}),
            ]:
                card = section_card()
                card.pack_start(lbl(format_usage_metric_value(metric), "metric-value"), False, False, 0)
                sub = format_usage_metric_detail(title, metric) if title != "Projection" else "Projected monthly billable burn"
                card.pack_start(lbl(sub, "metric-label"), False, False, 0)
                spend_top.pack_start(card, True, True, 0)
            body.pack_start(spend_top, False, False, 0)

            mode_card = section_card()
            mode_card.pack_start(lbl("Spend modes", "pkey"), False, False, 0)
            mode_grid = Gtk.Grid(column_spacing=8, row_spacing=8)
            spend_modes = [
                ("Billable", format_cost_short(usage["month"]["cost"]), "good"),
                ("Quota", format_tokens_short(usage["month"]["quotaTokens"]) + " tok", "warn"),
                ("Metered", format_tokens_short(usage["month"]["meteredTokens"]) + " tok", "warn"),
                ("Local", format_tokens_short(usage["month"]["localTokens"]) + " tok", "good"),
                ("Unknown", format_tokens_short(usage["month"]["unknownTokens"]) + " tok", "bad"),
            ]
            for idx, (title, value, tone) in enumerate(spend_modes):
                card = section_card()
                card.pack_start(lbl(value, "metric-value"), False, False, 0)
                card.pack_start(lbl(title, "metric-label"), False, False, 0)
                mode_grid.attach(card, idx % 2, idx // 2, 1, 1)
            mode_card.pack_start(mode_grid, False, False, 0)
            body.pack_start(mode_card, False, False, 0)

            recent_card = section_card()
            recent_card.pack_start(lbl("Recent velocity", "pkey"), False, False, 0)
            for day in usage["recent_days"]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(day["label"], "pval"), False, False, 0)
                row.pack_start(lbl(format_tokens_short(day["tokens"]) + " tok", "upd"), False, False, 0)
                row.pack_end(lbl(format_usage_metric_value(day), "pval"), False, False, 0)
                recent_card.pack_start(row, False, False, 0)
            body.pack_start(recent_card, False, False, 0)

            pricing_card = section_card()
            pricing_card.pack_start(lbl("Pricing registry", "pkey"), False, False, 0)
            pricing_card.pack_start(lbl("Manual pricing overrides convert metered lanes into billable cost tracking.", "upd"), False, False, 0)
            for row_data in get_pricing_registry_rows(usage["entries"], limit=6):
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                meta = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=1)
                meta.pack_start(lbl(short_model_name(row_data["modelRef"]), "pval"), False, False, 0)
                source_line = f"{row_data['provider']} · {format_tokens_short(row_data['tokens'])} tok · {row_data['source']}"
                if row_data.get("lastVerifiedAt"):
                    source_line += f" · {row_data['lastVerifiedAt']}"
                meta.pack_start(lbl(source_line, "upd", wrap=False, max_width=72, ellipsize=True), False, False, 0)
                row.pack_start(meta, True, True, 0)
                tone = "good" if row_data["priced"] else "warn"
                row.pack_start(pill("priced" if row_data["priced"] else "metered", tone), False, False, 0)
                row.pack_end(action_button("Edit", lambda _b, ref=row_data["modelRef"]: show_pricing_dialog(ref)), False, False, 0)
                pricing_card.pack_start(row, False, False, 0)
            body.pack_start(pricing_card, False, False, 0)

            provider_card = section_card()
            provider_card.pack_start(lbl("Provider rollup", "pkey"), False, False, 0)
            for row_data in summarize_rollups(usage["entries"], "provider")[:5]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(row_data["name"], "pval"), False, False, 0)
                row.pack_start(lbl(f"{format_tokens_short(row_data['tokens'])} tok · {row_data['count']} runs", "upd"), True, True, 0)
                row.pack_end(lbl(format_usage_metric_value(row_data), "pval"), False, False, 0)
                provider_card.pack_start(row, False, False, 0)
            body.pack_start(provider_card, False, False, 0)

            account_card = section_card()
            account_card.pack_start(lbl("Account rollup", "pkey"), False, False, 0)
            for row_data in summarize_rollups(usage["entries"], "account")[:5]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(row_data["name"], "pval"), False, False, 0)
                row.pack_start(lbl(f"{format_tokens_short(row_data['tokens'])} tok · {row_data['count']} runs", "upd"), True, True, 0)
                row.pack_end(lbl(format_usage_metric_value(row_data), "pval"), False, False, 0)
                account_card.pack_start(row, False, False, 0)
            body.pack_start(account_card, False, False, 0)

        elif tab == "ops":
            ops_card = section_card()
            ops_card.pack_start(lbl("Runtime control", "pkey"), False, False, 0)
            runtime_rows = [
                ("Gateway", "openclaw"),
                ("Codex", "codex"),
                ("NIM", "nim"),
                ("Gemini", "gemini"),
                ("Ollama", "ollama"),
            ]
            if show_openai_api_surface():
                runtime_rows.insert(3, ("OpenAI API", "openai"))
            for title, key in runtime_rows:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(title, "pval"), False, False, 0)
                row.pack_start(lbl(data.get(key, {}).get("detail", "unknown"), "upd"), True, True, 0)
                tone = "good" if data.get(key, {}).get("ok") else ("warn" if data.get(key, {}).get("nokey") else "bad")
                row.pack_end(pill(data.get(key, {}).get("label", "n/a"), tone), False, False, 0)
                ops_card.pack_start(row, False, False, 0)
            body.pack_start(ops_card, False, False, 0)

            test_card = section_card()
            test_card.pack_start(lbl("Provider smoke tests", "pkey"), False, False, 0)
            test_card.pack_start(lbl("Run a human-style one-turn test against each provider lane and confirm the actual provider/model used.", "upd"), False, False, 0)
            test_grid = Gtk.Grid(column_spacing=8, row_spacing=8)
            smoke_providers = [
                ("NIM", "nim"),
                ("Gemini", "gemini"),
                ("Codex", "openai-codex"),
                ("Ollama", "ollama"),
            ]
            for row_idx, (title, provider_key) in enumerate(smoke_providers):
                test_grid.attach(lbl(title, "pkey"), 0, row_idx, 1, 1)
                result = provider_tests.get(provider_key) or {}
                if provider_key in state.get("testing_providers", set()):
                    label_text = "testing…"
                    tone = "warn"
                elif result.get("ok") is True:
                    label_text = f"OK · {short_model_name(result.get('model'))}"
                    tone = "good"
                elif result.get("ok") is False:
                    label_text = "FAILED"
                    tone = "bad"
                else:
                    label_text = "not run"
                    tone = "warn"
                test_grid.attach(pill(label_text, tone), 1, row_idx, 1, 1)
                detail = result.get("detail") or "No result yet."
                if result.get("checkedAt"):
                    detail = f"{detail} · {result['checkedAt']}"
                detail_label = lbl(compact_detail_text(detail), "upd", wrap=False, max_width=56, ellipsize=True)
                detail_label.set_tooltip_text(str(detail))
                test_grid.attach(detail_label, 2, row_idx, 1, 1)
                test_grid.attach(action_button("Test", lambda _b, p=provider_key: run_provider_smoke_test_async(p), True), 3, row_idx, 1, 1)
            test_card.pack_start(test_grid, False, False, 0)
            test_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            test_actions.pack_start(action_button("Test All", lambda _b: run_all_provider_smoke_tests_async(), True), True, True, 0)
            test_actions.pack_start(action_button("Clean Legacy Router", lambda _b: cleanup_legacy_router_async()), True, True, 0)
            test_card.pack_start(test_actions, False, False, 0)
            body.pack_start(test_card, False, False, 0)

            schedule_card = section_card()
            schedule_card.pack_start(lbl("Scheduled smoke tests", "pkey"), False, False, 0)
            schedule_card.pack_start(lbl(f"Last run {scheduled_smoke.get('lastRunAt') or 'never'}", "subdued"), False, False, 0)
            smoke_summary = scheduled_smoke.get("summary") or {}
            for provider_key in ("nim", "gemini", "openai-codex", "ollama"):
                row_data = smoke_summary.get(provider_key) or {}
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(provider_key, "pval"), False, False, 0)
                row.pack_start(lbl(short_model_name(row_data.get("model") or ""), "upd", wrap=False, max_width=36, ellipsize=True), True, True, 0)
                row.pack_end(pill("pass" if row_data.get("ok") else ("fail" if row_data else "idle"), "good" if row_data.get("ok") else ("bad" if row_data else "warn")), False, False, 0)
                schedule_card.pack_start(row, False, False, 0)
            schedule_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            schedule_actions.pack_start(action_button("Run Scheduled Tests", lambda _b: run_all_provider_smoke_tests_async(), True), True, True, 0)
            schedule_actions.pack_start(action_button("Refresh Ops", lambda _b: request_full_refresh(False), tone="ghost"), True, True, 0)
            schedule_card.pack_start(schedule_actions, False, False, 0)
            body.pack_start(schedule_card, False, False, 0)

            alert_card = section_card()
            alert_card.pack_start(lbl("Alert controls", "pkey"), False, False, 0)
            alert_card.pack_start(lbl(
                f"{'Muted until ' + datetime.fromtimestamp(muted_until).strftime('%H:%M') if alerts_muted else 'Alerts live'} · current down {', '.join(current_alert_state.get('down_signature') or []) or 'none'}",
                "subdued",
                wrap=False,
                max_width=84,
                ellipsize=True,
            ), False, False, 0)
            alert_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            alert_actions.pack_start(action_button("Acknowledge", lambda _b: acknowledge_current_alerts(), tone="good"), True, True, 0)
            alert_actions.pack_start(action_button("Mute 1h", lambda _b: mute_alerts_for(3600), tone="warn"), True, True, 0)
            alert_actions.pack_start(action_button("Unmute", lambda _b: clear_alert_mute(), tone="ghost"), True, True, 0)
            alert_card.pack_start(alert_actions, False, False, 0)
            body.pack_start(alert_card, False, False, 0)

            restore_card = section_card()
            restore_card.pack_start(lbl("Config history", "pkey"), False, False, 0)
            restore_card.pack_start(lbl("Recent restore points for routing, policy, daemon, and credential changes.", "upd"), False, False, 0)
            for point in restore_points[:5]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                row.pack_start(lbl(point.get("id", "unknown"), "pval"), False, False, 0)
                row.pack_start(lbl(point.get("reason", ""), "upd", wrap=False, max_width=44, ellipsize=True), True, True, 0)
                row.pack_end(lbl(point.get("createdAt", ""), "upd"), False, False, 0)
                restore_card.pack_start(row, False, False, 0)
            restore_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            restore_actions.pack_start(action_button("Create Restore Point", lambda _b: create_restore_point_async(), True), True, True, 0)
            restore_actions.pack_start(action_button("Restore Latest", lambda _b: restore_latest_restore_point_async(), tone="warn"), True, True, 0)
            restore_card.pack_start(restore_actions, False, False, 0)
            body.pack_start(restore_card, False, False, 0)

            events_card = section_card()
            events_card.pack_start(lbl("Event center", "pkey"), False, False, 0)
            events_card.pack_start(lbl("Live operational history across routing, smoke tests, and daemon actions.", "upd"), False, False, 0)
            filter_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            for filter_key, label_text in [
                ("all", "All"),
                ("alerts", "Alerts"),
                ("smoke", "Smoke"),
                ("restore", "Restore"),
                ("daemon", "Daemons"),
                ("policy", "Policy"),
            ]:
                def on_filter(_b, fk=filter_key):
                    save_event_filter(fk)
                    state["event_filter"] = fk
                    state["event_page"] = 0
                    rebuild()
                    win.show_all()
                    reposition()
                filter_row.pack_start(action_button(label_text, on_filter, tone="good" if event_filter == filter_key else "ghost"), False, False, 0)
            events_card.pack_start(filter_row, False, False, 0)
            for row_data in recent_events[:8]:
                row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
                row.pack_start(pill(str(row_data.get("kind") or "event").replace(".", " "), row_data.get("tone") or "good"), False, False, 0)
                row.pack_start(lbl(str(row_data.get("message") or ""), "subdued", wrap=False, max_width=52, ellipsize=True), True, True, 0)
                row.pack_end(lbl(str(row_data.get("ts") or "").split("T")[-1], "upd"), False, False, 0)
                def on_view(_b, event_row=row_data):
                    detail_lines = [f"{key}: {value}" for key, value in event_row.items()]
                    show_notice_dialog("Event detail", "\n".join(detail_lines))
                row.pack_end(action_button("View", on_view, tone="ghost"), False, False, 0)
                events_card.pack_start(row, False, False, 0)
            pager_row = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            pager_row.pack_start(lbl(f"Page {event_page + 1} / {max_page + 1 if all_filtered_events else 1}", "subdued"), False, False, 0)
            def prev_page(_b):
                state["event_page"] = max(0, int(state.get("event_page") or 0) - 1)
                rebuild()
                win.show_all()
                reposition()
            def next_page(_b):
                state["event_page"] = min(max_page, int(state.get("event_page") or 0) + 1)
                rebuild()
                win.show_all()
                reposition()
            pager_row.pack_end(action_button("Next", next_page, tone="ghost"), False, False, 0)
            pager_row.pack_end(action_button("Prev", prev_page, tone="ghost"), False, False, 0)
            events_card.pack_start(pager_row, False, False, 0)
            body.pack_start(events_card, False, False, 0)

            if gpu.get("ok"):
                vram_card = section_card()
                vram_card.pack_start(lbl("GPU load", "pkey"), False, False, 0)
                pct = gpu["used"] / gpu["total"]
                grow = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
                bar = Gtk.ProgressBar()
                bar.set_fraction(min(pct, 1.0))
                bar.set_show_text(True)
                bar.set_text(f"{gpu['used']}MB / {gpu['total']}MB · GPU {gpu['util']}%")
                sc(bar, "crit" if pct > 0.9 else ("warn" if pct > 0.7 else "blue"))
                grow.pack_start(bar, True, True, 0)
                vram_card.pack_start(grow, False, False, 0)
                body.pack_start(vram_card, False, False, 0)

            ctrl = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            router_cls = "btn-router-on" if router_on else "btn-router-off"
            router_lbl = "⚡ Router ON" if router_on else "⚡ Router OFF"
            rbtn = sc(Gtk.Button(label=router_lbl), router_cls)
            rbtn.set_relief(Gtk.ReliefStyle.NONE)
            def on_toggle_router(b):
                set_router_enabled(not get_router_enabled())
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False), False)[1])
            rbtn.connect("clicked", on_toggle_router)
            ctrl.pack_start(rbtn, True, True, 0)
            ctrl.pack_start(action_button("Restart Gateway", lambda _b: subprocess.Popen(["systemctl", "--user", "restart", "openclaw-gateway.service"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)), True, True, 0)
            ctrl.pack_start(action_button("Restart Router", lambda _b: subprocess.Popen(["systemctl", "--user", "restart", "openclaw-router.service"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)), True, True, 0)
            body.pack_start(ctrl, False, False, 0)

            ops_actions = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=6)
            ops_actions.pack_start(action_button("Refresh Runtime", lambda _b: request_full_refresh(False), True), True, True, 0)
            ops_actions.pack_start(action_button("Open UI", lambda _b: subprocess.Popen(["xdg-open", "http://127.0.0.1:18789"], stderr=subprocess.DEVNULL), True), True, True, 0)
            body.pack_start(ops_actions, False, False, 0)

        # ── Footer: refresh + close ──────────────────────────────────────
        body.pack_start(lbl(f"Updated {updated}", "upd"), False, False, 0)
        brow = Gtk.Box(orientation=Gtk.Orientation.HORIZONTAL, spacing=8)
        brow.set_margin_top(2)

        ref_btn = sc(Gtk.Button(label="⟳  Refresh"), "btn")
        ref_btn.set_relief(Gtk.ReliefStyle.NONE)
        def on_refresh(b):
            request_full_refresh(notify=True)
        ref_btn.connect("clicked", on_refresh)

        cls_btn = sc(Gtk.Button(label="✕  Close"), "btn")
        cls_btn.set_relief(Gtk.ReliefStyle.NONE)
        cls_btn.connect("clicked", lambda b: hide_popup())

        brow.pack_start(ref_btn, True, True, 0)
        brow.pack_start(cls_btn, True, True, 0)
        body.pack_start(brow, False, False, 0)
        body.show_all()

    def reposition():
        win.show_all()
        def do_move():
            display = Gdk.Display.get_default()
            monitor = display.get_primary_monitor() if display else None
            if monitor:
                geometry = monitor.get_workarea()
                screen_width = geometry.width
                screen_height = geometry.height
                screen_x = geometry.x
                screen_y = geometry.y
            else:
                screen_width = 1440
                screen_height = 900
                screen_x = 0
                screen_y = 0
            max_height = max(520, min(860, screen_height - 96))
            scroller.set_size_request(-1, max_height)
            w, h = win.get_size()
            win.move(screen_x + screen_width - w - 14, screen_y + 42)
        GLib.idle_add(do_move)

    def toggle_popup():
        if state["visible"]:
            hide_popup()
        else:
            state["visible"] = True
            rebuild()
            reposition()
            request_refresh(False, rebuild_after=False)

    # ── DBusMenu ────────────────────────────────────────────────────────
    class DbusMenu(dbus.service.Object):
        IFACE = "com.canonical.dbusmenu"
        def __init__(self, bus, path):
            super().__init__(bus, path); self._rev = 1

        @dbus.service.method(IFACE, in_signature="iias", out_signature="u(ia{sv}av)")
        def GetLayout(self, parent_id, recursion_depth, property_names):
            def item(id_, label, enabled=True, sep=False):
                props = {}
                if sep:
                    props["type"] = dbus.String("separator")
                else:
                    props["label"]   = dbus.String(label)
                    props["enabled"] = dbus.Boolean(enabled)
                return dbus.Struct(
                    (dbus.Int32(id_),
                     dbus.Dictionary(props, signature="sv"),
                     dbus.Array([], signature="v")),
                    signature="ia{sv}av")
            data    = state["data"]
            router_on = get_router_enabled()
            children = [
                item(10, "── OpenClaw Status ──", enabled=False),
                item(20, data.get("nim", {}).get("detail", "NIM: …"), enabled=False),
                item(21, data.get("gemini", {}).get("detail", "Gemini: …"), enabled=False),
                item(22, data.get("codex",  {}).get("detail", "Codex: …"), enabled=False),
                item(23, data.get("ollama", {}).get("detail", "Ollama: …"), enabled=False),
                item(24, f"Router: {'ON' if router_on else 'OFF'}", enabled=False),
                item(30, "", sep=True),
                item(40, "⟳  Refresh"),
                item(41, f"{'⚡ Disable Router' if router_on else '⚡ Enable Router'}"),
                item(50, "🪟  Show panel"),
                item(60, "✕  Quit CodexBar"),
            ]
            root = dbus.Struct(
                (dbus.Int32(0), dbus.Dictionary({}, signature="sv"),
                 dbus.Array(children, signature="v")),
                signature="ia{sv}av")
            return dbus.UInt32(self._rev), root

        @dbus.service.method(IFACE, in_signature="isvu", out_signature="")
        def Event(self, id, event_id, data, timestamp):
            if event_id != "clicked": return
            if id == 40:
                request_full_refresh(notify=True, rebuild_after=False)
            elif id == 41:
                set_router_enabled(not get_router_enabled())
                GLib.timeout_add_seconds(4, lambda: (request_full_refresh(False, rebuild_after=False), False)[1])
            elif id == 50:
                GLib.idle_add(toggle_popup)
            elif id == 60:
                loop.quit()

        @dbus.service.signal(IFACE, signature="ui")
        def LayoutUpdated(self, revision, parent): pass

        @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature="ss", out_signature="v")
        def Get(self, iface, prop): return self.GetAll(iface)[prop]

        @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature="s", out_signature="a{sv}")
        def GetAll(self, iface):
            return {"Version": dbus.UInt32(3), "Status": dbus.String("normal"),
                    "TextDirection": dbus.String("ltr")}

    # ── StatusNotifierItem ───────────────────────────────────────────────
    class StatusNotifierItem(dbus.service.Object):
        SNI = "org.kde.StatusNotifierItem"
        def __init__(self, bus):
            svc = f"org.kde.StatusNotifierItem-{os.getpid()}-1"
            self._bn = dbus.service.BusName(svc, bus=bus)
            super().__init__(bus, "/StatusNotifierItem")
            self._menu = DbusMenu(bus, "/StatusNotifierItem/Menu")

        @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature="ss", out_signature="v")
        def Get(self, iface, prop): return self.GetAll(iface)[prop]

        @dbus.service.method(dbus.PROPERTIES_IFACE, in_signature="s", out_signature="a{sv}")
        def GetAll(self, iface):
            data      = state["data"]
            configured = {k: v for k, v in data.items()
                          if v.get("ok") is not None and not v.get("nokey")}
            all_ok    = all(v.get("ok", False) for v in configured.values()) if configured else True
            any_ok    = any(v.get("ok", False) for v in data.values())
            parts     = "  ".join(data[k]["label"]
                                  for k in ("nim","gemini","codex","ollama")
                                  if k in data) if data else "loading…"
            tooltip   = "\n".join(v.get("detail","") for v in data.values()) if data else ""
            return {
                "Category":   dbus.String("ApplicationStatus"),
                "Id":         dbus.String("codexbar-linux"),
                "Title":      dbus.String(f"OpenClaw  {parts}"),
                "Status":     dbus.String("Active"),
                "IconName":   dbus.String(""),
                "IconPixmap": _make_sni_icon(all_ok, any_ok),
                "Menu":       dbus.ObjectPath("/StatusNotifierItem/Menu"),
                "ItemIsMenu": dbus.Boolean(False),
                "ToolTip": dbus.Struct(
                    ("", dbus.Array([], signature="(iiay)"),
                     f"OpenClaw  {parts}", tooltip),
                    signature="sa(iiay)ss"),
            }

        @dbus.service.signal(SNI)
        def NewTitle(self): pass
        @dbus.service.signal(SNI)
        def NewIcon(self): pass
        @dbus.service.signal(SNI, signature="s")
        def NewStatus(self, s): pass

        @dbus.service.method(SNI, in_signature="ii")
        def Activate(self, x, y): GLib.idle_add(toggle_popup)
        @dbus.service.method(SNI, in_signature="ii")
        def SecondaryActivate(self, x, y): pass
        @dbus.service.method(SNI, in_signature="is")
        def Scroll(self, delta, orientation): pass

    # ── Boot ─────────────────────────────────────────────────────────────
    apply_css()
    do_refresh(notify=False)
    bus = dbus.SessionBus()
    sni = StatusNotifierItem(bus)
    state["sni"] = sni
    state["menu"] = sni._menu
    try:
        watcher = bus.get_object("org.kde.StatusNotifierWatcher", "/StatusNotifierWatcher")
        watcher.RegisterStatusNotifierItem(
            f"org.kde.StatusNotifierItem-{os.getpid()}-1",
            dbus_interface="org.kde.StatusNotifierWatcher")
    except dbus.DBusException as e:
        print(f"SNI watcher: {e}", file=sys.stderr)

    loop = GLib.MainLoop()

    def bg_refresh():
        request_full_refresh(notify=True, rebuild_after=False)
        return True

    if not state.get("usage_snapshot"):
        request_usage_refresh(rebuild_after=False)
    elif usage_snapshot_is_stale(state.get("usage_snapshot"), max_age_seconds=600):
        GLib.timeout_add_seconds(15, lambda: (request_usage_refresh(rebuild_after=False), False)[1])
    GLib.timeout_add_seconds(300, bg_refresh)
    parts = "  ".join(state["data"][k]["label"]
                      for k in ("nim","gemini","codex","ollama")
                      if k in state["data"])
    print(f"CodexBar ready  {parts}", flush=True)
    loop.run()


# ── Entry point ────────────────────────────────────────────────────────

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "tray"

    if mode == "waybar":
        waybar_output()
    elif mode == "usage-snapshot":
        cmd_usage_snapshot(sys.argv)
    elif mode == "scheduled-smoke-tests":
        cmd_scheduled_smoke_tests(sys.argv)
    elif mode == "cost":
        cmd_cost(sys.argv)
    elif mode == "status":
        status = get_all_status()
        gpu    = get_gpu_info()
        for k, v in status.items():
            if k == "daemons":
                for daemon_id, daemon_status in v.items():
                    sym = "✓" if daemon_status.get("ok") else ("·" if daemon_status.get("needs_setup") else "✗")
                    print(f"  {sym} {daemon_status.get('detail', daemon_id)}")
                continue
            if k == "openai" and not show_openai_api_surface():
                continue
            sym = "✓" if v["ok"] else ("·" if v.get("nokey") else "✗")
            print(f"  {sym} {v['detail']}")
        if gpu.get("ok"):
            pct = gpu["used"] / gpu["total"] * 100
            print(f"  🎮 VRAM: {gpu['used']}MB / {gpu['total']}MB ({pct:.0f}%)  GPU {gpu['util']}%")
        router = get_router_enabled()
        print(f"  ⚡ Router: {'ON' if router else 'OFF'}")
    else:
        gnome_tray()
