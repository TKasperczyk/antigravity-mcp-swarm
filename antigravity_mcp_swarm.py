#!/usr/bin/env python3
"""
antigravity-mcp-swarm -- Parallel Antigravity (agy) MCP Server

An MCP server that wraps Google's Antigravity CLI (``agy``) with true parallel
execution, live JSONL task monitoring, batch wait, git worktree isolation,
session resume (``--conversation``), and full MCP 2026-07-28 + Tasks extension
support. Built on stdlib-only ``mcp-swarm-core``.

Features:
  - antigravity:        Synchronous execution (or native MCP Task when the
                        client advertises io.modelcontextprotocol/tasks)
  - antigravity_async:  Launch a task, return a task_id immediately (fan-out).
                        NOT fire-and-forget: results come back only via
                        antigravity_wait. Pass conversation_id to resume.
  - antigravity_reply:  Continue a previous conversation via --conversation
                        (automatically preserving worktree/original cwd)
  - antigravity_status: Live view of tool calls, tool errors, cumulative usage,
                        warnings, and failure diagnostics
  - antigravity_wait:   Block until multiple tasks complete and collect results
  - antigravity_cancel: Kill a running async task and its child process tree

Wire protocol to ``agy``:
  - Always invokes ``agy --input-format stream-json --output-format stream-json``
    (never ``-p`` with ``--input-format``, since ``-p`` consumes the next token).
  - Delivers prompts as pure-ASCII single-line NDJSON on stdin:
    ``{"event":"user","message":{"content":"..."}}``
    avoiding OS argv limits, leading-dash option injection, and Windows shell
    quoting issues.
  - Parses live ``init``, ``step_update``, and terminal ``result`` events, plus
    ``AGY_ERROR: {...}`` and ``warning:`` stderr lines.

License: MIT
Repository: https://github.com/TKasperczyk/antigravity-mcp-swarm
"""

import argparse
import json
import logging
import os
import re
import shlex
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import mcp_swarm_core as _core
except ImportError as _exc:
    raise ImportError(
        "antigravity-mcp-swarm requires 'mcp-swarm-core'. Install via "
        "'pip install antigravity-mcp-swarm' or run via 'uvx antigravity-mcp-swarm'."
    ) from _exc

__version__ = "0.1.0"

_IS_WINDOWS = _core.IS_WINDOWS
_DEFAULT_BASE = Path(_core.default_user_tmp_dir("antigravity-swarm"))
_DEFAULT_WORKTREE_DIR = Path(_core.default_worktree_dir("antigravity-swarm"))

# ---------------------------------------------------------------------------
# Configurable paths (created lazily in _ensure_runtime_dirs / main)
# ---------------------------------------------------------------------------
_LOG_ENV = os.environ.get("ANTIGRAVITY_SWARM_LOG")
LOG_FILE = _LOG_ENV or str(_DEFAULT_BASE / "antigravity_mcp_swarm.log")
LOG_LEVEL = os.environ.get("ANTIGRAVITY_SWARM_LOG_LEVEL", "WARNING").upper()

_TASK_DIR_ENV = os.environ.get("ANTIGRAVITY_SWARM_TASK_DIR")
TASK_DIR = Path(_TASK_DIR_ENV) if _TASK_DIR_ENV else (_DEFAULT_BASE / "tasks")

_WORKTREE_DIR_ENV = os.environ.get("ANTIGRAVITY_SWARM_WORKTREE_DIR")
WORKTREE_BASE_DIR = (
    Path(_WORKTREE_DIR_ENV) if _WORKTREE_DIR_ENV else _DEFAULT_WORKTREE_DIR
)


def _configure_logging() -> None:
    is_default = Path(LOG_FILE) == (_DEFAULT_BASE / "antigravity_mcp_swarm.log")
    _core.prepare_log_parent(
        LOG_FILE, is_default=is_default, default_base=_DEFAULT_BASE
    )
    logging.basicConfig(
        filename=LOG_FILE,
        level=getattr(logging, LOG_LEVEL, logging.WARNING),
        format="%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _ensure_runtime_dirs() -> None:
    _core.prepare_runtime_dir(
        TASK_DIR,
        is_default=TASK_DIR == (_DEFAULT_BASE / "tasks"),
        default_base=_DEFAULT_BASE,
    )
    _core.prepare_runtime_dir(
        WORKTREE_BASE_DIR,
        is_default=WORKTREE_BASE_DIR == _DEFAULT_WORKTREE_DIR,
        default_base=_DEFAULT_WORKTREE_DIR.parent,
    )


# ---------------------------------------------------------------------------
# Server-level defaults (populated in main from CLI args)
# ---------------------------------------------------------------------------
_VALID_EFFORTS = ("low", "medium", "high", "xhigh", "max")
_VALID_MODES = ("accept-edits", "auto", "plan")
_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_MAX_DIAGNOSTIC_ITEMS = 100

SERVER_DEFAULTS: Dict[str, Any] = {
    "model": None,
    "effort": None,
    "mode": None,
    "dangerously_skip_permissions": False,
    "sandbox": False,
    "agent": None,
    "add_dir": [],
    "settings": None,
    "disable_slash_commands": False,
    "print_timeout": None,
    "project": None,
    "remote_agent": False,
}

# In-memory conversation_id -> cwd cache so antigravity_reply stays inside
# worktrees / original working directories even after sync calls without .meta.
_SESSION_CWDS: Dict[str, str] = {}
_session_cwds_lock = threading.Lock()
_MAX_SESSION_CWDS = 512

# Polling, progress, wait floor, and cleanup defaults
_POLL_INTERVAL = 2
_PROGRESS_INTERVAL = 20
_CLIENT_AUTO_BACKGROUND_S = 120
_WAIT_MIN_TIMEOUT = _core.parse_env_int(
    "ANTIGRAVITY_SWARM_MIN_WAIT", _CLIENT_AUTO_BACKGROUND_S + 30
)
_TASK_MAX_AGE = _core.parse_env_int("ANTIGRAVITY_SWARM_TASK_MAX_AGE", 86400)
_last_cleanup = 0.0

_PROCESS_CHECK_CMD = (
    '`tasklist /FI "IMAGENAME eq agy.exe"`'
    if _IS_WINDOWS
    else "`pgrep -af 'agy .*stream-json'`"
)


def _record_session_cwd(session_id: Optional[str], cwd: Optional[str]) -> None:
    if not session_id or not cwd:
        return
    with _session_cwds_lock:
        if len(_SESSION_CWDS) >= _MAX_SESSION_CWDS and session_id not in _SESSION_CWDS:
            oldest = next(iter(_SESSION_CWDS))
            _SESSION_CWDS.pop(oldest, None)
        _SESSION_CWDS[session_id] = cwd


def _lookup_session_cwd(session_id: str) -> Optional[str]:
    """Recover the last known existing cwd/worktree for ``session_id``."""
    with _session_cwds_lock:
        cached = _SESSION_CWDS.get(session_id)
    if cached and os.path.isdir(cached):
        return cached

    # Fallback: scan persisted task metadata (e.g. after server restart)
    if not TASK_DIR.exists():
        return None
    best_ts = -1.0
    best_cwd: Optional[str] = None
    for meta_file in TASK_DIR.glob("*.meta"):
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if meta.get("conversation_id") != session_id:
            continue
        candidate = meta.get("worktree_path") or meta.get("cwd")
        ts = float(meta.get("completed_at") or meta.get("started_at") or 0)
        if isinstance(candidate, str) and os.path.isdir(candidate) and ts >= best_ts:
            best_ts = ts
            best_cwd = candidate
    if best_cwd:
        _record_session_cwd(session_id, best_cwd)
    return best_cwd


# ---------------------------------------------------------------------------
# Antigravity CLI launcher resolution
# ---------------------------------------------------------------------------
_agy_launcher_cache_key: Optional[Tuple[bool, Optional[str]]] = None
_agy_launcher_cache_val: Optional[List[str]] = None


def _agy_launcher() -> List[str]:
    """Resolve the executable prefix for ``agy``.

    Respects ``ANTIGRAVITY_SWARM_BIN`` on all platforms. When unset, searches
    ``PATH`` (skipping cwd on Windows to avoid binary hijacking) and falls back
    to ``~/.local/bin/agy`` (standard installer location). On Windows, batch
    shims (``.cmd``/``.bat``) are refused.
    """
    global _agy_launcher_cache_key, _agy_launcher_cache_val
    override = os.environ.get("ANTIGRAVITY_SWARM_BIN")
    cache_key = (_IS_WINDOWS, override)
    if _agy_launcher_cache_key == cache_key and _agy_launcher_cache_val is not None:
        return list(_agy_launcher_cache_val)

    if override:
        has_sep = (os.sep in override) or (os.altsep and os.altsep in override)
        if has_sep:
            resolved = os.path.abspath(override)
        else:
            found_override = _core.safe_which(override, is_windows=_IS_WINDOWS)
            resolved = found_override if found_override else override
        if _IS_WINDOWS and not resolved.lower().endswith(".exe"):
            raise RuntimeError(
                f"ANTIGRAVITY_SWARM_BIN={resolved} must be a .exe file on Windows; "
                "batch files would route arguments through cmd.exe"
            )
        launcher = [resolved]
    else:
        found = _core.safe_which("agy", is_windows=_IS_WINDOWS)
        if not found:
            home_bin = (
                Path.home() / ".local" / "bin" / ("agy.exe" if _IS_WINDOWS else "agy")
            )
            if home_bin.is_file():
                found = str(home_bin)
        if _IS_WINDOWS:
            if not found:
                raise RuntimeError(
                    "agy not found on PATH or ~/.local/bin (set ANTIGRAVITY_SWARM_BIN)"
                )
            if not found.lower().endswith(".exe"):
                raise RuntimeError(
                    f"{found} is not a .exe binary; refusing to run agy through cmd.exe "
                    "(set ANTIGRAVITY_SWARM_BIN)"
                )
            launcher = [os.path.abspath(found)]
        else:
            launcher = [os.path.abspath(found)] if found else ["agy"]

    logging.info("Antigravity launcher: %s", launcher)
    _agy_launcher_cache_key = cache_key
    _agy_launcher_cache_val = launcher
    return list(launcher)


def _exec_argv(cmd: List[str]) -> List[str]:
    """Map a logical ``agy ...`` command to the actual executable argv."""
    if cmd and cmd[0] == "agy":
        return _agy_launcher() + cmd[1:]
    return cmd


# ---------------------------------------------------------------------------
# Stdin NDJSON formatter & command builder
# ---------------------------------------------------------------------------
def _format_stdin_payload(params: Dict[str, Any]) -> Optional[str]:
    """Format the prompt as a single pure-ASCII NDJSON user event line."""
    prompt = params.get("prompt", "")
    if not prompt:
        return None
    return (
        json.dumps(
            {"event": "user", "message": {"content": prompt}},
            ensure_ascii=True,
        )
        + "\n"
    )


def _encode_json_or_str(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, str) and value.strip():
        return value
    return None


def _resolve_model(params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    params = params or {}
    model = params.get("model")
    if isinstance(model, str) and model.strip():
        return model.strip()
    srv_model = SERVER_DEFAULTS.get("model")
    if isinstance(srv_model, str) and srv_model.strip():
        return srv_model.strip()
    return None


def _resolve_effort(params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    params = params or {}
    effort = params.get("effort")
    if isinstance(effort, str) and effort.strip():
        return effort.strip()
    srv_effort = SERVER_DEFAULTS.get("effort")
    if isinstance(srv_effort, str) and srv_effort.strip():
        return srv_effort.strip()
    return None


def _resolve_mode(params: Optional[Dict[str, Any]] = None) -> Optional[str]:
    params = params or {}
    mode = params.get("mode")
    if isinstance(mode, str) and mode.strip():
        return mode.strip()
    srv_mode = SERVER_DEFAULTS.get("mode")
    if isinstance(srv_mode, str) and srv_mode.strip():
        return srv_mode.strip()
    return None


_AGY_VALUE_FLAGS = frozenset({
    "--input-format",
    "--output-format",
    "--conversation",
    "--model",
    "--effort",
    "--mode",
    "--agent",
    "--add-dir",
    "--json-schema",
    "--settings",
    "--print-timeout",
    "--project",
    "--log-file",
})


def _model_from_command(command: Any) -> Optional[str]:
    """Extract ``--model`` from a stored ``agy`` command list or shell string."""
    if isinstance(command, (list, tuple)):
        tokens = [str(t) for t in command]
    elif isinstance(command, str) and command.strip():
        try:
            tokens = shlex.split(command)
        except ValueError:
            tokens = command.split()
    else:
        return None

    i = (
        1
        if tokens and os.path.basename(tokens[0]).lower() in ("agy", "agy.exe")
        else 0
    )
    found_model: Optional[str] = None
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--" or not tok.startswith("-"):
            break
        if tok == "--model":
            if i + 1 < len(tokens):
                val = tokens[i + 1].strip()
                if val:
                    found_model = val
                i += 2
                continue
            break
        if tok.startswith("--model="):
            val = tok.split("=", 1)[1].strip()
            if val:
                found_model = val
            i += 1
            continue
        if tok in _AGY_VALUE_FLAGS:
            i += 2
            continue
        i += 1
    return found_model


def _build_command(params: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
    """Build an ``agy --input-format stream-json --output-format stream-json`` command.

    Value flags use ``--flag=value`` syntax so user-supplied values starting
    with ``-`` can never be misinterpreted as separate CLI options.
    """
    cmd: List[str] = [
        "agy",
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
    ]

    conv_id = params.get("conversation_id")
    if isinstance(conv_id, str) and conv_id.strip():
        cmd.append(f"--conversation={conv_id.strip()}")

    model = _resolve_model(params)
    if model:
        cmd.append(f"--model={model}")

    effort = _resolve_effort(params)
    if effort:
        cmd.append(f"--effort={effort}")

    mode = _resolve_mode(params)
    if mode:
        cmd.append(f"--mode={mode}")

    agent = params.get("agent")
    if not (isinstance(agent, str) and agent.strip()):
        agent = SERVER_DEFAULTS.get("agent")
    if isinstance(agent, str) and agent.strip():
        cmd.append(f"--agent={agent.strip()}")

    project = params.get("project")
    if not (isinstance(project, str) and project.strip()):
        project = SERVER_DEFAULTS.get("project")
    if isinstance(project, str) and project.strip():
        cmd.append(f"--project={project.strip()}")

    print_timeout = params.get("print_timeout")
    if not (isinstance(print_timeout, str) and print_timeout.strip()):
        print_timeout = SERVER_DEFAULTS.get("print_timeout")
    if isinstance(print_timeout, str) and print_timeout.strip():
        cmd.append(f"--print-timeout={print_timeout.strip()}")

    # Tri-state boolean flags: explicit per-call True/False overrides server default
    for param_key, cli_flag in (
        ("dangerously_skip_permissions", "--dangerously-skip-permissions"),
        ("sandbox", "--sandbox"),
        ("disable_slash_commands", "--disable-slash-commands"),
        ("remote_agent", "--remote-agent"),
    ):
        val = params.get(param_key)
        effective = bool(SERVER_DEFAULTS.get(param_key)) if val is None else bool(val)
        if effective:
            cmd.append(cli_flag)

    # Repeatable --add-dir: merge server defaults + per-call additions
    dirs: List[str] = []
    seen_dirs = set()
    for src in (SERVER_DEFAULTS.get("add_dir") or [], params.get("add_dir") or []):
        for entry in src:
            if isinstance(entry, str) and entry.strip() and entry not in seen_dirs:
                seen_dirs.add(entry)
                dirs.append(entry)
    for d in dirs:
        cmd.append(f"--add-dir={d}")

    # Session-only settings (per-call overrides server default)
    raw_settings = (
        params["settings"]
        if params.get("settings") is not None
        else SERVER_DEFAULTS.get("settings")
    )
    encoded_settings = _encode_json_or_str(raw_settings)
    if encoded_settings is not None:
        cmd.append(f"--settings={encoded_settings}")

    # Optional structured output JSON schema (per-call)
    encoded_schema = _encode_json_or_str(params.get("json_schema"))
    if encoded_schema is not None:
        cmd.append(f"--json-schema={encoded_schema}")

    cwd = params.get("cwd")
    proc_cwd = os.path.abspath(cwd) if isinstance(cwd, str) and cwd else None
    return cmd, proc_cwd


# ---------------------------------------------------------------------------
# Antigravity stream-json JSONL parser & diagnostics
# ---------------------------------------------------------------------------
_TOOL_ARG_PRIORITY_KEYS = (
    "CommandLine",
    "command",
    "cmd",
    "Path",
    "path",
    "TargetFile",
    "AbsolutePath",
    "DirectoryPath",
    "SearchPath",
    "Pattern",
    "Query",
    "query",
    "Url",
    "url",
    "Prompt",
)

_USAGE_KEYS = (
    "input_tokens",
    "output_tokens",
    "thinking_tokens",
    "cache_read_tokens",
    "total_tokens",
)


def _append_bounded(lst: List[Any], item: Any, limit: int = _MAX_DIAGNOSTIC_ITEMS) -> None:
    if len(lst) >= limit:
        del lst[0]
    lst.append(item)


def _extract_tool_args_preview(parameters: Any) -> Optional[str]:
    if parameters is None:
        return None
    if isinstance(parameters, dict):
        for key in _TOOL_ARG_PRIORITY_KEYS:
            val = parameters.get(key)
            if val is not None:
                return val[:150] if isinstance(val, str) else str(val)[:150]
        if parameters:
            return str(parameters)[:150]
        return None
    return parameters[:150] if isinstance(parameters, str) else str(parameters)[:150]


def _extract_tool_error(raw_err: Any) -> Tuple[str, str]:
    if isinstance(raw_err, dict):
        err_type = str(raw_err.get("type") or "")
        msg = _core.error_message(raw_err) or err_type or "tool error"
        return err_type, msg
    if isinstance(raw_err, str) and raw_err.strip():
        return "", raw_err.strip()
    return "", "tool error"


def _sanitize_usage(raw: Any) -> Optional[Dict[str, int]]:
    if not isinstance(raw, dict):
        return None
    out: Dict[str, int] = {}
    for key in _USAGE_KEYS:
        val = raw.get(key)
        if isinstance(val, int) and not isinstance(val, bool):
            out[key] = val
    return out or None


def _sum_step_usages(step_usages: Dict[Any, Dict[str, int]]) -> Optional[Dict[str, int]]:
    if not step_usages:
        return None
    totals: Dict[str, int] = {k: 0 for k in _USAGE_KEYS}
    for u in step_usages.values():
        for k in _USAGE_KEYS:
            totals[k] += u.get(k, 0)
    return totals if any(totals.values()) else None


def _new_agy_jsonl_state() -> Dict[str, Any]:
    return {
        "conversation_id": None,
        "model": None,
        "permission_mode": None,
        "final_response_text": None,
        "active_response_step": None,
        "active_response_chunks": [],
        "active_thinking_step": None,
        "active_thinking_chunks": [],
        "seen_tool_steps": set(),
        "step_usages": {},
        "result_usage": None,
        "errors": [],
        "warnings": [],
        "tool_errors": [],
        "terminal_events": [],
        "terminal_error": None,
        "finished_reason": None,
        "structured_output": None,
        "has_structured_output": False,
        "duration_seconds": None,
        "num_turns": None,
        "saw_json": False,
        "saw_turn_started": False,
        "phase": "starting",
        "tools_called": 0,
        "last_tool": None,
        "last_tool_args": None,
    }


def _clone_agy_jsonl_state(state: Dict[str, Any]) -> Dict[str, Any]:
    cloned = dict(state)
    cloned["active_response_chunks"] = list(state["active_response_chunks"])
    cloned["active_thinking_chunks"] = list(state["active_thinking_chunks"])
    cloned["seen_tool_steps"] = set(state["seen_tool_steps"])
    cloned["step_usages"] = {k: dict(v) for k, v in state["step_usages"].items()}
    if isinstance(state["result_usage"], dict):
        cloned["result_usage"] = dict(state["result_usage"])
    cloned["errors"] = list(state["errors"])
    cloned["warnings"] = list(state["warnings"])
    cloned["tool_errors"] = list(state["tool_errors"])
    cloned["terminal_events"] = list(state["terminal_events"])
    return cloned


def _consume_agy_jsonl_lines(state: Dict[str, Any], lines: List[str]) -> None:
    """Parse ``agy --output-format stream-json`` NDJSON events in a single pass."""
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue

        state["saw_json"] = True
        etype = event.get("event") or event.get("type") or ""

        top_cid = event.get("conversation_id")
        if isinstance(top_cid, str) and top_cid:
            state["conversation_id"] = top_cid

        if etype == "init":
            state["saw_turn_started"] = True
            state["phase"] = "running"
            init_obj = event.get("init")
            if isinstance(init_obj, dict):
                model = init_obj.get("model")
                if isinstance(model, str) and model:
                    state["model"] = model
                perm = init_obj.get("permission_mode")
                if isinstance(perm, str) and perm:
                    state["permission_mode"] = perm
        elif etype == "step_update":
            su = event.get("step_update")
            if not isinstance(su, dict):
                continue
            state["saw_turn_started"] = True
            state["phase"] = "running"
            su_cid = su.get("conversation_id")
            if isinstance(su_cid, str) and su_cid:
                state["conversation_id"] = su_cid

            step_type = su.get("step_type", "")
            step_state = str(su.get("state") or "").upper()
            step_index = su.get("step_index")
            valid_step_idx = isinstance(step_index, int) and not isinstance(
                step_index, bool
            )

            if step_type == "tool":
                tool_info = su.get("tool_info")
                if not isinstance(tool_info, dict):
                    tool_info = {}
                raw_tool = su.get("tool_name") or tool_info.get("name") or "tool"
                tool_name = str(raw_tool)
                if valid_step_idx:
                    if step_index not in state["seen_tool_steps"]:
                        state["seen_tool_steps"].add(step_index)
                        state["tools_called"] += 1
                elif step_state == "ACTIVE":
                    state["tools_called"] += 1
                state["last_tool"] = tool_name
                args_preview = _extract_tool_args_preview(tool_info.get("parameters"))
                if args_preview is not None:
                    state["last_tool_args"] = args_preview

                if step_state == "ERROR":
                    err_type, err_msg = _extract_tool_error(tool_info.get("error"))
                    _append_bounded(state["warnings"], f"{tool_name}: {err_msg}")
                    entry: Dict[str, str] = {
                        "tool": tool_name,
                        "message": err_msg[:300],
                    }
                    if err_type:
                        entry["type"] = err_type
                    _append_bounded(state["tool_errors"], entry)

            elif step_type == "agent_response":
                # Streamed text chunks (joined lazily in _finalize_agy_jsonl_views
                # to stay O(N) instead of O(N^2) across many deltas).
                text_delta = su.get("text_delta")
                if isinstance(text_delta, str) and text_delta:
                    if step_index != state["active_response_step"]:
                        state["active_response_step"] = step_index
                        state["active_response_chunks"] = [text_delta]
                    else:
                        state["active_response_chunks"].append(text_delta)

                for tkey in ("thinking_delta", "thought_delta", "reasoning_delta"):
                    td = su.get(tkey)
                    if isinstance(td, str) and td:
                        if step_index != state["active_thinking_step"]:
                            state["active_thinking_step"] = step_index
                            state["active_thinking_chunks"] = [td]
                        else:
                            state["active_thinking_chunks"].append(td)

                usage = _sanitize_usage(su.get("usage"))
                if usage:
                    ukey = step_index if valid_step_idx else f"anon_{len(state['step_usages'])}"
                    state["step_usages"][ukey] = usage

                if step_state == "ERROR":
                    msg = _core.error_message(su.get("error")) or _core.error_message(
                        su.get("message")
                    )
                    if msg:
                        _append_bounded(state["errors"], msg)

            elif step_type == "error" or step_state == "ERROR":
                msg = _core.error_message(su.get("error")) or _core.error_message(
                    su.get("message")
                )
                if msg:
                    _append_bounded(state["errors"], msg)

        elif etype == "error":
            msg = _core.error_message(event.get("message")) or _core.error_message(
                event.get("error")
            )
            if msg:
                _append_bounded(state["errors"], msg)

        elif etype == "result":
            res = event.get("result")
            if not isinstance(res, dict):
                continue
            res_cid = res.get("conversation_id")
            if isinstance(res_cid, str) and res_cid:
                state["conversation_id"] = res_cid

            res_status = str(res.get("status") or "").upper()
            finished_reason = res.get("finished_reason")
            if isinstance(finished_reason, str) and finished_reason:
                state["finished_reason"] = finished_reason

            response_text = res.get("response")
            if isinstance(response_text, str) and response_text:
                state["final_response_text"] = response_text

            if "structured_output" in res and res["structured_output"] is not None:
                state["structured_output"] = res["structured_output"]
                state["has_structured_output"] = True

            usage = _sanitize_usage(res.get("usage"))
            if usage:
                state["result_usage"] = usage

            dur = res.get("duration_seconds")
            if isinstance(dur, (int, float)) and not isinstance(dur, bool):
                state["duration_seconds"] = round(float(dur), 3)

            turns = res.get("num_turns")
            if isinstance(turns, int) and not isinstance(turns, bool):
                state["num_turns"] = turns

            # Note: agy emits status="SUCCESS" with finished_reason="print_timeout"
            # and exit 0 when --print-timeout aborts an in-progress turn.
            failed_reasons = ("error", "print_timeout", "failed", "cancelled")
            if res_status == "SUCCESS" and finished_reason not in failed_reasons:
                state["terminal_events"].append("completed")
            else:
                state["terminal_events"].append("failed")
                err_msg = _core.error_message(res.get("error"))
                if not err_msg and finished_reason == "print_timeout":
                    err_msg = (
                        "Antigravity print timeout reached before the turn completed "
                        "(finished_reason=print_timeout)."
                    )
                elif not err_msg and finished_reason:
                    err_msg = (
                        f"Antigravity turn ended unsuccessfully "
                        f"(status={res_status or 'unknown'}, finished_reason={finished_reason})."
                    )
                elif not err_msg:
                    err_msg = (
                        f"Antigravity turn failed (status={res_status or 'ERROR'})."
                    )
                state["terminal_error"] = err_msg


def _finalize_agy_jsonl_views(
    state: Dict[str, Any]
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    terminal_events = list(state["terminal_events"])
    terminal_types = set(terminal_events)
    terminal_status: Optional[str] = None
    terminal_conflict = False
    if len(terminal_types) == 1:
        terminal_status = next(iter(terminal_types))
    elif len(terminal_types) > 1:
        terminal_conflict = True

    streamed_text = (
        "".join(state["active_response_chunks"])
        if state["active_response_chunks"]
        else None
    )
    last_assistant_text = (
        state["final_response_text"]
        if state["final_response_text"] is not None
        else streamed_text
    )
    last_assistant_preview = last_assistant_text[-300:] if last_assistant_text else None
    last_reasoning = (
        "".join(state["active_thinking_chunks"])[-200:]
        if state["active_thinking_chunks"]
        else None
    )

    usage = (
        dict(state["result_usage"])
        if isinstance(state["result_usage"], dict)
        else _sum_step_usages(state["step_usages"])
    )

    errors = list(state["errors"])
    warnings = list(state["warnings"])
    tool_errors = list(state["tool_errors"])

    analysis: Dict[str, Any] = {
        "conversation_id": state["conversation_id"],
        "model": state["model"],
        "permission_mode": state["permission_mode"],
        "last_assistant_text": last_assistant_text,
        "errors": errors,
        "warnings": warnings,
        "tool_errors": tool_errors,
        "terminal_events": terminal_events,
        "terminal_status": terminal_status,
        "terminal_error": state["terminal_error"],
        "terminal_conflict": terminal_conflict,
        "finished_reason": state["finished_reason"],
        "structured_output": (
            state["structured_output"] if state["has_structured_output"] else None
        ),
        "has_structured_output": state["has_structured_output"],
        "usage": usage,
        "duration_seconds": state["duration_seconds"],
        "num_turns": state["num_turns"],
        "saw_json": state["saw_json"],
        "saw_turn_started": state["saw_turn_started"],
    }

    phase = state["phase"]
    if terminal_status:
        phase = terminal_status
    elif terminal_conflict:
        phase = "unknown (conflicting terminal events)"

    status: Dict[str, Any] = {
        "phase": phase,
        "tools_called": state["tools_called"],
        "last_tool": state["last_tool"],
        "last_tool_args": state["last_tool_args"],
        "last_reasoning": last_reasoning,
        "last_assistant_text": last_assistant_preview,
        "last_error": errors[-1] if errors else None,
        "last_warning": warnings[-1] if warnings else None,
        "terminal_error": state["terminal_error"],
        "terminal_conflict": terminal_conflict,
        "tool_errors": tool_errors,
        "permission_mode": state["permission_mode"],
    }
    return analysis, status


_JSONL_CACHE_OBJ = _core.IncrementalJsonlCache(
    _core.JsonlParserAdapter(
        new_state=_new_agy_jsonl_state,
        clone_state=_clone_agy_jsonl_state,
        consume_lines=_consume_agy_jsonl_lines,
        finalize_views=_finalize_agy_jsonl_views,
    )
)


def _parse_jsonl_full(jsonl_text: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _JSONL_CACHE_OBJ.parse_text(jsonl_text)


def _parse_jsonl_file(stdout_path: Path) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _JSONL_CACHE_OBJ.parse_file(stdout_path)


def _extract_conversation_id_fast(
    stdout_path: Path, meta: Optional[Dict[str, Any]] = None
) -> Optional[str]:
    """Read the opening ``init`` event from ``.stdout`` to discover the actual conversation_id."""
    if stdout_path.exists():
        try:
            with open(stdout_path, "r", encoding="utf-8", errors="replace") as f:
                bytes_read = 0
                for line in f:
                    bytes_read += len(line)
                    stripped = line.strip()
                    if stripped:
                        try:
                            event = json.loads(stripped)
                        except json.JSONDecodeError:
                            event = None
                        if isinstance(event, dict):
                            cid = event.get("conversation_id")
                            if isinstance(cid, str) and cid:
                                return cid
                            su = event.get("step_update")
                            if isinstance(su, dict):
                                cid = su.get("conversation_id")
                                if isinstance(cid, str) and cid:
                                    return cid
                    if bytes_read >= 65536:
                        break
        except OSError:
            pass
    if (
        meta
        and isinstance(meta.get("conversation_id"), str)
        and meta["conversation_id"]
    ):
        return meta["conversation_id"]
    return None


def _parse_agy_stderr_error(stderr: str) -> Optional[Dict[str, Any]]:
    """Extract the last structured ``AGY_ERROR: {...}`` payload from stderr, if any."""
    if not stderr:
        return None
    for line in reversed(stderr.splitlines()):
        idx = line.find("AGY_ERROR:")
        if idx >= 0:
            raw = line[idx + len("AGY_ERROR:") :].strip()
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                continue
    return None


def _extract_stderr_warnings(stderr: str) -> List[str]:
    """Extract CLI ``warning: ...`` lines from stderr (deduplicated, bounded)."""
    if not stderr:
        return []
    warnings: List[str] = []
    seen = set()
    for line in stderr.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("warning:"):
            msg = stripped[len("warning:") :].strip()
            if msg and msg not in seen:
                seen.add(msg)
                warnings.append(msg[:300])
                if len(warnings) >= 20:
                    break
    return warnings


def _extract_stderr_error_line(stderr: str) -> Optional[str]:
    """Extract the last plain ``error: ...`` line from stderr, if present."""
    if not stderr:
        return None
    for line in reversed(stderr.splitlines()):
        stripped = line.strip()
        if stripped.lower().startswith("error:"):
            msg = stripped[len("error:") :].strip()
            if msg:
                return msg
    return None


def _classify_agy_failure(
    message: str,
    *,
    agy_err: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    lower = message.lower()
    reason = str((agy_err or {}).get("reason") or "").lower()

    if reason == "print_timeout" or "print_timeout" in lower or "print timeout" in lower:
        return {
            "category": "print_timeout",
            "retryable": False,
            "action": (
                "Increase or remove print_timeout, or run asynchronously with "
                "antigravity_async + antigravity_wait."
            ),
        }

    if "unavailable with --input-format stream-json" in lower:
        return {
            "category": "invalid_request",
            "retryable": False,
            "action": (
                "Pass disable_slash_commands=true if the prompt starts with '/' "
                "and is intended as plain text rather than a CLI slash command."
            ),
        }

    classified = _core.classify_failure(message, product_name="Antigravity")
    if agy_err and isinstance(agy_err.get("retryable"), bool):
        if classified["retryable"] is None:
            classified["retryable"] = agy_err["retryable"]
    return classified


def _build_agy_failure_info(
    stdout: str,
    stderr: str,
    command: Any = None,
    analysis: Optional[Dict[str, Any]] = None,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    if analysis is None:
        analysis, _ = _parse_jsonl_full(stdout)
    agy_err = _parse_agy_stderr_error(stderr)

    effective_analysis = dict(analysis)
    if (
        agy_err
        and isinstance(agy_err.get("short_error"), str)
        and agy_err["short_error"].strip()
    ):
        short_err = agy_err["short_error"].strip()
        cur_term = effective_analysis.get("terminal_error")
        if not cur_term or cur_term.startswith("Antigravity print timeout reached"):
            effective_analysis["terminal_error"] = short_err
    elif not effective_analysis.get("terminal_error") and not effective_analysis.get("errors"):
        stderr_err = _extract_stderr_error_line(stderr)
        if stderr_err:
            effective_analysis["terminal_error"] = stderr_err

    return _core.build_failure_info(
        stdout,
        stderr,
        analysis=effective_analysis,
        product_name="Antigravity",
        command=command,
        model=model or effective_analysis.get("model"),
        model_from_command_fn=_model_from_command,
        classify_fn=lambda msg: _classify_agy_failure(msg, agy_err=agy_err),
    )


def _extract_agy_result(
    stdout: str, stderr: str, analysis: Optional[Dict[str, Any]] = None
) -> Tuple[str, Optional[str]]:
    if analysis is None:
        analysis, _ = _parse_jsonl_full(stdout)
    cid = analysis.get("conversation_id")
    if analysis.get("saw_json"):
        if analysis.get("last_assistant_text"):
            return analysis["last_assistant_text"], cid
        if analysis.get("has_structured_output"):
            return (
                json.dumps(analysis["structured_output"], ensure_ascii=False),
                cid,
            )
        # Never leak raw NDJSON stream lines as the assistant result text.
        return "", cid
    result = stdout.strip()
    if not result and stderr:
        result = stderr.strip()
    return result or "No output from Antigravity", cid


def _format_tool_errors_note(tool_errors: List[Dict[str, str]]) -> str:
    lines = [f"Tool errors observed during turn ({len(tool_errors)}):"]
    for item in tool_errors[:5]:
        etype = f" ({item['type']})" if item.get("type") else ""
        lines.append(f"  - {item.get('tool', 'tool')}{etype}: {item.get('message', '')}")
    if len(tool_errors) > 5:
        lines.append(f"  - ... and {len(tool_errors) - 5} more")
    return "\n".join(lines)


def _format_cli_warnings_note(warnings: List[str]) -> str:
    lines = [f"CLI warnings ({len(warnings)}):"]
    for w in warnings[:5]:
        lines.append(f"  - {w}")
    if len(warnings) > 5:
        lines.append(f"  - ... and {len(warnings) - 5} more")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------
def _validate_agy_params(
    args: Dict[str, Any], *, is_reply: bool = False
) -> Optional[str]:
    cid = args.get("conversation_id")
    if cid is not None:
        if not isinstance(cid, str) or not _UUID_RE.fullmatch(cid.strip()):
            return (
                "conversation_id must be a valid UUID string "
                "(from a previous Antigravity response, not a task_id)"
            )
    if not is_reply:
        wt = args.get("worktree")
        if wt is not None and not isinstance(wt, bool):
            return "worktree must be a boolean"

    effort = args.get("effort")
    if effort is not None:
        if not isinstance(effort, str) or effort not in _VALID_EFFORTS:
            return f"effort must be one of {', '.join(_VALID_EFFORTS)}"

    mode = args.get("mode")
    if mode is not None:
        if not isinstance(mode, str) or mode not in _VALID_MODES:
            return f"mode must be one of {', '.join(_VALID_MODES)}"

    for str_key in ("cwd", "model", "agent", "print_timeout", "project"):
        val = args.get(str_key)
        if val is not None and not isinstance(val, str):
            return f"{str_key} must be a string"

    for bool_key in (
        "dangerously_skip_permissions",
        "sandbox",
        "disable_slash_commands",
        "remote_agent",
    ):
        val = args.get(bool_key)
        if val is not None and not isinstance(val, bool):
            return f"{bool_key} must be a boolean"

    add_dir = args.get("add_dir")
    if add_dir is not None:
        if not isinstance(add_dir, list):
            return "add_dir must be an array of directory path strings"
        for item in add_dir:
            if not isinstance(item, str) or not item.strip():
                return "Every element of add_dir must be a non-empty string"

    for json_or_str_key in ("json_schema", "settings"):
        val = args.get(json_or_str_key)
        if val is not None:
            if not isinstance(val, dict) and not (
                isinstance(val, str) and val.strip()
            ):
                return f"{json_or_str_key} must be a JSON object or non-empty string"

    return None


# ---------------------------------------------------------------------------
# Server instructions, Tool definitions & Resources
# ---------------------------------------------------------------------------
SERVER_INSTRUCTIONS = (
    "antigravity-mcp-swarm wraps the Antigravity CLI (`agy`) for parallel execution, "
    "conversation resume, git worktree isolation, and live progress monitoring.\n\n"
    "Execution lifecycle:\n"
    "- Sync calls (`antigravity`, `antigravity_reply`) have no server timeout, but MCP "
    "clients typically enforce ~300s limits; use `antigravity_async` + `antigravity_wait` "
    "for long runs or parallel fan-out. Clients supporting MCP 2026-07-28 with "
    "`io.modelcontextprotocol/tasks` receive native async tasks directly from "
    "`antigravity` and `antigravity_reply` (poll via `tasks/get`, cancel via `tasks/cancel`).\n"
    "- `antigravity_async` task IDs are internal to this server and NOT tracked by legacy "
    "MCP clients. Always call `antigravity_wait` in the same turn (passing all launched "
    "task_ids in one call). `antigravity_status` is a read-only peek and collects nothing.\n"
    "- Let `antigravity_wait` run past the client's ~120s auto-backgrounding threshold; "
    "short timeouts are raised to a floor (finished tasks still return instantly). "
    "A wait timeout is NOT a failure and does not kill tasks -- call `antigravity_wait` again.\n\n"
    "Session resume, slash commands & diagnostics:\n"
    "- `conversation_id` must be a UUID returned by Antigravity (never a `task_id`). If "
    "`agy` cannot find the requested conversation and falls back to a fresh one, this "
    "server reports `conversation_not_found` (`isError: true`) rather than silently succeeding.\n"
    "- `antigravity_reply` automatically defaults `cwd` to the original conversation's "
    "working directory (including its worktree path if still present) unless overridden.\n"
    "- Prompts beginning with a built-in CLI slash command (e.g. `/help`) fail under "
    "`--input-format stream-json` unless `disable_slash_commands: true` is set. Note that "
    "`--mode plan` requires slash-command expansion and has no effect when slash commands "
    "are disabled.\n"
    "- On resumed conversations, `usage`, `num_turns`, and `duration_seconds` reported by "
    "`agy` are cumulative across the conversation.\n\n"
    "Worktree isolation:\n"
    "- `worktree: true` creates an isolated git worktree and branch (`antigravity-swarm/<id>`) "
    "based on `HEAD`. Uncommitted and untracked working-tree changes are NOT included "
    "in the worktree -- commit them first if the task needs them. Expired worktrees "
    "(default 24h) are force-removed and their branches deleted on a subsequent launch."
)

_AGY_COMMON_OVERRIDE_PROPERTIES: Dict[str, Any] = {
    "cwd": {
        "type": "string",
        "description": (
            "Working directory for the Antigravity session. "
            "If relative, resolved against the server's cwd. On resume, defaults "
            "to the original conversation's cwd/worktree when omitted."
        ),
    },
    "model": {
        "type": "string",
        "description": (
            "Optional model override (e.g. 'gemini-4-neon-high', 'gemini-4-neon', "
            "'gemini-3.8-flash-medium', 'claude-opus-5-5-high')."
        ),
    },
    "effort": {
        "type": "string",
        "enum": list(_VALID_EFFORTS),
        "description": "Reasoning effort level for this session.",
    },
    "mode": {
        "type": "string",
        "enum": list(_VALID_MODES),
        "description": (
            "Execution mode (accept-edits, auto, plan). Note: plan mode requires "
            "slash command expansion (avoid setting disable_slash_commands=true with plan)."
        ),
    },
    "dangerously_skip_permissions": {
        "type": "boolean",
        "description": (
            "Auto-approve all tool permission requests without prompting "
            "(passes --dangerously-skip-permissions). Explicit false overrides server default."
        ),
    },
    "sandbox": {
        "type": "boolean",
        "description": (
            "Run in a sandbox with terminal restrictions enabled (passes --sandbox). "
            "Explicit false overrides server default."
        ),
    },
    "agent": {
        "type": "string",
        "description": "Named Antigravity agent for this session (passes --agent).",
    },
    "add_dir": {
        "type": "array",
        "items": {"type": "string"},
        "description": "Additional directories to add to the workspace (passes repeatable --add-dir).",
    },
    "json_schema": {
        "description": (
            "Optional JSON Schema (object or string/path) to enforce structured output "
            "on the final result (passes --json-schema)."
        ),
    },
    "settings": {
        "description": (
            "Session-only settings (JSON object or file path, never saved; passes --settings)."
        ),
    },
    "disable_slash_commands": {
        "type": "boolean",
        "description": (
            "Disable slash command and skill expansion in print mode. Set true when "
            "sending plain-text prompts that begin with '/'."
        ),
    },
    "print_timeout": {
        "type": "string",
        "description": (
            "Optional time limit for agy print mode (Go duration string like '300s' or '10m'; "
            "'0s' waits until completion)."
        ),
    },
    "project": {
        "type": "string",
        "description": "Project ID or name for this CLI session (passes --project).",
    },
    "remote_agent": {
        "type": "boolean",
        "description": "Run this conversation using remote agents (passes --remote-agent).",
    },
}

_AGY_PROPERTIES: Dict[str, Any] = {
    "conversation_id": {
        "type": "string",
        "description": (
            "Optional conversation UUID from a previous Antigravity call. When set, "
            "resumes that conversation via --conversation instead of starting fresh."
        ),
    },
    "prompt": {
        "type": "string",
        "description": "The user prompt for the Antigravity session.",
    },
    "worktree": {
        "type": "boolean",
        "description": (
            "Create an isolated git worktree and branch (antigravity-swarm/<task_id>) "
            "based on HEAD so parallel tasks never conflict. Note: branches from HEAD "
            "and excludes uncommitted or untracked working-tree changes -- commit them "
            "first if the task needs them. Merge the branch back before expiry."
        ),
    },
    **_AGY_COMMON_OVERRIDE_PROPERTIES,
}

_USAGE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "input_tokens": {"type": "integer"},
        "output_tokens": {"type": "integer"},
        "thinking_tokens": {"type": "integer"},
        "cache_read_tokens": {"type": "integer"},
        "total_tokens": {"type": "integer"},
    },
}

_TOOL_ERRORS_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "tool": {"type": "string"},
            "type": {"type": "string"},
            "message": {"type": "string"},
        },
        "required": ["tool", "message"],
    },
}

_WARNINGS_SCHEMA: Dict[str, Any] = {
    "type": "array",
    "items": {"type": "string"},
}

_AGY_EXTRA_RESULT_SCHEMA: Dict[str, Any] = {
    "permission_mode": {"type": "string"},
    "finished_reason": {"type": "string"},
    "duration_seconds": {"type": "number"},
    "num_turns": {"type": "integer"},
    "usage": _USAGE_SCHEMA,
    "structured_output": {},
    "tool_errors": _TOOL_ERRORS_SCHEMA,
    "warnings": _WARNINGS_SCHEMA,
    "effort": {"type": "string"},
    "mode": {"type": "string"},
}

_AGY_EXTRA_STATUS_SCHEMA: Dict[str, Any] = {
    "permission_mode": {"type": "string"},
    "finished_reason": {"type": "string"},
    "usage": _USAGE_SCHEMA,
    "tool_errors": _TOOL_ERRORS_SCHEMA,
    "effort": {"type": "string"},
    "mode": {"type": "string"},
}

_AGY_OUTPUT_SCHEMA = _core.make_sync_output_schema(
    "conversation_id", extra_properties=_AGY_EXTRA_RESULT_SCHEMA
)
_AGY_ASYNC_OUTPUT_SCHEMA = _core.make_async_output_schema()
_AGY_REPLY_OUTPUT_SCHEMA = _core.make_reply_output_schema(
    "conversation_id", extra_properties=_AGY_EXTRA_RESULT_SCHEMA
)
_AGY_STATUS_OUTPUT_SCHEMA = _core.make_status_output_schema(
    "conversation_id", extra_task_properties=_AGY_EXTRA_STATUS_SCHEMA
)
_AGY_WAIT_OUTPUT_SCHEMA = _core.make_wait_output_schema(
    "conversation_id", extra_task_properties=_AGY_EXTRA_RESULT_SCHEMA
)
_AGY_CANCEL_OUTPUT_SCHEMA = _core.make_cancel_output_schema()

TOOLS = [
    {
        "name": "antigravity",
        "description": (
            "Run an Antigravity (`agy`) session synchronously (or as a native MCP "
            "Task when the client advertises io.modelcontextprotocol/tasks). "
            "This server applies NO timeout of its own, but legacy MCP clients "
            "typically cut a tool call around 300s -- use antigravity_async + "
            "antigravity_wait for long runs or parallel fan-out on non-Tasks clients."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _AGY_PROPERTIES,
            "required": ["prompt"],
        },
        "outputSchema": _AGY_OUTPUT_SCHEMA,
        "annotations": {
            "title": "Run Antigravity",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    },
    {
        "name": "antigravity_async",
        "description": (
            "Launch an Antigravity task and return immediately with a task_id for "
            "parallel fan-out, then collect with a single antigravity_wait. "
            "CRITICAL: the task_id is internal to this server and NOT tracked by "
            "your MCP client -- you MUST call antigravity_wait in the same turn or "
            "results are stranded on disk. Pass conversation_id (UUID) to resume an "
            "existing conversation asynchronously, and set worktree=true to isolate "
            "parallel edits in a HEAD-based git worktree."
        ),
        "inputSchema": {
            "type": "object",
            "properties": _AGY_PROPERTIES,
            "required": ["prompt"],
        },
        "outputSchema": _AGY_ASYNC_OUTPUT_SCHEMA,
        "annotations": {
            "title": "Launch Async Antigravity Task",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    },
    {
        "name": "antigravity_reply",
        "description": (
            "Continue an Antigravity conversation via `agy --conversation <uuid>` "
            "(runs synchronously, or as a native MCP Task when the client "
            "advertises io.modelcontextprotocol/tasks). Automatically reuses the "
            "original conversation's cwd/worktree when cwd is omitted. For long "
            "follow-ups on non-Tasks clients, prefer antigravity_async(conversation_id=..., "
            "prompt=...) + antigravity_wait."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "conversation_id": {
                    "type": "string",
                    "description": "The conversation UUID from a previous Antigravity call.",
                },
                "prompt": {
                    "type": "string",
                    "description": "The follow-up prompt to continue the conversation.",
                },
                **_AGY_COMMON_OVERRIDE_PROPERTIES,
            },
            "required": ["prompt", "conversation_id"],
        },
        "outputSchema": _AGY_REPLY_OUTPUT_SCHEMA,
        "annotations": {
            "title": "Continue Antigravity Conversation",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True,
        },
    },
    {
        "name": "antigravity_status",
        "description": (
            "Get live status of async Antigravity tasks (tool calls, tool errors, "
            "warnings, cumulative token usage, and terminal failure details). "
            "This is a read-only peek: it collects nothing and does not keep a task "
            "attached to your session -- only antigravity_wait does that. A FAILED "
            "verdict here is not always authoritative when inferred without a "
            "terminal event or exit code (flagged explicitly in the output)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of task_ids to check status for.",
                },
            },
            "required": ["task_ids"],
        },
        "outputSchema": _AGY_STATUS_OUTPUT_SCHEMA,
        "annotations": {
            "title": "Check Async Antigravity Task Status",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    },
    {
        "name": "antigravity_wait",
        "description": (
            "Block until one or more async Antigravity tasks complete, then return "
            "all results. Pass every task_id you launched in ONE call. Let it run "
            "long past your client's backgrounding threshold (short timeouts are "
            "raised to a floor; finished tasks return instantly). A wait timeout "
            "does NOT kill tasks -- call antigravity_wait again to keep waiting."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of task_ids to wait for.",
                },
                "timeout": {
                    "type": "integer",
                    "description": (
                        "Max seconds to wait (default: 1800). Leave this alone "
                        "unless you have a specific reason; values below the "
                        "client backgrounding floor are raised to it. The task "
                        "keeps running even if this times out."
                    ),
                },
            },
            "required": ["task_ids"],
        },
        "outputSchema": _AGY_WAIT_OUTPUT_SCHEMA,
        "annotations": {
            "title": "Wait for Async Antigravity Tasks",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    },
    {
        "name": "antigravity_cancel",
        "description": (
            "Kill a running async Antigravity task and its child processes. Any "
            "worktree and partial output are preserved for inspection. If the task "
            "had already failed, the response includes its structured failure details."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "The task_id to cancel.",
                },
            },
            "required": ["task_id"],
        },
        "outputSchema": _AGY_CANCEL_OUTPUT_SCHEMA,
        "annotations": {
            "title": "Cancel Async Antigravity Task",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    },
]

RESOURCES = [
    {
        "uri": "antigravity-swarm:///server-info",
        "name": "Server Info",
        "description": "Server version, configuration, and capabilities.",
        "mimeType": "application/json",
    },
    {
        "uri": "antigravity-swarm:///config",
        "name": "Server Config",
        "description": "Current server-level Antigravity CLI defaults.",
        "mimeType": "application/json",
    },
    {
        "uri": "antigravity-swarm:///tasks",
        "name": "Active Tasks",
        "description": (
            "List of all known async tasks and their current state. Includes "
            "conversation_id, model, usage, tool errors, and failure details."
        ),
        "mimeType": "application/json",
    },
]


# ---------------------------------------------------------------------------
# Antigravity Backend adapter & server wiring
# ---------------------------------------------------------------------------
class AntigravityBackend(_core.SwarmBackend):
    server_name = "antigravity-mcp-swarm"
    version = __version__
    product_name = "Antigravity"
    tool_prefix = "antigravity"
    uri_scheme = "antigravity-swarm"
    branch_prefix = "antigravity-swarm"
    session_param_name = "conversation_id"
    session_field_name = "conversation_id"
    session_label = "Conversation ID"
    prefer_observed_session_id = True
    wants_stderr_on_success = True
    process_check_cmd = _PROCESS_CHECK_CMD
    server_instructions = SERVER_INSTRUCTIONS
    tools = TOOLS
    resources = RESOURCES
    reply_extra_param_keys = (
        "cwd",
        "model",
        "effort",
        "mode",
        "dangerously_skip_permissions",
        "sandbox",
        "agent",
        "add_dir",
        "json_schema",
        "settings",
        "disable_slash_commands",
        "print_timeout",
        "project",
        "remote_agent",
    )

    def validate_call_params(
        self, args: Dict[str, Any], *, is_reply: bool = False
    ) -> Optional[str]:
        return _validate_agy_params(args, is_reply=is_reply)

    def build_command(self, params: Dict[str, Any]) -> Tuple[List[str], Optional[str]]:
        return _build_command(params)

    def format_stdin_payload(
        self, params: Dict[str, Any]
    ) -> Optional[Union[str, bytes]]:
        return _format_stdin_payload(params)

    def exec_argv(self, cmd: List[str]) -> List[str]:
        return _exec_argv(cmd)

    def resolve_model(self, params: Optional[Dict[str, Any]] = None) -> Optional[str]:
        return _resolve_model(params)

    def model_from_command(self, command: Any) -> Optional[str]:
        return _model_from_command(command)

    def parse_jsonl_file(
        self, stdout_path: Path
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return _parse_jsonl_file(stdout_path)

    def parse_jsonl_text(
        self, jsonl_text: str
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return _parse_jsonl_full(jsonl_text)

    def invalidate_jsonl_cache(self, stdout_path: Path) -> None:
        _JSONL_CACHE_OBJ.invalidate(stdout_path)

    def extract_session_id_fast(
        self, stdout_path: Path, meta: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        return _extract_conversation_id_fast(stdout_path, meta)

    def extract_result(
        self,
        stdout: str,
        stderr: str,
        analysis: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, Optional[str]]:
        return _extract_agy_result(stdout, stderr, analysis=analysis)

    def validate_observed_session(
        self,
        requested_id: str,
        observed_id: Optional[str],
        analysis: Dict[str, Any],
        stderr: str,
    ) -> Optional[Dict[str, Any]]:
        req_norm = requested_id.strip().lower()
        obs_norm = observed_id.strip().lower() if isinstance(observed_id, str) else ""
        not_found_warn = (
            f'conversation "{requested_id.strip()}" not found'.lower()
            in (stderr or "").lower()
        )
        if not_found_warn or (obs_norm and obs_norm != req_norm):
            info: Dict[str, Any] = {
                "cause": (
                    f'Conversation "{requested_id.strip()}" was not found; agy ran '
                    f'the prompt in a fresh conversation ({observed_id or "unknown"}) '
                    "without prior context (any requested edits may already have been applied)."
                ),
                "model": analysis.get("model"),
                "category": "conversation_not_found",
                "retryable": False,
                "action": (
                    "Inspect the workspace for any edits from the fresh run, verify "
                    "the conversation_id from a previous Antigravity response, or call "
                    "antigravity without conversation_id to start a new conversation."
                ),
            }
            if stderr.strip():
                info["stderr_diagnostics"] = stderr.strip()[-500:]
            return info
        return None

    def resolve_reply_cwd(
        self, session_id: str, extra_params: Dict[str, Any]
    ) -> Optional[str]:
        return _lookup_session_cwd(session_id)

    def record_session_cwd(
        self, session_id: Optional[str], cwd: Optional[str]
    ) -> None:
        _record_session_cwd(session_id, cwd)

    def classify_failure(self, message: str) -> Dict[str, Any]:
        return _classify_agy_failure(message)

    def build_failure_info(
        self,
        stdout: str,
        stderr: str,
        command: Any = None,
        analysis: Optional[Dict[str, Any]] = None,
        model: Optional[str] = None,
    ) -> Dict[str, Any]:
        return _build_agy_failure_info(
            stdout, stderr, command=command, analysis=analysis, model=model
        )

    def get_server_info_extra(self) -> Dict[str, Any]:
        return {
            "cli_protocol": {
                "input_format": "stream-json",
                "output_format": "stream-json",
                "terminal_event": "result",
                "stderr_structured_error_prefix": "AGY_ERROR:",
                "wrapper_retries": False,
            },
        }

    def get_config_resource(self) -> Dict[str, Any]:
        return {"server_defaults": dict(SERVER_DEFAULTS)}

    def extra_meta_on_start(self, params: Dict[str, Any]) -> Dict[str, Any]:
        extra: Dict[str, Any] = {}
        effort = _resolve_effort(params)
        if effort:
            extra["effort"] = effort
        mode = _resolve_mode(params)
        if mode:
            extra["mode"] = mode
        return extra

    def extra_check_task_fields(
        self,
        analysis: Dict[str, Any],
        meta: Dict[str, Any],
        status: str,
        stderr: str = "",
    ) -> Dict[str, Any]:
        extra: Dict[str, Any] = {}
        for key in (
            "permission_mode",
            "finished_reason",
            "usage",
            "duration_seconds",
            "num_turns",
        ):
            if analysis.get(key) is not None:
                extra[key] = analysis[key]
        if analysis.get("has_structured_output"):
            extra["structured_output"] = analysis["structured_output"]
        if analysis.get("tool_errors"):
            extra["tool_errors"] = analysis["tool_errors"]
        cli_warnings = _extract_stderr_warnings(stderr)
        if cli_warnings:
            extra["warnings"] = cli_warnings
        if meta.get("effort"):
            extra["effort"] = meta["effort"]
        if meta.get("mode"):
            extra["mode"] = meta["mode"]
        return extra

    def extra_sync_outcome_fields(
        self,
        analysis: Dict[str, Any],
        outcome: Dict[str, Any],
        stderr: str = "",
    ) -> Dict[str, Any]:
        extra: Dict[str, Any] = {}
        for key in (
            "permission_mode",
            "finished_reason",
            "usage",
            "duration_seconds",
            "num_turns",
        ):
            if analysis.get(key) is not None:
                extra[key] = analysis[key]
        if analysis.get("has_structured_output"):
            extra["structured_output"] = analysis["structured_output"]
        if analysis.get("tool_errors"):
            extra["tool_errors"] = analysis["tool_errors"]
        cli_warnings = _extract_stderr_warnings(stderr)
        if cli_warnings:
            extra["warnings"] = cli_warnings
        return extra

    def format_extra_completion_details(
        self, info: Dict[str, Any], *, is_reply: bool = False
    ) -> List[str]:
        details: List[str] = []
        tool_errors = info.get("tool_errors")
        if tool_errors:
            details.append(_format_tool_errors_note(tool_errors))
        cli_warnings = info.get("warnings")
        if cli_warnings:
            details.append(_format_cli_warnings_note(cli_warnings))
        return details

    def format_extra_status_lines(
        self,
        analysis: Dict[str, Any],
        jsonl_status: Dict[str, Any],
        meta: Dict[str, Any],
        status: str,
    ) -> List[str]:
        lines: List[str] = []
        if analysis.get("permission_mode"):
            lines.append(f"Permission mode: {analysis['permission_mode']}")
        tool_errors = analysis.get("tool_errors") or []
        if tool_errors:
            last = tool_errors[-1]
            lines.append(
                f"Tool errors: {len(tool_errors)} "
                f"(last: {last.get('tool', 'tool')}: {last.get('message', '')[:160]})"
            )
        usage = analysis.get("usage")
        if isinstance(usage, dict) and usage.get("total_tokens"):
            lines.append(
                f"Tokens: {usage.get('total_tokens')} total "
                f"(in={usage.get('input_tokens', 0)}, out={usage.get('output_tokens', 0)}, "
                f"thinking={usage.get('thinking_tokens', 0)})"
            )
        return lines

    def extra_structured_status_fields(
        self,
        analysis: Dict[str, Any],
        jsonl_status: Dict[str, Any],
        meta: Dict[str, Any],
        status: str,
    ) -> Dict[str, Any]:
        return _core.clean_none({
            "permission_mode": analysis.get("permission_mode"),
            "finished_reason": analysis.get("finished_reason"),
            "usage": analysis.get("usage"),
            "tool_errors": (
                analysis["tool_errors"] if analysis.get("tool_errors") else None
            ),
            "effort": meta.get("effort"),
            "mode": meta.get("mode"),
        })

    def extra_structured_wait_fields(self, info: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key in (
            "permission_mode",
            "finished_reason",
            "duration_seconds",
            "num_turns",
            "usage",
            "tool_errors",
            "warnings",
            "effort",
            "mode",
        ):
            if info.get(key) is not None:
                out[key] = info[key]
        if "structured_output" in info and info["structured_output"] is not None:
            out["structured_output"] = info["structured_output"]
        return out

    def extra_structured_sync_fields(
        self, outcome: Dict[str, Any], *, is_reply: bool = False
    ) -> Dict[str, Any]:
        return self.extra_structured_wait_fields(outcome)

    def extra_resource_task_fields(
        self,
        resolved_meta: Dict[str, Any],
        analysis: Optional[Dict[str, Any]],
        state: Dict[str, Any],
    ) -> Dict[str, Any]:
        extra: Dict[str, Any] = {}
        for key in ("effort", "mode"):
            if resolved_meta.get(key):
                extra[key] = resolved_meta[key]
        if analysis:
            if analysis.get("usage"):
                extra["usage"] = analysis["usage"]
            if analysis.get("tool_errors"):
                extra["tool_errors"] = analysis["tool_errors"]
            if analysis.get("finished_reason"):
                extra["finished_reason"] = analysis["finished_reason"]
        return extra


def _set_last_cleanup(val: float) -> None:
    global _last_cleanup
    _last_cleanup = val


def _get_task_dir() -> Path:
    _core.ensure_private_dir(TASK_DIR)
    return TASK_DIR


def _get_worktree_dir() -> Path:
    _core.ensure_private_dir(WORKTREE_BASE_DIR)
    return WORKTREE_BASE_DIR


_HOOKS = _core.RuntimeHooks(
    task_dir=_get_task_dir,
    worktree_base_dir=_get_worktree_dir,
    branch_prefix="antigravity-swarm",
    task_max_age=lambda: _TASK_MAX_AGE,
    wait_min_timeout=lambda: _WAIT_MIN_TIMEOUT,
    client_auto_background_s=_CLIENT_AUTO_BACKGROUND_S,
    poll_interval=lambda: _POLL_INTERVAL,
    progress_interval=lambda: _PROGRESS_INTERVAL,
    log_file=lambda: LOG_FILE,
    log_level=lambda: LOG_LEVEL,
    is_windows_fn=lambda: _IS_WINDOWS,
    send_fn=lambda resp: _send(resp),
    get_last_cleanup_fn=lambda: _last_cleanup,
    set_last_cleanup_fn=_set_last_cleanup,
)

_BACKEND = AntigravityBackend()
_SERVER = _core.McpSwarmServer(_BACKEND, _HOOKS)


def _send(response: Dict[str, Any]) -> None:
    _SERVER.send(response)


def _handle(request: Dict[str, Any]) -> None:
    _SERVER.handle(request)


# ---------------------------------------------------------------------------
# Main CLI entrypoint
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="antigravity-mcp-swarm -- Parallel Antigravity (agy) MCP Server",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="Default Antigravity model (e.g. gemini-4-neon-high, gemini-3.8-flash-medium)",
    )
    parser.add_argument(
        "--effort",
        choices=list(_VALID_EFFORTS),
        default=None,
        help="Default reasoning effort level",
    )
    parser.add_argument(
        "--mode",
        choices=list(_VALID_MODES),
        default=None,
        help="Default execution mode (accept-edits, auto, plan)",
    )
    parser.add_argument(
        "--dangerously-skip-permissions",
        action="store_true",
        help="Auto-approve all tool permission requests by default",
    )
    parser.add_argument(
        "--sandbox",
        action="store_true",
        help="Run in sandbox mode with terminal restrictions enabled by default",
    )
    parser.add_argument(
        "--agent",
        default=None,
        help="Default Antigravity agent name",
    )
    parser.add_argument(
        "--add-dir",
        action="append",
        default=[],
        metavar="DIR",
        help="Directory to add to workspace by default (repeatable)",
    )
    parser.add_argument(
        "--settings",
        default=None,
        help="Default session-only settings JSON object or file path",
    )
    parser.add_argument(
        "--disable-slash-commands",
        action="store_true",
        help="Disable slash command and skill expansion in print mode by default",
    )
    parser.add_argument(
        "--print-timeout",
        default=None,
        help="Default print-mode time limit (e.g. '300s', '10m', '0s')",
    )
    parser.add_argument(
        "--project",
        default=None,
        help="Default project ID or name",
    )
    parser.add_argument(
        "--remote-agent",
        action="store_true",
        help="Run conversations using remote agents by default",
    )
    args = parser.parse_args(argv)

    SERVER_DEFAULTS["model"] = args.model
    SERVER_DEFAULTS["effort"] = args.effort
    SERVER_DEFAULTS["mode"] = args.mode
    SERVER_DEFAULTS["dangerously_skip_permissions"] = bool(
        args.dangerously_skip_permissions
    )
    SERVER_DEFAULTS["sandbox"] = bool(args.sandbox)
    SERVER_DEFAULTS["agent"] = args.agent
    SERVER_DEFAULTS["add_dir"] = list(args.add_dir or [])
    SERVER_DEFAULTS["settings"] = args.settings
    SERVER_DEFAULTS["disable_slash_commands"] = bool(args.disable_slash_commands)
    SERVER_DEFAULTS["print_timeout"] = args.print_timeout
    SERVER_DEFAULTS["project"] = args.project
    SERVER_DEFAULTS["remote_agent"] = bool(args.remote_agent)


def main() -> None:
    _parse_args()
    _configure_logging()
    _ensure_runtime_dirs()
    _SERVER.run_stdio_loop(
        on_start=lambda: logging.info(
            "Server starting -- defaults=%s", SERVER_DEFAULTS
        )
    )


if __name__ == "__main__":
    main()
