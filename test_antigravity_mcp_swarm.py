#!/usr/bin/env python3
"""Comprehensive tests for antigravity-mcp-swarm.

Runnable via ``python3 test_antigravity_mcp_swarm.py`` or ``pytest test_antigravity_mcp_swarm.py``.
Uses isolated temporary directories and fake Python subprocesses (no real API
calls) to exercise CLI flag construction, NDJSON stdin transport, stream-json
parsing, failed resume detection, worktree cwd continuity on reply, tool errors
& CLI warnings, AGY_ERROR / print_timeout handling, structured output &
cumulative usage, MCP 2026-07-28 + Tasks extension, git worktrees, process-group
cancellation, and strict outputSchema conformance across happy & error paths.
"""

import atexit
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

TEST_ROOT = tempfile.mkdtemp(prefix="antigravity_swarm_test_")
atexit.register(shutil.rmtree, TEST_ROOT, True)
os.environ["ANTIGRAVITY_SWARM_TASK_DIR"] = os.path.join(TEST_ROOT, "tasks")
os.environ["ANTIGRAVITY_SWARM_WORKTREE_DIR"] = os.path.join(TEST_ROOT, "worktrees")
os.environ["ANTIGRAVITY_SWARM_LOG"] = os.path.join(TEST_ROOT, "server.log")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import antigravity_mcp_swarm as m  # noqa: E402, I001


UUID_1 = "11111111-2222-4333-8444-555555555555"
UUID_2 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
UUID_MISSING = "00000000-0000-4000-8000-000000000000"


class Captured:
    """Swap out _send and collect every frame the server writes."""

    def __enter__(self):
        self.frames = []
        self._real = m._send
        lock = threading.Lock()

        def fake(resp):
            with lock:
                self.frames.append(resp)

        m._send = fake
        return self

    def __exit__(self, *args):
        m._send = self._real

    def responses(self):
        return [f for f in self.frames if "result" in f or "error" in f]

    def progress(self):
        return [f for f in self.frames if f.get("method") == "notifications/progress"]


_REQ_ID = 100


def _next_id():
    global _REQ_ID
    _REQ_ID += 1
    return _REQ_ID


def send_req(method, params=None, rid=None):
    if rid is None:
        rid = _next_id()
    req = {"jsonrpc": "2.0", "id": rid, "method": method}
    if params is not None:
        req["params"] = params
    with Captured() as cap:
        m._handle(req)
    return cap


def make_fake_agy_script(name, body):
    path = Path(TEST_ROOT) / name
    path.write_text(body, encoding="utf-8")
    return path


def with_fake_agy(script_path):
    class _Ctx:
        def __enter__(self):
            self._real = m._exec_argv

            def fake_exec(cmd):
                if cmd and cmd[0] == "agy":
                    return [sys.executable, str(script_path)] + cmd[1:]
                return cmd

            m._exec_argv = fake_exec
            return self

        def __exit__(self, *args):
            m._exec_argv = self._real

    return _Ctx()


def validate_against_schema(value, schema, path="$"):
    """Strict recursive JSON Schema subset validator for outputSchema checks."""
    if not schema:
        return  # unrestricted schema (e.g. structured_output)
    expected_type = schema.get("type")
    if expected_type == "object":
        assert isinstance(value, dict), f"{path}: expected object, got {type(value).__name__}"
        for req_key in schema.get("required", []):
            assert req_key in value, f"{path}: missing required key {req_key!r}"
        props = schema.get("properties", {})
        if props:
            for k, v in value.items():
                assert k in props, f"{path}: unexpected key {k!r} not in outputSchema properties"
                validate_against_schema(v, props[k], f"{path}.{k}")
    elif expected_type == "array":
        assert isinstance(value, list), f"{path}: expected array, got {type(value).__name__}"
        item_schema = schema.get("items")
        if item_schema:
            for idx, elem in enumerate(value):
                validate_against_schema(elem, item_schema, f"{path}[{idx}]")
    elif expected_type == "string":
        assert isinstance(value, str), f"{path}: expected string, got {type(value).__name__}"
        if "enum" in schema:
            assert value in schema["enum"], f"{path}: {value!r} not in {schema['enum']}"
    elif expected_type == "integer":
        assert isinstance(value, int) and not isinstance(value, bool), (
            f"{path}: expected integer, got {type(value).__name__}"
        )
    elif expected_type == "number":
        assert isinstance(value, (int, float)) and not isinstance(value, bool), (
            f"{path}: expected number, got {type(value).__name__}"
        )
    elif expected_type == "boolean":
        assert isinstance(value, bool), f"{path}: expected boolean, got {type(value).__name__}"


# ===================================================================
# 1. Command Builder, CLI Flags, Stdin NDJSON, and Launcher
# ===================================================================

def test_command_builder_flags_tristate_and_merging():
    saved = {k: (list(v) if isinstance(v, list) else v) for k, v in m.SERVER_DEFAULTS.items()}
    try:
        m._parse_args([
            "--model", "gemini-4-neon-high",
            "--effort", "xhigh",
            "--mode", "accept-edits",
            "--dangerously-skip-permissions",
            "--sandbox",
            "--agent", "reviewer",
            "--add-dir", "/dir/a",
            "--add-dir", "/dir/b",
            "--settings", '{"foo":1}',
            "--disable-slash-commands",
            "--print-timeout", "5m",
            "--project", "proj-1",
            "--remote-agent",
        ])

        cmd, cwd = m._build_command({"prompt": "hi"})
        assert cmd[:5] == ["agy", "--input-format", "stream-json", "--output-format", "stream-json"]
        assert "-p" not in cmd and "--print" not in cmd
        assert "--model=gemini-4-neon-high" in cmd
        assert "--effort=xhigh" in cmd
        assert "--mode=accept-edits" in cmd
        assert "--agent=reviewer" in cmd
        assert "--project=proj-1" in cmd
        assert "--print-timeout=5m" in cmd
        assert "--dangerously-skip-permissions" in cmd
        assert "--sandbox" in cmd
        assert "--disable-slash-commands" in cmd
        assert "--remote-agent" in cmd
        assert "--add-dir=/dir/a" in cmd and "--add-dir=/dir/b" in cmd
        assert '--settings={"foo":1}' in cmd
        assert cwd is None
        assert m._model_from_command(cmd) == "gemini-4-neon-high"

        cmd2, cwd2 = m._build_command({
            "prompt": "override",
            "conversation_id": UUID_1,
            "cwd": TEST_ROOT,
            "model": "claude-opus-5-5-high",
            "effort": "low",
            "mode": "plan",
            "dangerously_skip_permissions": False,
            "sandbox": False,
            "disable_slash_commands": False,
            "remote_agent": False,
            "add_dir": ["/dir/b", "/dir/c"],
            "settings": {"theme": "dark"},
            "json_schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}},
            "print_timeout": "30s",
        })
        assert f"--conversation={UUID_1}" in cmd2
        assert "--model=claude-opus-5-5-high" in cmd2
        assert "--effort=low" in cmd2
        assert "--mode=plan" in cmd2
        assert "--print-timeout=30s" in cmd2
        assert "--dangerously-skip-permissions" not in cmd2
        assert "--sandbox" not in cmd2
        assert "--disable-slash-commands" not in cmd2
        assert "--remote-agent" not in cmd2
        assert cmd2.count("--add-dir=/dir/b") == 1
        assert "--add-dir=/dir/a" in cmd2 and "--add-dir=/dir/c" in cmd2
        assert '--settings={"theme":"dark"}' in cmd2
        assert '--json-schema={"type":"object","properties":{"ok":{"type":"boolean"}}}' in cmd2
        assert cwd2 == os.path.abspath(TEST_ROOT)
        assert m._model_from_command(cmd2) == "claude-opus-5-5-high"
    finally:
        m.SERVER_DEFAULTS.clear()
        m.SERVER_DEFAULTS.update(saved)


def test_stdin_ndjson_payload_and_launcher_override():
    prompt = "-model fake\nline 2\u2028café 日本語"
    payload = m._format_stdin_payload({"prompt": prompt})
    assert payload.endswith("\n")
    assert payload.count("\n") == 1
    # Pure ASCII ensures U+2028/U+2029 are escaped safely
    payload.encode("ascii")
    parsed = json.loads(payload)
    assert parsed == {"event": "user", "message": {"content": prompt}}
    assert m._format_stdin_payload({"prompt": ""}) is None

    saved_env = os.environ.get("ANTIGRAVITY_SWARM_BIN")
    saved_win = m._IS_WINDOWS
    try:
        os.environ["ANTIGRAVITY_SWARM_BIN"] = "/custom/bin/agy"
        m._IS_WINDOWS = False
        assert m._agy_launcher() == [os.path.abspath("/custom/bin/agy")]

        # Bare name must NOT be blindly resolved against cwd
        os.environ["ANTIGRAVITY_SWARM_BIN"] = "nonexistent_bare_agy_cmd_xyz"
        assert m._agy_launcher() == ["nonexistent_bare_agy_cmd_xyz"]

        os.environ["ANTIGRAVITY_SWARM_BIN"] = "C:\\tools\\agy.cmd"
        m._IS_WINDOWS = True
        try:
            m._agy_launcher()
            raise AssertionError("expected RuntimeError for Windows .cmd override")
        except RuntimeError as exc:
            assert ".exe" in str(exc)
    finally:
        if saved_env is None:
            os.environ.pop("ANTIGRAVITY_SWARM_BIN", None)
        else:
            os.environ["ANTIGRAVITY_SWARM_BIN"] = saved_env
        m._IS_WINDOWS = saved_win


# ===================================================================
# 2. Stream-JSON Parsing, Cumulative Usage, Tool Errors, Warnings, No Raw Leak
# ===================================================================

def test_stream_json_deltas_cumulative_usage_tool_dedup_and_no_raw_ndjson_leak():
    # Live run WITHOUT final result yet: step usages must SUM across step_index values!
    live_events = [
        {
            "event": "init",
            "conversation_id": UUID_1,
            "init": {"model": "gemini-4-neon-high", "permission_mode": "always-proceed"},
        },
        {
            "event": "step_update",
            "step_update": {
                "conversation_id": UUID_1,
                "step_index": 1,
                "state": "DONE",
                "step_type": "agent_response",
                "usage": {"input_tokens": 9000, "output_tokens": 100, "thinking_tokens": 50, "cache_read_tokens": 0, "total_tokens": 9100},
            },
        },
        {
            "event": "step_update",
            "step_update": {
                "conversation_id": UUID_1,
                "step_index": 2,
                "state": "ACTIVE",
                "step_type": "tool",
                "tool_name": "run_command",
                "tool_info": {"name": "run_command", "parameters": {"CommandLine": "pytest -q"}},
            },
        },
        {
            "event": "step_update",
            "step_update": {
                "conversation_id": UUID_1,
                "step_index": 2,
                "state": "DONE",
                "step_type": "tool",
                "tool_name": "run_command",
                "tool_info": {"name": "run_command", "parameters": {"CommandLine": "pytest -q"}, "output": "10 passed"},
            },
        },
        {
            "event": "step_update",
            "step_update": {
                "conversation_id": UUID_1,
                "step_index": 3,
                "state": "ACTIVE",
                "step_type": "agent_response",
                "text_delta": '{"ok":',
            },
        },
        {
            "event": "step_update",
            "step_update": {
                "conversation_id": UUID_1,
                "step_index": 3,
                "state": "DONE",
                "step_type": "agent_response",
                "text_delta": " true}\n",
                "usage": {"input_tokens": 2000, "output_tokens": 200, "thinking_tokens": 30, "cache_read_tokens": 7000, "total_tokens": 2200},
            },
        },
    ]
    live_jsonl = "\n".join(json.dumps(e) for e in live_events) + "\n"
    a_live, s_live = m._parse_jsonl_full(live_jsonl)
    assert a_live["usage"]["total_tokens"] == 11300  # 9100 + 2200 cumulative!
    assert a_live["usage"]["thinking_tokens"] == 80
    assert s_live["tools_called"] == 1  # step 2 ACTIVE+DONE deduplicated
    assert s_live["last_reasoning"] is None  # tool output NOT mislabelled as Thinking!
    assert a_live["last_assistant_text"] == '{"ok": true}\n'

    # Now append final result -> authoritative result.usage + structured_output
    final_ev = {
        "event": "result",
        "result": {
            "conversation_id": UUID_1,
            "status": "SUCCESS",
            "response": '{"ok": true}\n',
            "structured_output": {"ok": True},
            "duration_seconds": 3.4567,
            "num_turns": 1,
            "usage": {"input_tokens": 11000, "output_tokens": 300, "thinking_tokens": 80, "cache_read_tokens": 7000, "total_tokens": 11300},
        },
    }
    a_done, _ = m._parse_jsonl_full(live_jsonl + json.dumps(final_ev) + "\n")
    assert a_done["terminal_status"] == "completed"
    assert a_done["structured_output"] == {"ok": True}
    assert a_done["duration_seconds"] == 3.457

    # When saw_json is True and there is NO assistant text and NO structured_output,
    # _extract_agy_result MUST NOT leak raw NDJSON!
    raw_only_events = json.dumps({"event": "init", "conversation_id": UUID_1}) + "\n"
    txt, cid = m._extract_agy_result(raw_only_events, "")
    assert cid == UUID_1
    assert txt == "" and '"event"' not in txt

    # Empty response with structured_output extracts JSON text cleanly
    empty_resp_jsonl = json.dumps({
        "event": "result",
        "result": {
            "conversation_id": UUID_1,
            "status": "SUCCESS",
            "response": "",
            "structured_output": {"answer": 42},
        },
    }) + "\n"
    txt2, cid2 = m._extract_agy_result(empty_resp_jsonl, "")
    assert cid2 == UUID_1 and json.loads(txt2) == {"answer": 42}


def test_tool_errors_neutral_stderr_warnings_and_print_timeout():
    tools_by_name = {t["name"]: t for t in m.TOOLS}

    # 1. Plain OS EACCES tool error + stderr warning on SUCCESS:
    #    MUST report neutrally under tool_errors + warnings and MUST NOT tell caller
    #    to pass dangerously_skip_permissions=true!
    tool_err_script = make_fake_agy_script(
        "fake_agy_tool_err.py",
        f"""
import json, sys
_ = sys.stdin.read()
print(json.dumps({{"event": "init", "conversation_id": {UUID_1!r}, "init": {{"model": "gemini-3.8-flash-low", "permission_mode": "request-review"}}}}))
print(json.dumps({{
    "event": "step_update",
    "step_update": {{
        "conversation_id": {UUID_1!r},
        "step_index": 2,
        "state": "ERROR",
        "step_type": "tool",
        "tool_name": "write_file",
        "tool_info": {{
            "name": "write_file",
            "parameters": {{"Path": "/etc/test.txt"}},
            "error": {{"type": "TOOL_ERROR", "message": "open /etc/test.txt: permission denied"}}
        }}
    }}
}}))
print(json.dumps({{
    "event": "result",
    "result": {{
        "conversation_id": {UUID_1!r},
        "status": "SUCCESS",
        "response": "Could not write /etc/test.txt.",
        "duration_seconds": 1.2,
        "num_turns": 1,
        "usage": {{"input_tokens": 100, "output_tokens": 20, "thinking_tokens": 0, "cache_read_tokens": 0, "total_tokens": 120}}
    }}
}}))
sys.stderr.write("warning: --mode auto is not available for this account; continuing in the default mode.\\n")
""",
    )
    with with_fake_agy(tool_err_script):
        res = send_req(
            "tools/call",
            {"name": "antigravity", "arguments": {"prompt": "write protected file", "mode": "auto"}},
        ).responses()[0]["result"]
    assert res["isError"] is False
    sc = res["structuredContent"]
    validate_against_schema(sc, tools_by_name["antigravity"]["outputSchema"])
    assert len(sc["tool_errors"]) == 1
    assert sc["tool_errors"][0]["tool"] == "write_file"
    assert sc["tool_errors"][0]["type"] == "TOOL_ERROR"
    assert sc["warnings"] == ["--mode auto is not available for this account; continuing in the default mode."]
    body = res["content"][0]["text"]
    assert "Tool errors observed during turn (1):" in body
    assert "CLI warnings (1):" in body
    # Critical H3 invariant: plain OS EACCES must NEVER prompt caller to disable permissions!
    assert "dangerously_skip_permissions=true" not in body

    # 2. print_timeout -> status=failed, category=print_timeout, retryable=False, partial_output preserved
    timeout_script = make_fake_agy_script(
        "fake_agy_print_timeout.py",
        f"""
import json, sys
_ = sys.stdin.read()
print(json.dumps({{"event": "init", "conversation_id": {UUID_1!r}, "init": {{"model": "gemini-3.8-flash-low", "permission_mode": "request-review"}}}}))
print(json.dumps({{"event": "step_update", "step_update": {{"conversation_id": {UUID_1!r}, "step_index": 1, "state": "ACTIVE", "step_type": "agent_response", "text_delta": "Partial work before timeout"}}}}))
print(json.dumps({{"event": "result", "result": {{"conversation_id": {UUID_1!r}, "status": "SUCCESS", "response": "", "duration_seconds": 0, "num_turns": 1, "finished_reason": "print_timeout"}}}}))
sys.stderr.write('[agy] print timeout after 1ms\\nAGY_ERROR: {{"short_error":"print timeout after 1ms with the turn still in progress; the response is partial","status":"DEADLINE_EXCEEDED","retryable":false,"reason":"print_timeout"}}\\n')
sys.exit(0)
""",
    )
    with with_fake_agy(timeout_script):
        res_to = send_req(
            "tools/call",
            {"name": "antigravity", "arguments": {"prompt": "slow", "print_timeout": "1ms"}},
        ).responses()[0]["result"]
    assert res_to["isError"] is True
    sc_to = res_to["structuredContent"]
    validate_against_schema(sc_to, tools_by_name["antigravity"]["outputSchema"])
    assert sc_to["status"] == "failed"
    assert sc_to["finished_reason"] == "print_timeout"
    assert sc_to["failure"]["category"] == "print_timeout"
    assert sc_to["failure"]["retryable"] is False
    assert "print timeout after 1ms" in sc_to["failure"]["cause"]
    assert sc_to["partial_output"] == "Partial work before timeout"

    # Generic DEADLINE_EXCEEDED without print_timeout reason must NOT classify as print_timeout
    c_generic = m._classify_agy_failure(
        "upstream rpc timed out", agy_err={"status": "DEADLINE_EXCEEDED", "reason": "upstream"}
    )
    assert c_generic["category"] == "transport"

    # Slash-command stream-json rejection gives clear disable_slash_commands hint
    c_slash = m._classify_agy_failure(
        "/help is answered by the CLI itself and is unavailable with --input-format stream-json"
    )
    assert c_slash["category"] == "invalid_request"
    assert c_slash["retryable"] is False
    assert "disable_slash_commands=true" in c_slash["action"]


def test_failed_resume_detected_as_error_sync_and_async():
    """H1: when agy ignores a missing --conversation UUID and falls back to a
    fresh conversation, both antigravity_reply and antigravity_async must report
    status=failed with category=conversation_not_found and isError=True!
    """
    tools_by_name = {t["name"]: t for t in m.TOOLS}
    fallback_script = make_fake_agy_script(
        "fake_agy_resume_fallback.py",
        f"""
import json, sys
_ = sys.stdin.read()
print(json.dumps({{"event": "init", "conversation_id": {UUID_2!r}, "init": {{"model": "gemini-3.8-flash-low"}}}}))
print(json.dumps({{"event": "result", "result": {{"conversation_id": {UUID_2!r}, "status": "SUCCESS", "response": "Fresh unrelated answer"}}}}))
sys.stderr.write('warning: conversation "{UUID_MISSING}" not found\\n')
""",
    )
    with with_fake_agy(fallback_script):
        # 1. Sync antigravity_reply
        r_sync = send_req(
            "tools/call",
            {"name": "antigravity_reply", "arguments": {"conversation_id": UUID_MISSING, "prompt": "continue"}},
        ).responses()[0]["result"]
        assert r_sync["isError"] is True
        sc = r_sync["structuredContent"]
        validate_against_schema(sc, tools_by_name["antigravity_reply"]["outputSchema"])
        assert sc["status"] == "failed"
        assert sc["failure"]["category"] == "conversation_not_found"
        assert sc["failure"]["retryable"] is False

        # 2. Async antigravity_async + wait + status
        r_async = send_req(
            "tools/call",
            {"name": "antigravity_async", "arguments": {"conversation_id": UUID_MISSING, "prompt": "continue"}},
        ).responses()[0]["result"]
        tid = r_async["structuredContent"]["task_id"]

        r_wait = send_req(
            "tools/call",
            {"name": "antigravity_wait", "arguments": {"task_ids": [tid], "timeout": 10}},
        ).responses()[0]["result"]
        assert r_wait["isError"] is True
        validate_against_schema(r_wait["structuredContent"], tools_by_name["antigravity_wait"]["outputSchema"])
        wt = r_wait["structuredContent"]["tasks"][0]
        assert wt["status"] == "failed"
        assert wt["failure_category"] == "conversation_not_found"

        r_st = send_req(
            "tools/call",
            {"name": "antigravity_status", "arguments": {"task_ids": [tid]}},
        ).responses()[0]["result"]
        validate_against_schema(r_st["structuredContent"], tools_by_name["antigravity_status"]["outputSchema"])
        assert r_st["structuredContent"]["tasks"][0]["failure"]["category"] == "conversation_not_found"


def test_reply_preserves_worktree_cwd_automatically():
    """H2: a follow-up antigravity_reply after a worktree=True call must stay
    inside the worktree directory even when cwd is omitted, AND the completion
    hint must include cwd="<worktree_path>".
    """
    if sys.platform == "win32":
        return
    repo = Path(TEST_ROOT) / "agy_wt_reply_repo"
    repo.mkdir()
    git_env = dict(os.environ)
    git_env["GIT_AUTHOR_NAME"] = "Test"
    git_env["GIT_AUTHOR_EMAIL"] = "t@example.com"
    git_env["GIT_COMMITTER_NAME"] = "Test"
    git_env["GIT_COMMITTER_EMAIL"] = "t@example.com"
    subprocess.run(["git", "init", "-q", str(repo)], check=True, env=git_env)
    (repo / "committed.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "committed.txt"], check=True, env=git_env)
    subprocess.run(
        ["git", "-C", str(repo), "-c", "commit.gpgsign=false", "commit", "-q", "-m", "init"],
        check=True,
        env=git_env,
    )
    (repo / "committed.txt").write_text("dirty\n", encoding="utf-8")
    (repo / "untracked.txt").write_text("untracked\n", encoding="utf-8")

    wt_probe = make_fake_agy_script(
        "fake_agy_wt_reply.py",
        f"""
import json, os, sys
from pathlib import Path
_ = sys.stdin.read()
cwd = Path(os.getcwd())
info = {{
    "cwd": os.path.realpath(str(cwd)),
    "committed": (cwd / "committed.txt").read_text().strip() if (cwd / "committed.txt").exists() else None,
    "has_untracked": (cwd / "untracked.txt").exists(),
}}
print(json.dumps({{"event": "init", "conversation_id": {UUID_1!r}}}))
print(json.dumps({{"event": "result", "result": {{"conversation_id": {UUID_1!r}, "status": "SUCCESS", "response": json.dumps(info)}}}}))
""",
    )
    with with_fake_agy(wt_probe):
        res1 = send_req(
            "tools/call",
            {"name": "antigravity", "arguments": {"prompt": "first", "cwd": str(repo), "worktree": True}},
        ).responses()[0]["result"]
        assert res1["isError"] is False
        sc1 = res1["structuredContent"]
        wt_path = sc1["worktree_path"]
        assert sc1["worktree_branch"].startswith("antigravity-swarm/")
        assert f'cwd="{wt_path}"' in res1["content"][0]["text"]
        p1 = json.loads(res1["content"][0]["text"].split("\n\n---\n")[0])
        assert p1["committed"] == "base" and p1["has_untracked"] is False

        # Follow-up antigravity_reply WITHOUT explicit cwd MUST stay in wt_path!
        res2 = send_req(
            "tools/call",
            {"name": "antigravity_reply", "arguments": {"conversation_id": UUID_1, "prompt": "follow-up"}},
        ).responses()[0]["result"]
        assert res2["isError"] is False
        p2 = json.loads(res2["content"][0]["text"])
        assert p2["cwd"] == os.path.realpath(wt_path), f"reply left worktree! got {p2['cwd']} vs {wt_path}"

    assert m._HOOKS.remove_worktree(wt_path, sc1["worktree_branch"]) is True


def test_all_six_tools_happy_and_error_paths_schema_and_tasks_extension():
    tools = send_req("tools/list").responses()[0]["result"]["tools"]
    by_name = {t["name"]: t for t in tools}
    assert set(by_name) == {
        "antigravity",
        "antigravity_async",
        "antigravity_reply",
        "antigravity_status",
        "antigravity_wait",
        "antigravity_cancel",
    }

    echo_script = make_fake_agy_script(
        "fake_agy_echo_all.py",
        f"""
import json, os, sys
line = sys.stdin.readline()
msg = json.loads(line)
prompt = msg["message"]["content"]
cid = {UUID_1!r}
for a in sys.argv[1:]:
    if a.startswith("--conversation="):
        cid = a.split("=", 1)[1]
print(json.dumps({{"event": "init", "conversation_id": cid, "init": {{"model": "gemini-4-neon-high", "permission_mode": "always-proceed"}}}}), flush=True)
print(json.dumps({{"event": "step_update", "step_update": {{"conversation_id": cid, "step_index": 1, "state": "ACTIVE", "step_type": "tool", "tool_name": "read_file", "tool_info": {{"name": "read_file", "parameters": {{"Path": "README.md"}}}}}}}}), flush=True)
print(json.dumps({{"event": "result", "result": {{"conversation_id": cid, "status": "SUCCESS", "response": "OK:" + prompt, "structured_output": {{"echo": prompt[:20]}}, "duration_seconds": 0.42, "num_turns": 1, "usage": {{"input_tokens": 500, "output_tokens": 50, "thinking_tokens": 5, "cache_read_tokens": 100, "total_tokens": 550}}}}}}), flush=True)
""",
    )
    fail_script = make_fake_agy_script(
        "fake_agy_fail_all.py",
        """
import json, sys
_ = sys.stdin.read()
print(json.dumps({"event": "result", "result": {"conversation_id": "", "status": "ERROR", "response": "", "error": 'invalid model selection (--model "bogus"): not recognized as a known model', "finished_reason": "error"}}), flush=True)
sys.exit(1)
""",
    )
    sleep_script = make_fake_agy_script(
        "fake_agy_sleep_all.py",
        "import sys, time\n_ = sys.stdin.read()\ntime.sleep(60)\n",
    )

    with with_fake_agy(echo_script):
        # 1. Sync success
        r_sync = send_req(
            "tools/call",
            {"name": "antigravity", "arguments": {"prompt": "hello", "model": "gemini-4-neon-high"}},
        ).responses()[0]["result"]
        assert r_sync["isError"] is False
        validate_against_schema(r_sync["structuredContent"], by_name["antigravity"]["outputSchema"])

        # 2. Reply success
        r_rep = send_req(
            "tools/call",
            {"name": "antigravity_reply", "arguments": {"conversation_id": UUID_1, "prompt": "more"}},
        ).responses()[0]["result"]
        assert r_rep["isError"] is False
        validate_against_schema(r_rep["structuredContent"], by_name["antigravity_reply"]["outputSchema"])

        # 3. Async -> Wait -> Status -> Cancel(already completed)
        r_a = send_req(
            "tools/call",
            {"name": "antigravity_async", "arguments": {"prompt": "fan", "effort": "high"}},
        ).responses()[0]["result"]
        validate_against_schema(r_a["structuredContent"], by_name["antigravity_async"]["outputSchema"])
        tid = r_a["structuredContent"]["task_id"]

        r_w = send_req(
            "tools/call",
            {"name": "antigravity_wait", "arguments": {"task_ids": [tid], "timeout": 10}},
        ).responses()[0]["result"]
        validate_against_schema(r_w["structuredContent"], by_name["antigravity_wait"]["outputSchema"])

        r_s = send_req(
            "tools/call",
            {"name": "antigravity_status", "arguments": {"task_ids": [tid]}},
        ).responses()[0]["result"]
        validate_against_schema(r_s["structuredContent"], by_name["antigravity_status"]["outputSchema"])

        r_c = send_req(
            "tools/call",
            {"name": "antigravity_cancel", "arguments": {"task_id": tid}},
        ).responses()[0]["result"]
        validate_against_schema(r_c["structuredContent"], by_name["antigravity_cancel"]["outputSchema"])

    # 4. Non-happy paths: running status, wait timeout, cancel running, not_found, launch error, failed MCP task
    tasks_meta = {
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientCapabilities": {
                "extensions": {"io.modelcontextprotocol/tasks": {}},
            },
        }
    }
    saved_floor = m._WAIT_MIN_TIMEOUT
    m._WAIT_MIN_TIMEOUT = 1
    try:
        with with_fake_agy(sleep_script):
            r_slow = send_req(
                "tools/call", {"name": "antigravity_async", "arguments": {"prompt": "slow"}}
            ).responses()[0]["result"]
            slow_tid = r_slow["structuredContent"]["task_id"]

            r_st_run = send_req(
                "tools/call", {"name": "antigravity_status", "arguments": {"task_ids": [slow_tid]}}
            ).responses()[0]["result"]
            assert r_st_run["isError"] is False
            validate_against_schema(r_st_run["structuredContent"], by_name["antigravity_status"]["outputSchema"])
            assert r_st_run["structuredContent"]["tasks"][0]["status"] == "running"

            r_w_to = send_req(
                "tools/call", {"name": "antigravity_wait", "arguments": {"task_ids": [slow_tid], "timeout": 1}}
            ).responses()[0]["result"]
            assert r_w_to["isError"] is False
            validate_against_schema(r_w_to["structuredContent"], by_name["antigravity_wait"]["outputSchema"])
            assert r_w_to["structuredContent"]["tasks"][0]["status"] == "timeout"

            r_c_run = send_req(
                "tools/call", {"name": "antigravity_cancel", "arguments": {"task_id": slow_tid}}
            ).responses()[0]["result"]
            assert r_c_run["isError"] is False
            validate_against_schema(r_c_run["structuredContent"], by_name["antigravity_cancel"]["outputSchema"])
            assert r_c_run["structuredContent"]["status"] == "cancelled"
    finally:
        m._WAIT_MIN_TIMEOUT = saved_floor

    # Failed MCP Task (SEP-2663: status="completed", result.isError=True)
    with with_fake_agy(fail_script):
        t_create = send_req(
            "tools/call",
            {"name": "antigravity", "arguments": {"prompt": "fail", "model": "bogus"}, **tasks_meta},
        ).responses()[0]["result"]
        assert t_create["resultType"] == "task"
        ftid = t_create["taskId"]
        final_poll = None
        for _ in range(60):
            poll = send_req("tasks/get", {"taskId": ftid, **tasks_meta}).responses()[0]["result"]
            if poll["status"] != "working":
                final_poll = poll
                break
            time.sleep(0.05)
        assert final_poll is not None and final_poll["status"] == "completed"
        assert final_poll["result"]["isError"] is True
        validate_against_schema(
            final_poll["result"]["structuredContent"],
            by_name["antigravity"]["outputSchema"],
        )

    # Launch error on antigravity_async
    real_exec = m._exec_argv
    m._exec_argv = lambda cmd: ["/nonexistent_agy_bin_12345"]
    try:
        r_err = send_req(
            "tools/call", {"name": "antigravity_async", "arguments": {"prompt": "boom"}}
        ).responses()[0]["result"]
        assert r_err["isError"] is True
        validate_against_schema(r_err["structuredContent"], by_name["antigravity_async"]["outputSchema"])
        assert r_err["structuredContent"]["status"] == "error"
    finally:
        m._exec_argv = real_exec

    # Not-found task on status & cancel
    r_nf_c = send_req(
        "tools/call", {"name": "antigravity_cancel", "arguments": {"task_id": "deadbeef"}}
    ).responses()[0]["result"]
    assert r_nf_c["isError"] is True
    validate_against_schema(r_nf_c["structuredContent"], by_name["antigravity_cancel"]["outputSchema"])


def test_input_validation_errors():
    bad_cases = [
        {"prompt": ""},
        {"prompt": "hi", "effort": "extreme"},
        {"prompt": "hi", "mode": "yolo"},
        {"prompt": "hi", "dangerously_skip_permissions": "yes"},
        {"prompt": "hi", "sandbox": 1},
        {"prompt": "hi", "disable_slash_commands": "no"},
        {"prompt": "hi", "remote_agent": "true"},
        {"prompt": "hi", "worktree": "yes"},
        {"prompt": "hi", "add_dir": "/not/a/list"},
        {"prompt": "hi", "add_dir": [""]},
        {"prompt": "hi", "json_schema": 123},
        {"prompt": "hi", "settings": []},
        {"prompt": "hi", "conversation_id": ""},
        {"prompt": "hi", "conversation_id": "deadbeef"},  # 8-char task_id, NOT a UUID!
        {"prompt": "hi", "conversation_id": "-leading-dash"},
        {"prompt": "hi", "model": 42},
    ]
    for bad in bad_cases:
        err = send_req(
            "tools/call", {"name": "antigravity", "arguments": bad}
        ).responses()[0]["error"]
        assert err["code"] == -32602, f"expected -32602 for {bad!r}, got {err}"

    # antigravity_reply also rejects non-UUID conversation_id
    err_rep = send_req(
        "tools/call",
        {"name": "antigravity_reply", "arguments": {"conversation_id": "deadbeef", "prompt": "hi"}},
    ).responses()[0]["error"]
    assert err_rep["code"] == -32602


def test_stdio_end_to_end_and_version_flag():
    server_py = Path(__file__).resolve().parent / "antigravity_mcp_swarm.py"
    env = dict(os.environ)
    env["ANTIGRAVITY_SWARM_TASK_DIR"] = os.path.join(TEST_ROOT, "e2e_tasks")
    env["ANTIGRAVITY_SWARM_WORKTREE_DIR"] = os.path.join(TEST_ROOT, "e2e_worktrees")
    env["ANTIGRAVITY_SWARM_LOG"] = os.path.join(TEST_ROOT, "e2e_server.log")

    # --version exits 0 and prints version without creating task/log files
    ver_proc = subprocess.run(
        [sys.executable, str(server_py), "--version"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        timeout=10,
        check=False,
    )
    assert ver_proc.returncode == 0
    assert m.__version__ in ver_proc.stdout
    assert not Path(env["ANTIGRAVITY_SWARM_TASK_DIR"]).exists()

    requests = [
        {"jsonrpc": "2.0", "id": 1, "method": "server/discover", "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"}}},
        {"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": {"protocolVersion": "2025-11-25", "capabilities": {}}},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
        {"jsonrpc": "2.0", "method": "notifications/unknown"},
        {"jsonrpc": "2.0", "id": 3, "method": "tools/list", "params": {"_meta": {"io.modelcontextprotocol/protocolVersion": "2026-07-28"}}},
        {"jsonrpc": "2.0", "id": 4, "method": "resources/read", "params": {"uri": "antigravity-swarm:///config"}},
    ]
    proc = subprocess.run(
        [
            sys.executable,
            str(server_py),
            "--model",
            "gemini-4-neon-high",
            "--effort",
            "high",
            "--dangerously-skip-permissions",
        ],
        input="\n".join(json.dumps(r) for r in requests) + "\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        timeout=15,
        check=False,
    )
    assert proc.returncode == 0, f"server exited {proc.returncode}: {proc.stderr}"
    frames = [json.loads(line) for line in proc.stdout.splitlines() if line.strip()]
    assert [f["id"] for f in frames] == [1, 2, 3, 4]
    assert frames[0]["result"]["serverInfo"]["name"] == "antigravity-mcp-swarm"
    assert len(frames[2]["result"]["tools"]) == 6
    cfg = json.loads(frames[3]["result"]["contents"][0]["text"])
    assert cfg["server_defaults"]["model"] == "gemini-4-neon-high"
    assert cfg["server_defaults"]["effort"] == "high"
    assert cfg["server_defaults"]["dangerously_skip_permissions"] is True

    # Explicit /tmp log override must succeed without failing on root-owned /tmp
    if sys.platform != "win32" and Path("/tmp").exists():
        env_tmp_log = dict(env)
        tmp_log = f"/tmp/agy_swarm_override_test_{os.getpid()}.log"
        env_tmp_log["ANTIGRAVITY_SWARM_LOG"] = tmp_log
        try:
            p2 = subprocess.run(
                [sys.executable, str(server_py)],
                input='{"jsonrpc":"2.0","id":1,"method":"ping"}\n',
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env_tmp_log,
                timeout=15,
                check=False,
            )
            assert p2.returncode == 0, f"failed with /tmp log override: {p2.stderr}"
            assert json.loads(p2.stdout.strip())["result"] == {}
        finally:
            Path(tmp_log).unlink(missing_ok=True)


def _run_all():
    tests = [
        fn for name, fn in sorted(globals().items())
        if name.startswith("test_") and callable(fn)
    ]
    passed, failed = 0, []
    for fn in tests:
        try:
            fn()
            print(f"  PASS  {fn.__name__}")
            passed += 1
        except Exception as exc:
            print(f"  FAIL  {fn.__name__}: {exc}")
            failed.append((fn.__name__, exc))
    print(f"\n{passed} passed, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run_all())
