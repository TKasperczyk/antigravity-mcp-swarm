# antigravity-mcp-swarm

An MCP server that wraps Google's Antigravity CLI (`agy`) with **true parallel execution**, **live task monitoring**, **git worktree isolation**, and **MCP Tasks** support. Powered by stdlib-only [`mcp-swarm-core`](https://github.com/TKasperczyk/mcp-swarm-core) (zero third-party runtime dependencies).

## Why?

Run multiple Antigravity (`agy`) agents concurrently from Claude Code or any MCP client without serial queueing, with live visibility into tool calls, cumulative token usage, structured output, tool errors / CLI warnings, and isolated `HEAD`-based git worktrees.

**Key features:**

- **True parallel fan-out** -- each task runs as an independent `agy --input-format stream-json --output-format stream-json` subprocess
- **Worktree isolation** -- `worktree: true` creates an isolated `HEAD`-based git worktree (`antigravity-swarm/<id>`) per task so parallel runs never edit past each other
- **Batch wait & MCP Tasks** -- supports both explicit fan-out (`antigravity_async` + `antigravity_wait`) and the official MCP 2026-07-28 Tasks extension (`io.modelcontextprotocol/tasks`)
- **Live JSONL streaming status** -- parses `init`, `step_update`, and `result` NDJSON events incrementally for live tool counts, last tool call, cumulative token usage, tool errors, and CLI warnings
- **Non-interactive `agy` flag support** -- `--model`, `--effort`, `--mode`, `--dangerously-skip-permissions`, `--sandbox`, `--agent`, `--add-dir`, `--json-schema`, `--settings`, `--disable-slash-commands`, `--print-timeout`, `--project`, `--remote-agent`, and `--conversation` resume (interactive-only flags like `--continue`, `--remote-control`, `--new-project`, and `--log-file` are omitted)
- **Safe NDJSON stdin transport** -- prompts are sent as pure-ASCII single-line NDJSON `{"event":"user","message":{"content":...}}` over stdin, avoiding OS argv length limits, leading-`-` option injection, and Windows shell quoting issues

## MCP Protocol Support

- **Stateless MCP `2026-07-28`** -- supports `server/discover`, per-request `_meta["io.modelcontextprotocol/protocolVersion"]` and `_meta["io.modelcontextprotocol/clientCapabilities"]`, `resultType` discriminators, and unsupported-version rejection (`-32022`).
- **Official Tasks extension (`io.modelcontextprotocol/tasks`)** -- when a client advertises `io.modelcontextprotocol/tasks`, `antigravity` and `antigravity_reply` immediately return a `CreateTaskResult` (`resultType: "task"`) with a 128-bit `taskId`. Clients poll `tasks/get` and cancel via `tasks/cancel` per SEP-2663.
- **Backward-compatible stateful MCP (`2024-11-05`--`2025-11-25`)** -- supports `initialize` negotiation, top-level server `instructions`, `ping`, tool `annotations`, `outputSchema` + `structuredContent`, accurate `isError` flags, enriched `notifications/progress`, and JSON-RPC 2.0 §4.1 silent handling of unknown notifications.

## Tools

| Tool | Description |
|------|-------------|
| `antigravity` | Synchronous execution (or native MCP Task when client supports `io.modelcontextprotocol/tasks`); returns text, `conversation_id`, token `usage`, optional `structured_output`, `tool_errors`, `warnings`, and structured failure details |
| `antigravity_async` | Launch a task, get a `task_id` immediately (fan-out). **Not** fire-and-forget: results come back only via `antigravity_wait`. Pass `conversation_id` (UUID) to resume a conversation |
| `antigravity_reply` | Continue a previous conversation via `agy --conversation <uuid>`. Automatically reuses the original conversation's working directory (including its worktree path) when `cwd` is omitted, and fails explicitly with `conversation_not_found` if `agy` cannot locate the requested conversation |
| `antigravity_status` | Live read-only peek: tools called, last tool call, permission mode, tool errors, cumulative token usage, warnings, and failure cause |
| `antigravity_wait` | Block until multiple tasks complete and return results or structured failure details |
| `antigravity_cancel` | Kill a running async task and its child process tree (preserves worktree for inspection); if already completed or failed, preserves and reports that outcome |

## Installation

### Claude Code

```bash
claude mcp add antigravity-swarm -- uvx --upgrade antigravity-mcp-swarm \
  --model gemini-4-neon-high
```

> **Note:** Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`) and `agy` CLI installed and authenticated.

### Manual (`~/.claude.json`)

```json
{
  "mcpServers": {
    "antigravity-swarm": {
      "type": "stdio",
      "command": "uvx",
      "args": [
        "--upgrade",
        "antigravity-mcp-swarm",
        "--model", "gemini-4-neon-high"
      ]
    }
  }
}
```

### Unattended permissions

If you want `agy` to auto-approve all tool permission prompts without confirmation in unattended runs, add `--dangerously-skip-permissions` to the server args (or pass `dangerously_skip_permissions: true` per call). Only enable this in trusted repositories or sandboxed environments (`--sandbox`), as it bypasses tool approval checks.

## Usage

### Parallel execution

```
1. Call antigravity_async(prompt="Audit module A", worktree=true)  -->  task_id: "a1b2c3d4"
2. Call antigravity_async(prompt="Audit module B", worktree=true)  -->  task_id: "e5f60718"
3. Call antigravity_wait(task_ids=["a1b2c3d4", "e5f60718"])
   --> blocks until all finish, returns all results
```

`antigravity_async` and `antigravity_wait` belong in the **same turn**: `task_id` is internal to this server and not tracked by legacy MCP clients. Short `timeout` values on `antigravity_wait` are raised to a floor (default `150s`, configurable via `ANTIGRAVITY_SWARM_MIN_WAIT`) so long waits cross the client's 2-minute auto-backgrounding threshold; already-finished tasks return immediately.

### Session continuity & worktree cwd preservation

- `antigravity_reply` validates that `conversation_id` is a valid UUID (never a `task_id`). If `agy` cannot find the requested conversation and falls back to creating a new one, `antigravity-mcp-swarm` detects the mismatch and returns `status: "failed"` (`category: "conversation_not_found"`, `isError: true`) instead of silently reporting success without prior context. Note that `agy` still ran the prompt in that fresh conversation before returning, so inspect the workspace if the prompt requested edits.
- When `cwd` is omitted on `antigravity_reply` (or `antigravity_async(conversation_id=...)`), the server automatically reuses the original conversation's working directory (including its worktree path if it still exists; persisted on disk for async and worktree runs, and tracked in-process for non-worktree sync runs).
- On resumed conversations, `usage`, `num_turns`, and `duration_seconds` returned by `agy` are cumulative across the entire conversation.

### Structured output (`json_schema`)

Pass `json_schema` (JSON object or file path) to enforce structured output on the final result. When `agy` returns `result.structured_output`, it is exposed directly on `structuredContent.structured_output`.

### Slash commands, tool errors, and diagnostics

- Prompts starting with a built-in CLI slash command (such as `/help`) are rejected by `agy` under `--input-format stream-json` unless `disable_slash_commands: true` is set. Conversely, `--mode plan` relies on slash-command expansion and has no effect while `disable_slash_commands: true` is active.
- Tool step errors (`step_update` with `state: "ERROR"`) are surfaced in `structuredContent.tool_errors` and response notes without conflating OS errors with policy denials. CLI `warning:` lines written to stderr (such as unavailable modes) are surfaced in `structuredContent.warnings`.
- When `--print-timeout` terminates an in-progress turn (`finished_reason: "print_timeout"`, `AGY_ERROR` on stderr), the server classifies it cleanly as a `print_timeout` failure (preserving any partial output) rather than silently reporting an empty success.

### Worktree isolation

> **Caveats:**
> - Because worktrees branch from `HEAD` (`git worktree add -b antigravity-swarm/<id> ... HEAD`), **uncommitted and untracked changes in your working tree are not included** in the worktree. Commit prerequisite edits first if the task needs them.
> - Expired worktrees (older than `ANTIGRAVITY_SWARM_TASK_MAX_AGE`, default 24 hours) are lazily removed on a subsequent task launch via `git worktree remove --force` and `git branch -D`. Any uncommitted edits or unmerged commits remaining in an expired worktree are discarded -- merge or cherry-pick before expiry.

## Server flags

| Flag | Description |
|------|-------------|
| `--version` | Show version and exit |
| `--model MODEL` | Default model (e.g. `gemini-4-neon-high`, `gemini-3.8-flash-medium`, `claude-opus-5-5-high`) |
| `--effort LEVEL` | Default reasoning effort (`low`, `medium`, `high`, `xhigh`, `max`) |
| `--mode MODE` | Default execution mode (`accept-edits`, `auto`, `plan`) |
| `--dangerously-skip-permissions` | Auto-approve all tool permission requests by default |
| `--sandbox` | Run in sandbox mode with terminal restrictions enabled by default |
| `--agent NAME` | Default Antigravity agent name |
| `--add-dir DIR` | Add workspace directory by default (repeatable) |
| `--settings JSON_OR_PATH` | Default session-only settings JSON object or file path |
| `--disable-slash-commands` | Disable slash command/skill expansion in print mode by default |
| `--print-timeout DURATION` | Default print-mode time limit (e.g. `300s`, `10m`, `0s`) |
| `--project PROJECT` | Default project ID or name |
| `--remote-agent` | Run conversations using remote agents by default |

Boolean server flags can be explicitly overridden to `false` on any individual tool call.

## MCP Resources

| URI | Description |
|-----|-------------|
| `antigravity-swarm:///server-info` | Version, capabilities, directories, CLI protocol contract |
| `antigravity-swarm:///config` | Current server-level CLI defaults |
| `antigravity-swarm:///tasks` | All known tasks and their state, usage, tool errors, and failure details |

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ANTIGRAVITY_SWARM_LOG` | `<user-runtime>/antigravity-swarm/antigravity_mcp_swarm.log` | Log file path |
| `ANTIGRAVITY_SWARM_LOG_LEVEL` | `WARNING` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `ANTIGRAVITY_SWARM_TASK_DIR` | `<user-runtime>/antigravity-swarm/tasks` | Task output storage directory (owner-only `0700` when using default) |
| `ANTIGRAVITY_SWARM_WORKTREE_DIR` | `<user-cache>/antigravity-swarm/worktrees` | Worktree storage directory (owner-only `0700` when using default) |
| `ANTIGRAVITY_SWARM_TASK_MAX_AGE` | `86400` (24h) | Seconds after completion / last reply activity before task artifacts and worktrees are cleaned up |
| `ANTIGRAVITY_SWARM_MIN_WAIT` | `150` | Floor in seconds for `antigravity_wait` |
| `ANTIGRAVITY_SWARM_BIN` | _(auto)_ | Explicit path or command name for `agy` (auto-searches `PATH` and `~/.local/bin/agy`) |

`<user-runtime>` defaults to `$XDG_RUNTIME_DIR` when available, else `/tmp/antigravity-swarm-<uid>` on POSIX or `%TEMP%\antigravity-swarm` on Windows. `<user-cache>` defaults to `${XDG_CACHE_HOME:-~/.cache}` on POSIX (so full worktree checkouts do not sit on a RAM-capped runtime tmpfs wiped at logout) or `%TEMP%` on Windows.

## Requirements

- Python 3.8+
- Antigravity CLI (`agy` v1.5+) installed and authenticated
- Only depends on stdlib-only [`mcp-swarm-core`](https://github.com/TKasperczyk/mcp-swarm-core) (no third-party packages)
- Designed for Linux, macOS, and Windows (verified on Linux)

## License

MIT
