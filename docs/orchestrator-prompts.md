# Orchestrator Prompts — MCP-First Agent Dispatch

## Purpose

Replace OpenCode subagent spawning with direct MCP tool calls to the HPC
Hermes queue. Instead of "spawn an oracle subagent to review this code",
the orchestrator calls `run_hpc_agent_task` with the appropriate system
prompt. The same AIAgent handles every role — only the system prompt changes.

## How It Works

```
Before:   "Let me spawn an oracle subagent to evaluate this architecture."
          → OpenCode creates a subagent process with baked-in system prompt
          → Subagent routes through provider URL → hpc_gateway → queue

After:    "Call run_hpc_agent_task with system_prompt='oracle prompt'"
          → Single MCP call → Hermes queue → AIAgent (27 tools)
          → Returns result — no separate subagent lifecycle
```

The MCP server (`mcps/hpc_hermes.py`) exposes these tools:

| Tool | When to use |
|------|-------------|
| `run_hpc_agent_task(messages, system_prompt, timeout)` | Full agent task with role behavior. **Default choice.** |
| `run_on_hpc_tool(prompt, timeout)` | Simple string prompt, no system_prompt needed |
| `submit_hpc_task(prompt)` | Fire-and-forget — get task ID, check later |
| `check_hpc_task_status(task_id)` | Poll a fire-and-forget task |

## Role-to-System-Prompt Mappings

These system prompts replicate the behavior of the built-in OpenCode subagents:

### oracle

System prompt for strategic technical review and architecture decisions:

```
You are a strategic technical advisor for a software engineering team.
Review code, architecture, and designs for:

- Long-term maintainability and technical debt
- Security, scalability, and data integrity risks
- Simplification opportunities (YAGNI, deep modules)
- Trade-offs between competing approaches

You have access to read_file, grep, glob, and terminal for codebase
exploration. Be concise and direct — no flattery, no preamble.

When asked for a plan: produce a short work graph with independent lanes,
dependency-ordered lanes, and verification/review steps.
```

### librarian

System prompt for external knowledge and library research:

```
You are a research librarian. Your job is to find authoritative answers
from documentation, API references, and community examples.

For library questions:
- Check official docs first (prefer llms.txt or structured docs)
- Look at real usage examples on GitHub
- Note version-specific behavior
- Distinguish between "how does this library work?" (check docs) and
  "how does programming work?" (answer from knowledge)

You have access to webfetch and web search. Always cite your sources.
```

### fixer

System prompt for bounded implementation:

```
You are an implementation specialist. Given a complete specification,
write clean, correct code.

Rules:
- Read existing code before editing — understand the patterns first
- Make minimal changes — don't refactor beyond the scope
- Always verify by running tests after changes
- Prefer existing patterns over introducing new ones

You have access to read, write, edit, grep, glob, bash, and git tools.
Do NOT make architectural decisions — those belong to oracle.
```

### explorer

System prompt for fast codebase reconnaissance:

```
You are a codebase explorer. Given a question about unfamiliar code:

1. Use glob and grep to find relevant files and patterns
2. Use AST queries to find symbol definitions and references
3. Return a compressed summary — file paths, line numbers, key patterns
4. Do NOT read entire files — use targeted reads of specific functions

Your output should be concise and actionable: enough context for the
caller to decide what to read next, not a full dump.
```

### designer (not applicable to HPC)

The designer subagent handles UI/UX — visual layout, CSS, interaction
design. This runs locally with browser tools and is NOT a candidate for
HPC text generation. Keep spawning the designer subagent directly.

## Usage Patterns

### Single-turn (default, <300s)

```json
// Orchestrator calls:
run_hpc_agent_task(
  messages: [{"role": "user", "content": "Review this code for race conditions: ..."}],
  system_prompt: "You are a strategic technical advisor...",
  timeout: 300
)
// Returns: {"content": "The race condition is at line 42..."}
```

### Multi-turn with history

```json
// First turn:
run_hpc_agent_task(
  messages: [
    {"role": "system", "content": "You are a research librarian..."},
    {"role": "user", "content": "Find the latest FastMCP documentation"}
  ],
  timeout: 300
)

// Second turn (append response + new question):
run_hpc_agent_task(
  messages: [
    {"role": "system", "content": "..."},
    {"role": "user", "content": "Find the latest FastMCP documentation"},
    {"role": "assistant", "content": "FastMCP is at github.com/jlowin/fastmcp..."},
    {"role": "user", "content": "Now show me a streaming example"}
  ],
  timeout: 300
)
```

### Long-running (>300s, fire-and-forget + poll)

```json
// Step 1: Submit
submit_hpc_task(prompt: "Analyze this entire codebase for coupling...")
// Returns: "task-abc-123"

// Step 2: Later, check result
check_hpc_task_status(task_id: "task-abc-123")
// Returns: {"status": "completed", "result": "The coupling hotspots are..."}
```

### Complex research (tool-using agent)

The AIAgent has 27 tools including read_file, write, bash, grep, glob,
web search, and browser automation. For complex tasks, let the agent
explore:

```json
run_hpc_agent_task(
  messages: [{"role": "user", "content": "Investigate why the WAL race
    happens in hpc_batch.py. Read the code, trace the execution flow,
    and propose a fix."}],
  system_prompt: "You are a senior systems engineer. Debug concurrency
    issues in Python/SQLite applications. Use read_file, grep, and
    terminal to explore the codebase."
)
```

## Migration Guide

### Step 1: Replace subagent spawns with MCP calls

In the orchestrator's system prompt, replace patterns like:

```
# BEFORE
Let me spawn an oracle subagent to review this architecture.
→ OpenCode creates subagent with built-in oracle prompt
→ Routes through HTTP provider

# AFTER  
I'll call run_hpc_agent_task to review this architecture.
→ Single MCP call with system_prompt="oracle prompt"
→ Routes through Hermes queue
```

### Step 2: Adjust for timeout

The default MCP timeout is 300s (configurable in opencode.json as
`mcp_timeout: 300000` = 300s for the tool call itself, but the
`run_hpc_agent_task` has its own `timeout` parameter).

For tasks that need longer:
1. Set a higher `timeout` on `run_hpc_agent_task`
2. Or use submit+check pattern for unlimited duration

### Step 3: Handle tool execution location

The AIAgent runs on the daemon machine. For tasks that need LOCAL
filesystem access (the orchestrator's machine), keep using local
tools directly — don't dispatch to HPC for file operations.

Good candidates for HPC dispatch:
- Text generation (any length)
- Code review / architecture analysis
- Research and documentation lookup
- Multi-step investigation with tool use

Poor candidates for HPC dispatch:
- UI/UX design (needs local browser)
- Direct terminal commands on orchestrator machine
- File edits on orchestrator machine

## Debugging

### System diagnostics

```bash
hpc_batch.py diag                    # Quick system health (runs scripts/diag.py checks)
hpc_batch.py diag --recent           # Last 10 HPC job lifecycle summaries
hpc_batch.py diag --job <job_id>     # Full event trace for a specific job
hpc_batch.py diag --fills            # Fill task performance per prompt type
```

The `--job` flag shows a chronological trace:

```
Job 17305411  (mode=qos=preemptqos)
------------------------------------------------------------
  03:16:12 hpc.submit                  +0s
  03:17:00 vllm.monitor.start          +48s
  03:22:48 vllm.ready                  +396s   dur=368s
  03:23:00 fill.claim                  +408s   fill=fill-abc
  03:28:30 fill.complete               +738s   dur=330s
  03:31:42 hpc.kill                    +930s   dur=542s  (idle_timeout)
```

### Event naming convention

All task_events use dot-separated names for queryable namespaces:

| Namespace | Events | Query |
|-----------|--------|-------|
| `fill.*` | `fill.claim`, `fill.complete`, `fill.yield`, `fill.converge` | `WHERE event LIKE 'fill.%'` |
| `vllm.*` | `vllm.ready`, `vllm.timeout`, `vllm.monitor.start`, `vllm.job.lost` | `WHERE event LIKE 'vllm.%'` |
| `hpc.*` | `hpc.submit`, `hpc.kill` | `WHERE event LIKE 'hpc.%'` |
| `task.*` | `task.claim`, `task.complete`, `task.recover`, `task.fail.permanent` | `WHERE event LIKE 'task.%'` |
| `breaker.*` | `breaker.close`, `breaker.open`, `breaker.half_open` | `WHERE event LIKE 'breaker.%'` |
| `daemon.*` | `daemon.state.restore` | `WHERE event LIKE 'daemon.%'` |

### Daemon state files (temp)

| File | Purpose |
|------|---------|
| `/tmp/hermes-worker.heartbeat` | Daemon liveness + mode + queue state |
| `/tmp/hermes-worker.state` | Persisted state (breaker, fill cycling, timers) |
| `/tmp/hermes-worker.pid` | PID for watchdog |
| `/tmp/hpc-daemon-alert` | Health sentinel (WARN/FAIL — written by `_check_health_and_alert()`) |
| `/tmp/hpc-fill-converged` | Convergence alert (written when any fill converges) |
| `/tmp/hpc-hermes-mcp.lock` | MCP server singleton lock |
| `/tmp/.hpc-task-ready` | Task wakeup sentinel (fast daemon wakeup) |

### Health sentinel

The daemon writes `/tmp/hpc-daemon-alert` when health degrades:

```json
# WARN — fills converged, breaker half-open
{"score": "WARN", "warnings": ["ALL fills converged"]}

# FAIL — vLLM unavailable, breaker open
{"score": "FAIL", "warnings": ["Breaker OPEN", "vLLM unavailable"]}
```

The watchdog (`scripts/watchdog.sh`, run every minute via crontab) reads this file:
- WARN → logged to `logs/watchdog.log`
- FAIL → logged to `logs/watchdog.log` + stderr (cron mail)
- No file → healthy (PASS)

### Event queries

```bash
# All events for a job
python3 -c "from hpc_batch import events; ..."

# Via the events command:
hpc_batch.py events --event fill.claim     # Fill claims only
hpc_batch.py events --event vllm.timeout   # vLLM timeouts
hpc_batch.py events --stats                # Aggregate counts

# Raw SQL:
sqlite3 tasks.db "SELECT datetime(ts,'unixepoch'), event, duration
FROM task_events WHERE event LIKE 'vllm.%' ORDER BY ts DESC LIMIT 10;"

# KPI dashboard:
hpc_batch.py kpi                          # System health + fill convergence
hpc_batch.py kpi --by-prompt              # Per-prompt evolution over runs
hpc_batch.py kpi --fill --last 50         # Fill convergence analysis
```

### Maintenance

```bash
# Clean up session files older than 7 days + events older than 30 days
hpc_batch.py cleanup --dry-run            # Preview first
hpc_batch.py cleanup                      # Execute

# Migrate old underscore event names to dot-separated (one-time backfill)
hpc_batch.py migrate-events --dry-run     # Preview counts
hpc_batch.py migrate-events               # Rename in place
hpc_batch.py migrate-events --event fill_claimed  # Single event type
```

## Reference

- `mcps/hpc_hermes.py` — MCP server with 4 tools
- `worker/hermes_worker.py` — Daemon that processes tasks via AIAgent
- `scripts/watchdog.sh` — Liveness + health sentinel checker (cron: every 1 min)
- `scripts/seed-fill-tasks.sh` — Seeds fill tasks into the queue
- `hpc_batch.py diag` — System diagnostics + job tracing
- `hpc_batch.py cleanup` — Stale data cleanup
- `docs/orchestrator-prompts.md` — This file
