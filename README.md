# Governor

**Quality gates for AI agents. Guards that don't get tired.**

```bash
pip install ai-governor
```

```python
from governor.backend.memory_backend import MemoryBackend
from governor.engine.transition_engine import TransitionEngine
import governor.guards.executor_guards      # noqa: F401

backend = MemoryBackend()
engine = TransitionEngine(backend=backend)

# Try to submit without a self-review — Governor blocks it
result = engine.transition_task("TASK_001", "READY_FOR_REVIEW", "DEVELOPER")
# FAIL — EG-01: No self-review found. Fix: Create a self-review before submission.
```

Your agents produce output. But who checks it before it hits production?

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-465%20passing-brightgreen.svg)](#running-tests)

<p align="center">
  <img src="docs/assets/governor_lifecycle_demo.gif" alt="Governor lifecycle demo — guard failure, rework, and pass" width="720">
</p>

> Extracted from a system with 5,000+ governed task executions. See [docs/PROOF.md](docs/PROOF.md) for anonymized evidence.

---

## Without Governance vs. With Governor

| | Without Governance | With Governor |
|---|---|---|
| **Agent submits work** | Goes straight to production | Hits guard evaluation first |
| **Missing self-review** | Nobody notices | EG-01 blocks transition |
| **Deploy without rollback plan** | Hope for the best | EG-06 blocks until rollback documented |
| **Evidence claims** | Trust the agent | EG-07 requires multi-source verification |
| **Audit trail** | What audit trail? | Transition outcomes returned consistently; persistable via backend + graph model |
| **Quality over time** | Degrades silently | Scoring rubric enforces consistent standards |

---

## Real Failure Patterns Governor Prevents

These aren't hypothetical. We've seen each one in production.

**The Silent Deploy** — An agent marked a deployment task as complete. No rollback plan. Production went down two hours later.
Governor's EG-06 blocks any DEPLOY task that doesn't mention a rollback strategy. The task stays in ACTIVE until the agent adds one.

**The Missing Evidence** — An investigation came back "done" with a two-sentence summary. No sources, no linked reports. Plausible but unverifiable.
EG-02 requires a linked report for INVESTIGATION and AUDIT tasks. EG-07 checks for multi-source evidence. Thin output gets blocked.

**The Self-Approver** — An agent submitted work and approved it in the same step. No second pair of eyes ever evaluated the output.
Governor's state machine enforces role separation. EXECUTOR submits. REVIEWER approves. One role cannot do both.

**The Quality Slide** — Early on, every agent output was manually reviewed. Volume increased. Reviews got faster and shallower. Quality degraded — nobody noticed until a customer did.
The scoring rubric provides a consistent quality signal on every task. Guards don't get tired.

> See [docs/WHY.md](docs/WHY.md) for the full analysis.

---

## How It Works

Governor enforces a loop:

1. **Agent submits work** — transitions task to `READY_FOR_REVIEW`
2. **Guards evaluate** — pluggable validation functions check preconditions
3. **PASS or FAIL** — reviewer approves (task completes) or rejects (task reworks)
4. **Audit trail** — every transition records structured guard outcomes for persistence and analytics

```
ACTIVE ──> READY_FOR_REVIEW ──> COMPLETED
                 │    ^
                 │    │
                 v    │
               REWORK
```

Every transition requires authorization (role-based) and validation (guard-based).
No task moves forward without passing its guards.

### Concurrency Safety

Transition writes use optimistic concurrency (`expected_current_status`) to prevent
lost updates under concurrent callers. If task state changes between read and
write, the transition fails with `STATE_CONFLICT` and no state mutation is applied.

### Graph-Native Audit Events

Governor records every transition attempt as a `TransitionEvent` with attached
`GuardEvaluation` records. This enables:

- full per-task audit trails
- guard failure hotspot analysis
- policy coverage metrics
- rework lineage analysis

---

## Quick Start

```bash
pip install ai-governor
```

```python
from governor.backend.memory_backend import MemoryBackend
from governor.engine.transition_engine import TransitionEngine
import governor.guards.executor_guards      # noqa: F401

backend = MemoryBackend()
engine = TransitionEngine(backend=backend)
```

Zero external dependencies. In-memory backend. Ready in 4 lines.

Or install from source:

```bash
git clone https://github.com/june-jule/ai-governor.git
cd governor
pip install -e ".[dev]"
```

### Full Lifecycle Example

<details>
<summary>Terminal output from <code>python examples/full_task_lifecycle.py</code></summary>

```
============================================================
Governor — Full Task Lifecycle Demo
============================================================

[1] Created task: TASK_DEMO_001 (status=ACTIVE)

[2] Available transitions for EXECUTOR from ACTIVE:
    -> READY_FOR_REVIEW          NOT READY (2 guards unmet)
       EG-01: No SELF_REVIEW found
       EG-03: Missing deliverables: auth.py, auth_test.py

[3] Dry-run submission: FAIL (state unchanged)

[4] Submit without self-review (expect failure):
    Result: FAIL
    FAIL EG-01: No SELF_REVIEW found
         Fix: Create a self-review before submission
    FAIL EG-03: Missing deliverables: auth.py, auth_test.py
         Fix: Ensure all stated deliverables exist on filesystem

[5] Added report + self-review (EG-01 and EG-03 now satisfied)
    Submit for review: PASS

[6] Reviewer approves: PASS
    Task COMPLETED!

============================================================
Final status:   COMPLETED
============================================================
```

</details>

```python
import governor.guards.executor_guards      # noqa: F401

# Create a task (starts in ACTIVE state)
backend.create_task({
    "task_id": "TASK_001",
    "task_name": "Implement login flow",
    "task_type": "IMPLEMENTATION",
    "role": "DEVELOPER",
    "status": "ACTIVE",
    "priority": "HIGH",
    "content": "Build OAuth2 login flow with tests.",
})

# Add a self-review (required by EG-01)
backend.add_review("TASK_001", {
    "review_type": "SELF_REVIEW",
    "rating": 8.5,
    "content": "Login flow implemented with PKCE. All tests pass.",
})

# Submit for review (ACTIVE -> READY_FOR_REVIEW)
result = engine.transition_task("TASK_001", "READY_FOR_REVIEW", "DEVELOPER")
print(result["result"])  # "PASS"

# Reviewer approves (READY_FOR_REVIEW -> COMPLETED)
result = engine.transition_task("TASK_001", "COMPLETED", "REVIEWER")
print(result["result"])  # "PASS" — task is now complete
```

### What a Failed Transition Looks Like

This is the "aha moment" — Governor tells your agent *exactly* what's wrong and how to fix it:

```python
# Try to submit WITHOUT a self-review
result = engine.transition_task("TASK_001", "READY_FOR_REVIEW", "DEVELOPER")

print(result["result"])  # "FAIL"

for gr in result["guard_results"]:
    if not gr["passed"]:
        print(f"  BLOCKED by {gr['guard_id']}: {gr['reason']}")
        print(f"  Fix: {gr['fix_hint']}")

# Output:
#   BLOCKED by EG-01: No SELF_REVIEW found
#   Fix: Create a self-review before submission
```

Guards don't just say "no" — they tell the agent what to do next.
Agents can self-correct in a loop: submit, read guard feedback, fix, resubmit.

### Async Support

For async agent frameworks (LangChain, CrewAI, OpenAI Agents SDK):

```python
from governor.backend.async_base import AsyncGovernorBackend
from governor.engine.async_engine import AsyncTransitionEngine

engine = AsyncTransitionEngine(backend=my_async_backend)

result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "DEVELOPER")
```

Async Neo4j is supported via `AsyncNeo4jBackend`:

```python
from governor.backend.async_neo4j_backend import AsyncNeo4jBackend
from governor.engine.async_engine import AsyncTransitionEngine

backend = AsyncNeo4jBackend(
    uri="neo4j://localhost:7687",
    user="neo4j",
    password="password",
)
engine = AsyncTransitionEngine(backend=backend)
```

### TypeScript SDK

A wire-compatible TypeScript SDK is available in [`governor-ts/`](governor-ts/):

```bash
npm install @governor/core
```

```typescript
import { MemoryBackend, TransitionEngine } from "@governor/core";

const backend = new MemoryBackend();
const engine = new TransitionEngine(backend);

const result = await engine.transitionTask("TASK_001", "READY_FOR_REVIEW", "EXECUTOR");
```

Same state machine, same guards (EG-01 through EG-08), same contract. See the [TypeScript README](governor-ts/README.md) for full docs and feature parity table.

---

## Guard Rules

Governor ships with the EG (Executor Governor) guard family:

### Executor Guards (EG) — Pre-submission

| Guard | Check | Fix Hint |
|-------|-------|----------|
| EG-01 | Self-review exists | Add a self-review before submitting |
| EG-02 | Report exists (task-type aware) | Create a report documenting your work |
| EG-03 | Deliverables referenced | Reference deliverable files in your report |
| EG-04 | No implied deploys in non-DEPLOY tasks | Remove deploy references or change task type |
| EG-05 | No secrets or credentials in content | Remove credentials; use env vars or secrets manager |
| EG-06 | Rollback plan exists (DEPLOY only) | Add a rollback plan to your report |
| EG-07 | Multi-source evidence (AUDIT only) | Reference multiple evidence sources |
| EG-08 | Test references (IMPLEMENTATION only) | Reference test files or test results |

Guards are **pure evaluators** — they read state but never mutate it. All guards
run on every transition (no short-circuit), giving callers a full picture of what
needs fixing.

See [docs/GUARD_CATALOG.md](docs/GUARD_CATALOG.md) for the complete catalog and
extension points.

---

## Custom Guards

Register domain-specific guards with the `@register_guard` decorator:

```python
from governor.engine.transition_engine import GuardContext, GuardResult, register_guard

@register_guard("REVIEW-01")
def guard_min_review_rating(ctx: GuardContext) -> GuardResult:
    """Require self-review rating >= 7.0 before submission."""
    for r in ctx.relationships:
        if r.get("type") == "HAS_REVIEW":
            node = r.get("node") or {}
            if node.get("review_type") == "SELF_REVIEW":
                rating = float(node.get("rating", 0))
                if rating >= 7.0:
                    return GuardResult("REVIEW-01", True, f"Rating {rating} meets threshold")
                return GuardResult(
                    "REVIEW-01", False,
                    f"Rating {rating} below minimum 7.0",
                    fix_hint="Improve work quality and update self-review rating",
                )
    return GuardResult("REVIEW-01", False, "No self-review found",
                       fix_hint="Add a self-review first")
```

Then reference it in your state machine JSON:

```json
{
    "id": "T01",
    "guards": ["EG-01", "EG-02", "REVIEW-01"]
}
```

---

## Scoring

Governor scores tasks in two parts — compliance (did you meet requirements?) and excellence (did you go beyond them?):

- **Compliance categories** (up to 85 points): Completion gate, core execution,
  code quality, documentation quality — weighted by your rubric
- **Excellence bonus** (up to 15 points): Innovation, clean design, going beyond
  requirements
- **Deductions**: Penalty points for specific issues (linter errors, missing
  tests, etc.)

```python
from governor.scoring.rubric import ScoringRubric

rubric = ScoringRubric()
result = rubric.score(
    categories={
        "completion_gate": 20,
        "core_execution": 18,
        "code_quality": 23,
        "documentation_quality": 17,
    },
    deductions=[{"type": "linter_errors", "points": 5}],
    excellence=10,
)

print(result["final_score"])   # 73
print(result["rating"])        # "NEEDS_IMPROVEMENT"
```

Rubrics are JSON-configurable. Ship your own weights, categories, and rating
thresholds. See [docs/SCORING_RUBRIC.md](docs/SCORING_RUBRIC.md) for details.

---

## Role Aliases

Map your organization's role names to the canonical state machine roles:

```python
engine = TransitionEngine(
    backend=backend,
    role_aliases={
        "DEVELOPER": "EXECUTOR",
        "QA": "EXECUTOR",
        "SRE": "EXECUTOR",
        "TEAM_LEAD": "REVIEWER",
    },
)

# Now "DEVELOPER" can submit for review (mapped to EXECUTOR)
result = engine.transition_task("TASK_001", "READY_FOR_REVIEW", "DEVELOPER")
```

---

## Framework Integrations

Governor has no framework dependencies. Here's how it plugs into three popular ones:

| Framework | Pattern | Example |
|-----------|---------|---------|
| **LangChain** | `GovernorCallback` on `on_agent_finish` — guards gate output | [`examples/langchain_guard.py`](examples/langchain_guard.py) |
| **CrewAI** | Crew pipeline with Governor quality gate between agents | [`examples/crewai_lifecycle.py`](examples/crewai_lifecycle.py) |
| **OpenAI Agents SDK** | `governor_submit` tool for agent self-correction | [`examples/openai_agents_sdk.py`](examples/openai_agents_sdk.py) |

Each example is standalone glue code (~55 LOC).

---

## MCP Server

Expose Governor as MCP tools for Claude, Cursor, and other MCP-compatible agents:

```bash
pip install "ai-governor[mcp]"
```

```python
from governor.mcp.tools import create_governor_tools

tools = create_governor_tools(engine)
# Returns 6 tools:
# - governor_transition_task
# - governor_get_available_transitions
# - governor_get_task_audit_trail
# - governor_get_guard_failure_hotspots
# - governor_get_rework_lineage
# - governor_get_policy_coverage
```

See [`examples/mcp_server.py`](examples/mcp_server.py) for a minimal MCP server.

### MCP Tool Contracts

- `governor_transition_task`
  - input: `task_id`, `target_state`, `calling_role`, optional `dry_run`, optional `transition_params`
  - output: transition result with `result`, `guard_results`, `temporal_updates`, and error codes like `STATE_CONFLICT`
- `governor_get_available_transitions`
  - input: `task_id`, `calling_role`
  - output: current state and per-transition readiness/guard gaps
- `governor_get_task_audit_trail`
  - input: `task_id`, optional `limit`
  - output: transition events (including embedded guard evaluations)
- `governor_get_guard_failure_hotspots`
  - input: optional `limit`
  - output: ranked guards by `failures` and `evaluations`
- `governor_get_rework_lineage`
  - input: `task_id`
  - output: task lineage plus `rework_count`
- `governor_get_policy_coverage`
  - input: none
  - output: per-guard pass/fail coverage and aggregate totals

---

## Why Neo4j

Governance data is relationships: tasks depend on tasks, guards reference
policies, reviews link to reports. Neo4j stores these as a graph, which makes
queries like "what's transitively blocked by task X?" a one-liner instead of a
recursive CTE.

<p align="center">
  <img src="docs/assets/neo4j_review_graph.png" alt="Neo4j graph showing a Task with three Review nodes scored 72, 78, and 90, connected to a Governor node via WRITTEN_BY and REVIEWED_BY relationships" width="600">
</p>

> **One Cypher query returns the full review graph above.** A Task node with three scored Reviews, each linked to the Governor agent that wrote and reviewed them. In SQL, this would require 3+ JOINs across separate tables.

### Graph vs. SQL for Governance Queries

| Query Pattern | SQL | Neo4j |
|---|---|---|
| Full audit trail for a task | 3-4 JOINs + subqueries | Single path traversal |
| All tasks transitively blocked by task X | Recursive CTE (complex, slow) | Variable-length path `[:DEPENDS_ON*]` |
| Guard failure impact ranking | Multiple self-joins | `gds.pageRank.stream()` |
| Circular dependency detection | Very hard / not practical | `gds.scc.stream()` — O(V+E) |
| Task clustering by failure pattern | Not practical | `gds.louvain.stream()` |

For the full analytics reference, see [docs/GRAPH_ANALYTICS.md](docs/GRAPH_ANALYTICS.md).

### Cypher Examples

```cypher
// Full audit trail for a task — one path query
MATCH (t:Task {task_id: $task_id})-[r]->(n)
RETURN t.task_name, type(r) AS rel, labels(n) AS node_type, properties(n) AS detail

// Which guards fail most often? — one aggregation
MATCH (t:Task) WHERE t.status = 'ACTIVE'
RETURN t.task_type, count(*) AS blocked_count ORDER BY blocked_count DESC

// Trace rework lineage — relationship traversal
MATCH (t:Task)-[:HAS_REVIEW]->(r:Review {review_type: 'SELF_REVIEW'})
WHERE t.status = 'REWORK'
RETURN t.task_id, t.task_name, r.rating, t.revision_count

// Transitive blocking — variable-length path
MATCH (blocked:Task)-[:DEPENDS_ON*]->(root:Task {task_id: $task_id})
RETURN blocked.task_id, blocked.task_name, blocked.status
```

### Getting Started with Neo4j

Governor ships with a Neo4j backend. The in-memory backend works for development
and testing. For production governance at scale, use Neo4j.

```bash
pip install "ai-governor[neo4j]"
```

> **[Deploy to Neo4j AuraDB](https://neo4j.com/pricing/?utm_source=github&utm_medium=referral&utm_campaign=Neo4_labs_AI_Governor)** for a managed cloud experience — free tier available.

See [`examples/audit_trail.py`](examples/audit_trail.py) for a full audit trail
demo with graph query patterns.

Packaged schema file for bootstrap: `governor/schema/neo4j_schema.cypher`.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                  TransitionEngine                    │
│                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │  State    │    │  Guard   │    │   Event      │  │
│  │  Machine  │───>│  Registry│───>│   Callbacks  │  │
│  │  (JSON)   │    │          │    │              │  │
│  └──────────┘    └──────────┘    └──────────────┘  │
│        │                                            │
│        v                                            │
│  ┌──────────────────────────────────────────────┐   │
│  │            GovernorBackend (ABC)              │   │
│  │  get_task() · update_task(CAS) · task_exists()│   │
│  │  get_reviews_for_task() · get_reports_for_   │   │
│  │  task() · record_transition_event()          │   │
│  │  analytics APIs (audit/hotspots/coverage)   │   │
│  └──────────┬─────────────────┬─────────────────┘   │
└─────────────┼─────────────────┼─────────────────────┘
              │                 │
    ┌─────────v───┐   ┌────────v────┐
    │MemoryBackend│   │Neo4jBackend │   (or your own)
    │  (in-memory)│   │ (neo4j-py)  │
    └─────────────┘   └─────────────┘
```

---

## Project Structure

```
governor/
├── governor/               # Core Python package
│   ├── backend/            # GovernorBackend + AsyncGovernorBackend
│   ├── engine/             # TransitionEngine + AsyncTransitionEngine
│   ├── guards/             # Built-in EG guards
│   ├── analytics/          # Graph analytics (GDS algorithms)
│   ├── callbacks/          # Webhook event callbacks
│   ├── mcp/               # MCP tool wrappers
│   ├── scoring/            # ScoringRubric + rubric JSON files
│   └── schema/             # state_machine.json + neo4j_schema.cypher
├── governor-ts/            # TypeScript SDK (wire-compatible)
├── schema/                 # neo4j_schema.cypher (bootstrap)
├── docs/                   # Architecture docs
├── examples/               # Runnable demo scripts
├── benchmarks/             # Guard evaluation benchmarks
├── tests/                  # 465 pytest tests
├── pyproject.toml
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

All production code lives under `governor.engine.*`. Benchmarks use a
self-contained engine in `benchmarks/bench_engine.py`.

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest
```

465 tests covering engine, guards, scoring, validation, async support, Neo4j
backend (mocked), MCP tools, and error boundaries.

---

## Documentation

- [Guard Catalog](docs/GUARD_CATALOG.md) — All built-in guards with extension examples
- [State Machine Design](docs/STATE_MACHINE_DESIGN.md) — Transitions, roles, guards
- [MCP Tools Reference](docs/MCP_TOOLS.md) — Tool schemas and request/response payloads
- [Scoring Rubric](docs/SCORING_RUBRIC.md) — Evidence-based scoring model
- [Why Governance](docs/WHY.md) — Real failure scenarios and how guards prevent them
- [Graph Analytics](docs/GRAPH_ANALYTICS.md) — GDS algorithms for governance insights
- [Neo4j vs Alternatives](docs/NEO4J_VS_ALTERNATIVES.md) — Backend comparison and decision framework
- [Production Evidence](docs/PROOF.md) — Anonymized data from 5,000+ governed executions
- [Migration Guide](docs/MIGRATION.md) — Upgrade steps between versions
- [Benchmark Results](docs/BENCHMARK_RESULTS.md) — Guard evaluation performance data

---

## When NOT to Use Governor

Governor solves workflow-level governance. It's not the right tool for every problem:

- **Input/output schema validation** — If you need to validate that an LLM response
  matches a pydantic schema or passes regex checks, use [Guardrails AI](https://github.com/guardrails-ai/guardrails).
  Governor validates *workflow preconditions*, not data shapes.

- **Dialogue flow control** — If you need to enforce conversation rails, detect off-topic
  responses, or control chatbot behavior, use [NeMo Guardrails](https://github.com/NVIDIA/NeMo-Guardrails).
  Governor is for task workflows, not conversations.

- **Single-shot agents** — If your agent runs once, returns a result, and you move on,
  Governor adds overhead without benefit. Governor shines when agents produce work
  that needs review, approval, or audit trails before reaching production.

- **Lightweight prototypes** — If you're exploring an idea and don't yet care about
  quality gates, skip Governor for now. Add it when agent output starts going to
  real users or production systems.

**Use Governor when:** Agents produce work that moves through review stages, multiple
roles are involved, and you need audit trails of who approved what.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-guard`)
3. Install dev dependencies (`pip install -e ".[dev]"`)
4. Make your changes and add tests
5. Run tests (`pytest`)
6. Submit a pull request

Guards must follow the `GuardContext -> GuardResult` contract and be pure
evaluators (no state mutation). See [docs/GUARD_CATALOG.md](docs/GUARD_CATALOG.md)
for the full contract specification.

---

## License

[Apache License 2.0](LICENSE)
