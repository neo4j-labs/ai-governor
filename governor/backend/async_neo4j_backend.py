"""Async Neo4j backend for Governor using Neo4j async driver.

Requires the ``neo4j`` Python driver::

    pip install neo4j

Usage::

    from governor.backend.async_neo4j_backend import AsyncNeo4jBackend

    backend = AsyncNeo4jBackend(
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="<your-password>",
    )

Or from environment variables::

    export GOVERNOR_NEO4J_URI=neo4j://localhost:7687
    export GOVERNOR_NEO4J_USER=neo4j
    export GOVERNOR_NEO4J_PASSWORD=<your-password>

    backend = AsyncNeo4jBackend.from_env()
"""

import collections
from datetime import datetime, timezone
import logging
import os
import random
import time
from typing import Any, Dict, List, Optional, Tuple

from governor.backend.async_base import AsyncGovernorBackend
from governor.backend.neo4j_backend import _validate_property_name

_AsyncNeo4jDriver: Any = None
try:
    from neo4j import AsyncGraphDatabase
    _AsyncNeo4jDriver = AsyncGraphDatabase
except ImportError:
    pass

logger = logging.getLogger("governor.backend.async_neo4j")


class _AsyncQueryRateLimiter:
    """Sliding-window rate limiter for async Neo4j backend queries.

    Uses ``asyncio.Lock`` for proper async concurrency control.  This
    is correct for code running inside an asyncio event loop — unlike
    ``threading.Lock``, it will not block the loop and is safe under
    non-GIL runtimes (e.g. PyPy, free-threaded CPython 3.13+).
    """

    def __init__(self, max_queries: int, window_seconds: float) -> None:
        self._max = max(1, max_queries)
        self._window = max(0.01, window_seconds)
        self._timestamps: collections.deque = collections.deque()  # type: ignore[type-arg]
        self._lock: Optional[Any] = None  # lazily created asyncio.Lock

    def _get_lock(self) -> Any:
        """Lazy-init asyncio.Lock (must be created inside a running loop)."""
        if self._lock is None:
            import asyncio
            self._lock = asyncio.Lock()
        return self._lock

    async def check(self) -> bool:
        """Return True if the query is allowed, False if rate-limited."""
        now = time.monotonic()
        async with self._get_lock():
            cutoff = now - self._window
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max:
                return False
            self._timestamps.append(now)
            return True


def from_env(**overrides: Any) -> "AsyncNeo4jBackend":
    """Create an AsyncNeo4jBackend from environment variables.

    Reads ``GOVERNOR_NEO4J_URI``, ``GOVERNOR_NEO4J_USER``,
    ``GOVERNOR_NEO4J_PASSWORD``, and ``GOVERNOR_NEO4J_DATABASE`` from the
    environment.  Keyword arguments override env vars.
    """
    kwargs: Dict[str, Any] = {
        "uri": os.environ.get("GOVERNOR_NEO4J_URI"),
        "user": os.environ.get("GOVERNOR_NEO4J_USER"),
        "password": os.environ.get("GOVERNOR_NEO4J_PASSWORD"),
        "database": os.environ.get("GOVERNOR_NEO4J_DATABASE", "neo4j"),
    }
    kwargs.update(overrides)
    return AsyncNeo4jBackend(**kwargs)


class AsyncNeo4jBackend(AsyncGovernorBackend):
    """Async Neo4j backend with transactional transition persistence."""

    from_env = staticmethod(from_env)

    def __init__(
        self,
        uri: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        relationship_limit: int = 200,
        query_max_retries: int = 2,
        query_retry_backoff_seconds: float = 0.1,
        query_observer: Optional[Any] = None,
        max_connection_pool_size: int = 100,
        connection_acquisition_timeout: float = 60.0,
        query_timeout_seconds: Optional[float] = 30.0,
        query_rate_limit: Optional[Tuple[int, float]] = None,
    ) -> None:
        if _AsyncNeo4jDriver is None:
            raise ImportError(
                "The 'neo4j' package is required for AsyncNeo4jBackend. "
                "Install it with: pip install neo4j"
            )
        # Resolve from env vars when not supplied directly.
        uri = uri or os.environ.get("GOVERNOR_NEO4J_URI")
        user = user or os.environ.get("GOVERNOR_NEO4J_USER")
        password = password or os.environ.get("GOVERNOR_NEO4J_PASSWORD")
        database = database or os.environ.get("GOVERNOR_NEO4J_DATABASE", "neo4j")

        if not uri or not user or not password:
            raise ValueError(
                "Neo4j credentials required. Pass uri/user/password directly "
                "or set GOVERNOR_NEO4J_URI, GOVERNOR_NEO4J_USER, "
                "GOVERNOR_NEO4J_PASSWORD environment variables."
            )
        self._driver = _AsyncNeo4jDriver.driver(
            uri,
            auth=(user, password),
            max_connection_pool_size=max_connection_pool_size,
            connection_acquisition_timeout=connection_acquisition_timeout,
        )
        self._database = database
        self._relationship_limit = max(1, int(relationship_limit))
        self._query_max_retries = max(0, int(query_max_retries))
        self._query_retry_backoff_seconds = max(0.0, float(query_retry_backoff_seconds))
        self._query_observer = query_observer
        self._query_timeout_seconds = query_timeout_seconds
        self._query_rate_limiter: Optional[_AsyncQueryRateLimiter] = None
        if query_rate_limit is not None:
            max_queries, window_seconds = query_rate_limit
            self._query_rate_limiter = _AsyncQueryRateLimiter(max_queries, window_seconds)

    async def verify_connectivity(self) -> None:
        """Verify the Neo4j connection is reachable.

        Call after init for async backends (cannot await in ``__init__``).

        Raises:
            ConnectionError: If the connection cannot be established.
        """
        try:
            await self._driver.verify_connectivity()
        except Exception as exc:
            await self._driver.close()
            raise ConnectionError(
                f"Failed to connect to Neo4j: {exc}"
            ) from exc

    async def close(self) -> None:
        await self._driver.close()

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        """Fetch a task and its relationships from Neo4j (async).

        Relationships are limited to ``relationship_limit`` (default 200)
        per direction. When the limit is hit, the response includes
        ``"relationships_truncated": True`` so callers know that not all
        relationships were returned.
        """
        # Fetch one *extra* row per direction to detect truncation.
        fetch_limit = self._relationship_limit + 1

        query = """
        MATCH (t:Task {task_id: $task_id})
        CALL {
          WITH t
          OPTIONAL MATCH (t)-[r_out]->(n_out)
          WHERE type(r_out) IN ['HAS_REVIEW', 'HANDOFF_TO']
          WITH r_out, n_out ORDER BY coalesce(n_out.created_date, '0000-00-00') DESC LIMIT $fetch_limit
          RETURN collect({
            type: type(r_out),
            node: properties(n_out),
            node_labels: labels(n_out)
          }) AS out_rels
        }
        CALL {
          WITH t
          OPTIONAL MATCH (n_in)-[r_in]->(t)
          WHERE type(r_in) IN ['REPORTS_ON']
          WITH r_in, n_in ORDER BY coalesce(n_in.created_date, '0000-00-00') DESC LIMIT $fetch_limit
          RETURN collect({
            type: type(r_in),
            node: properties(n_in),
            node_labels: labels(n_in)
          }) AS in_rels
        }
        RETURN properties(t) AS task, out_rels, in_rels
        """
        rows = await self._run_read_query(
            query,
            {"task_id": task_id, "fetch_limit": fetch_limit},
        )
        if not rows:
            raise ValueError(f"Task not found: {task_id}")

        row = rows[0]
        out_rels = row.get("out_rels") or []
        in_rels = row.get("in_rels") or []

        truncated = len(out_rels) > self._relationship_limit or len(in_rels) > self._relationship_limit
        out_rels = out_rels[: self._relationship_limit]
        in_rels = in_rels[: self._relationship_limit]

        relationships = [r for r in out_rels + in_rels if r.get("type")]

        result: Dict[str, Any] = {"task": row["task"], "relationships": relationships}
        if truncated:
            result["relationships_truncated"] = True
            logger.warning(
                "Task %s has more relationships than the configured limit (%d). "
                "Increase relationship_limit for full results.",
                task_id,
                self._relationship_limit,
            )
        return result

    async def update_task(
        self,
        task_id: str,
        updates: Dict[str, Any],
        expected_current_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        set_clauses = []
        params: Dict[str, Any] = {"task_id": task_id, "expected_current_status": expected_current_status}
        for key, value in updates.items():
            _validate_property_name(key)
            param_name = f"upd_{key}"
            if value is None:
                set_clauses.append(f"t.{key} = null")
            else:
                set_clauses.append(f"t.{key} = ${param_name}")
                params[param_name] = _normalize_task_field(key, value)
        params["last_updated"] = datetime.now(timezone.utc).isoformat()
        set_clauses.append("t.last_updated = $last_updated")
        query = f"""
        MATCH (t:Task {{task_id: $task_id}})
        WHERE $expected_current_status IS NULL OR t.status = $expected_current_status
        SET {', '.join(set_clauses)}
        RETURN t.task_id AS task_id, t.status AS status
        """
        rows = await self._run_write_query(query, params)
        if not rows:
            if expected_current_status is not None and await self.task_exists(task_id):
                current = await self._run_read_query(
                    "MATCH (t:Task {task_id: $task_id}) RETURN t.status AS status",
                    {"task_id": task_id},
                )
                return {
                    "success": False,
                    "error_code": "STATE_CONFLICT",
                    "task_id": task_id,
                    "expected_current_status": expected_current_status,
                    "actual_current_status": (current[0].get("status") if current else None),
                }
            raise ValueError(f"Task not found during update: {task_id}")
        return {"success": True, "task_id": task_id, "new_status": rows[0].get("status")}

    async def apply_transition(
        self,
        task_id: str,
        updates: Dict[str, Any],
        event: Dict[str, Any],
        expected_current_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        set_clauses = []
        params: Dict[str, Any] = {
            "task_id": task_id,
            "expected_current_status": expected_current_status,
            "transition_id": event.get("transition_id"),
            "from_state": event.get("from_state"),
            "to_state": event.get("to_state"),
            "calling_role": event.get("calling_role"),
            "result": event.get("result"),
            "dry_run": bool(event.get("dry_run")),
            "rejection_reason": event.get("rejection_reason"),
            "occurred_at": event.get("occurred_at"),
            "guard_results": event.get("guard_results", []),
        }
        for key, value in updates.items():
            _validate_property_name(key)
            param_name = f"upd_{key}"
            if value is None:
                set_clauses.append(f"t.{key} = null")
            else:
                set_clauses.append(f"t.{key} = ${param_name}")
                params[param_name] = _normalize_task_field(key, value)
        params["last_updated"] = datetime.now(timezone.utc).isoformat()
        set_clauses.append("t.last_updated = $last_updated")
        query = f"""
        MATCH (t:Task {{task_id: $task_id}})
        WHERE $expected_current_status IS NULL OR t.status = $expected_current_status
        SET {', '.join(set_clauses)}
        WITH t
        CREATE (te:TransitionEvent {{
          event_id: randomUUID(),
          transition_id: $transition_id,
          from_state: $from_state,
          to_state: $to_state,
          calling_role: $calling_role,
          result: $result,
          dry_run: $dry_run,
          rejection_reason: $rejection_reason,
          occurred_at: $occurred_at
        }})
        CREATE (t)-[:HAS_TRANSITION_EVENT]->(te)
        FOREACH (gr IN $guard_results |
          CREATE (ge:GuardEvaluation {{
            eval_id: randomUUID(),
            guard_id: gr.guard_id,
            passed: gr.passed,
            warning: coalesce(gr.warning, false),
            reason: coalesce(gr.reason, ""),
            fix_hint: coalesce(gr.fix_hint, "")
          }})
          CREATE (te)-[:HAS_GUARD_EVALUATION]->(ge)
        )
        RETURN t.task_id AS task_id, t.status AS status, te.event_id AS event_id
        """
        rows = await self._run_write_query(query, params)
        if not rows:
            if expected_current_status is not None and await self.task_exists(task_id):
                current = await self._run_read_query(
                    "MATCH (t:Task {task_id: $task_id}) RETURN t.status AS status",
                    {"task_id": task_id},
                )
                return {
                    "success": False,
                    "error_code": "STATE_CONFLICT",
                    "task_id": task_id,
                    "expected_current_status": expected_current_status,
                    "actual_current_status": (current[0].get("status") if current else None),
                }
            raise ValueError(f"Task not found during update: {task_id}")
        return {
            "success": True,
            "task_id": rows[0].get("task_id"),
            "new_status": rows[0].get("status"),
            "event_id": rows[0].get("event_id"),
        }

    async def task_exists(self, task_id: str) -> bool:
        rows = await self._run_read_query(
            "MATCH (t:Task {task_id: $task_id}) RETURN count(t) AS cnt",
            {"task_id": task_id},
        )
        return bool(rows and rows[0].get("cnt", 0) > 0)

    async def get_reviews_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        rows = await self._run_read_query(
            """
            MATCH (t:Task {task_id: $task_id})-[:HAS_REVIEW]->(r:Review)
            RETURN properties(r) AS review
            """,
            {"task_id": task_id},
        )
        return [r["review"] for r in rows]

    async def get_reports_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        rows = await self._run_read_query(
            """
            MATCH (r:Report)-[:REPORTS_ON]->(t:Task {task_id: $task_id})
            RETURN properties(r) AS report
            """,
            {"task_id": task_id},
        )
        return [r["report"] for r in rows]

    async def record_transition_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        rows = await self._run_write_query(
            """
            MATCH (t:Task {task_id: $task_id})
            CREATE (te:TransitionEvent {
              event_id: randomUUID(),
              transition_id: $transition_id,
              from_state: $from_state,
              to_state: $to_state,
              calling_role: $calling_role,
              result: $result,
              dry_run: $dry_run,
              rejection_reason: $rejection_reason,
              occurred_at: $occurred_at
            })
            CREATE (t)-[:HAS_TRANSITION_EVENT]->(te)
            FOREACH (gr IN $guard_results |
              CREATE (ge:GuardEvaluation {
                eval_id: randomUUID(),
                guard_id: gr.guard_id,
                passed: gr.passed,
                warning: coalesce(gr.warning, false),
                reason: coalesce(gr.reason, ""),
                fix_hint: coalesce(gr.fix_hint, "")
              })
              CREATE (te)-[:HAS_GUARD_EVALUATION]->(ge)
            )
            RETURN te.event_id AS event_id
            """,
            {
                "task_id": event.get("task_id"),
                "transition_id": event.get("transition_id"),
                "from_state": event.get("from_state"),
                "to_state": event.get("to_state"),
                "calling_role": event.get("calling_role"),
                "result": event.get("result"),
                "dry_run": bool(event.get("dry_run")),
                "rejection_reason": event.get("rejection_reason"),
                "occurred_at": event.get("occurred_at"),
                "guard_results": event.get("guard_results", []),
            },
        )
        if not rows:
            return {"success": False, "error_code": "EVENT_WRITE_FAILED"}
        return {"success": True, "event_id": rows[0].get("event_id")}

    async def get_task_audit_trail(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        rows = await self._run_read_query(
            """
            MATCH (:Task {task_id: $task_id})-[:HAS_TRANSITION_EVENT]->(te:TransitionEvent)
            OPTIONAL MATCH (te)-[:HAS_GUARD_EVALUATION]->(ge:GuardEvaluation)
            WITH te, collect(properties(ge)) AS guard_results
            RETURN properties(te) AS event, guard_results
            ORDER BY te.occurred_at DESC
            LIMIT $limit
            """,
            {"task_id": task_id, "limit": max(1, int(limit))},
        )
        return [{**row["event"], "guard_results": row.get("guard_results", [])} for row in rows]

    async def get_guard_failure_hotspots(self, limit: int = 10) -> List[Dict[str, Any]]:
        return await self._run_read_query(
            """
            MATCH (ge:GuardEvaluation)
            RETURN ge.guard_id AS guard_id,
                   count(ge) AS evaluations,
                   sum(CASE WHEN ge.passed THEN 0 ELSE 1 END) AS failures
            ORDER BY failures DESC, evaluations DESC
            LIMIT $limit
            """,
            {"limit": max(1, int(limit))},
        )

    async def get_policy_coverage(self, since: Optional[str] = None) -> Dict[str, Any]:
        """Return per-guard evaluation counts.

        Args:
            since: Optional ISO-8601 timestamp.  When provided, only
                ``GuardEvaluation`` nodes linked to ``TransitionEvent``
                nodes with ``occurred_at >= since`` are counted.
        """
        if since:
            match_clause = (
                "MATCH (te:TransitionEvent)-[:HAS_GUARD_EVALUATION]->(ge:GuardEvaluation) "
                "WHERE te.occurred_at >= $since"
            )
            params: Dict[str, Any] = {"since": since}
        else:
            match_clause = "MATCH (ge:GuardEvaluation)"
            params = {}

        agg_cols = (
            "count(ge) AS evaluations, "
            "sum(CASE WHEN ge.passed THEN 1 ELSE 0 END) AS passes, "
            "sum(CASE WHEN ge.passed THEN 0 ELSE 1 END) AS fails"
        )
        per_guard_query = f"{match_clause} RETURN ge.guard_id AS guard_id, {agg_cols} ORDER BY guard_id"
        totals_query = f"{match_clause} RETURN {agg_cols}"

        guards = await self._run_read_query(per_guard_query, params)
        totals_rows = await self._run_read_query(totals_query, params)
        totals = totals_rows[0] if totals_rows else {"evaluations": 0, "passes": 0, "fails": 0}
        return {"guards": guards, "totals": totals}

    async def get_rework_lineage(self, task_id: str) -> Dict[str, Any]:
        lineage = await self._run_read_query(
            """
            MATCH (:Task {task_id: $task_id})-[:HAS_TRANSITION_EVENT]->(te:TransitionEvent)
            WHERE te.result = 'PASS'
            RETURN te.transition_id AS transition_id,
                   te.from_state AS from_state,
                   te.to_state AS to_state,
                   te.result AS result,
                   te.occurred_at AS occurred_at
            ORDER BY te.occurred_at ASC
            """,
            {"task_id": task_id},
        )
        rework_count = sum(1 for item in lineage if item.get("to_state") == "REWORK")
        return {"task_id": task_id, "rework_count": rework_count, "lineage": lineage}

    async def _run_read_query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        return await self._run_query(query, params, mode="read")

    async def _run_write_query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        return await self._run_query(query, params, mode="write")

    async def _run_query(self, query: str, params: Dict[str, Any], mode: str) -> List[Dict[str, Any]]:
        if self._query_rate_limiter is not None and not await self._query_rate_limiter.check():
            raise RuntimeError(
                "Neo4j backend query rate limit exceeded. "
                "Reduce query volume or increase the rate limit."
            )
        attempt = 0
        while True:
            attempt += 1
            start = time.perf_counter()
            try:
                async with self._driver.session(database=self._database) as session:
                    async def _read(tx):
                        result = await tx.run(query, params)
                        rows = []
                        async for record in result:
                            rows.append(dict(record))
                        return rows

                    tx_kwargs: Dict[str, Any] = {}
                    if self._query_timeout_seconds is not None:
                        tx_kwargs["timeout"] = self._query_timeout_seconds

                    if mode == "write":
                        rows = await session.execute_write(_read, **tx_kwargs)
                    else:
                        rows = await session.execute_read(_read, **tx_kwargs)

                elapsed_ms = int((time.perf_counter() - start) * 1000)
                if self._query_observer is not None:
                    self._query_observer(
                        {
                            "mode": mode,
                            "attempt": attempt,
                            "elapsed_ms": elapsed_ms,
                            "params_keys": sorted(params.keys()),
                        }
                    )
                return rows
            except Exception as exc:
                retryable = self._is_retryable(exc)
                if not retryable or attempt > self._query_max_retries:
                    raise
                base_delay = self._query_retry_backoff_seconds * (2 ** (attempt - 1))
                jitter = random.uniform(0, base_delay * 0.5)
                sleep_seconds = base_delay + jitter
                logger.warning(
                    "Async Neo4j query failed (attempt %s/%s), retrying in %.3fs: %s",
                    attempt,
                    self._query_max_retries + 1,
                    sleep_seconds,
                    exc,
                    extra={"ctx": {"mode": mode, "attempt": attempt, "max_retries": self._query_max_retries + 1}},
                )
                await _sleep(sleep_seconds)

    def _is_retryable(self, exc: Exception) -> bool:
        # Works with and without importing Neo4j exception classes.
        cls_name = exc.__class__.__name__
        if cls_name in {
            "TransientError", "ServiceUnavailable", "SessionExpired",
            "WriteServiceUnavailable",
        }:
            return True
        # Check Neo4j error codes for transient write conflicts / deadlocks
        code = getattr(exc, "code", "") or ""
        if isinstance(code, str) and code.startswith("Neo.TransientError."):
            return True
        return False


async def _sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


_MAX_FIELD_SIZE = 1_000_000  # 1 MB per field


def _normalize_task_field(key: str, value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and len(value) > _MAX_FIELD_SIZE:
        raise ValueError(
            f"Field '{key}' exceeds maximum size ({len(value)} > {_MAX_FIELD_SIZE} chars)"
        )
    if key in {"task_type", "status", "role", "priority"} and isinstance(value, str):
        return value.strip().upper()
    return value
