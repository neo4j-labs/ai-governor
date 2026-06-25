"""Neo4j backend for Governor — production graph database integration.

Requires the ``neo4j`` Python driver::

    pip install neo4j

Usage::

    from governor.backend.neo4j_backend import Neo4jBackend

    backend = Neo4jBackend(
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="<your-password>",
    )

Or from environment variables::

    export GOVERNOR_NEO4J_URI=neo4j://localhost:7687
    export GOVERNOR_NEO4J_USER=neo4j
    export GOVERNOR_NEO4J_PASSWORD=<your-password>

    backend = Neo4jBackend.from_env()
"""

import collections
from datetime import datetime, timezone
import logging
import os
import random
import threading
import time
from uuid import uuid4
from typing import Any, Dict, List, Optional, Tuple

from governor.backend.base import GovernorBackend

_Neo4jDriver: Any = None
try:
    from neo4j import GraphDatabase
    _Neo4jDriver = GraphDatabase
except ImportError:
    pass

logger = logging.getLogger("governor.backend.neo4j")


class _QueryRateLimiter:
    """Sliding-window rate limiter for Neo4j backend queries."""

    def __init__(self, max_queries: int, window_seconds: float) -> None:
        self._max = max(1, max_queries)
        self._window = max(0.01, window_seconds)
        self._timestamps: collections.deque = collections.deque()  # type: ignore[type-arg]
        self._lock = threading.Lock()

    def check(self) -> bool:
        """Return True if the query is allowed, False if rate-limited."""
        now = time.monotonic()
        with self._lock:
            cutoff = now - self._window
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self._max:
                return False
            self._timestamps.append(now)
            return True


def from_env(**overrides: Any) -> "Neo4jBackend":
    """Create a Neo4jBackend from environment variables.

    Reads ``GOVERNOR_NEO4J_URI``, ``GOVERNOR_NEO4J_USER``,
    ``GOVERNOR_NEO4J_PASSWORD``, and ``GOVERNOR_NEO4J_DATABASE`` from the
    environment.  Automatically loads a ``.env`` file if ``python-dotenv``
    is installed.  Keyword arguments override env vars.

    Raises:
        ValueError: If required env vars (URI, USER, PASSWORD) are missing
            and not supplied via *overrides*.
    """
    # Auto-load .env file if python-dotenv is available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    uri = overrides.pop("uri", None) or os.environ.get("GOVERNOR_NEO4J_URI")
    user = overrides.pop("user", None) or os.environ.get("GOVERNOR_NEO4J_USER")
    password = overrides.pop("password", None) or os.environ.get("GOVERNOR_NEO4J_PASSWORD")
    database = overrides.pop("database", None) or os.environ.get("GOVERNOR_NEO4J_DATABASE", "neo4j")

    missing = []
    if not uri:
        missing.append("GOVERNOR_NEO4J_URI")
    if not user:
        missing.append("GOVERNOR_NEO4J_USER")
    if not password:
        missing.append("GOVERNOR_NEO4J_PASSWORD")
    if missing:
        raise ValueError(
            f"Missing required Neo4j configuration. Set environment variable(s): "
            f"{', '.join(missing)}  — or pass them as keyword arguments."
        )

    return Neo4jBackend(uri=uri, user=user, password=password, database=database, **overrides)


class Neo4jBackend(GovernorBackend):
    """Neo4j graph database backend using the official Python driver.

    Args:
        uri: Neo4j connection URI.  Falls back to ``GOVERNOR_NEO4J_URI``.
        user: Database username.  Falls back to ``GOVERNOR_NEO4J_USER``.
        password: Database password.  Falls back to ``GOVERNOR_NEO4J_PASSWORD``.
        database: Database name.  Falls back to ``GOVERNOR_NEO4J_DATABASE``
            (default: ``neo4j``).
        query_timeout_seconds: Per-query timeout in seconds.
            ``None`` disables.  Default: ``30.0``.
        auto_schema: If True, run ``ensure_schema()`` during ``__init__``
            to create constraints and indexes automatically. Default: False.
    """

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
        verify_connectivity: bool = True,
        query_rate_limit: Optional[Tuple[int, float]] = None,
        auto_schema: bool = False,
    ) -> None:
        if _Neo4jDriver is None:
            raise ImportError(
                "The 'neo4j' package is required for Neo4jBackend. "
                "Install it with: pip install neo4j"
            )

        # Resolve from env vars when not supplied directly.
        uri = uri or os.environ.get("GOVERNOR_NEO4J_URI")
        user = user or os.environ.get("GOVERNOR_NEO4J_USER")
        password = password or os.environ.get("GOVERNOR_NEO4J_PASSWORD")
        database = database or os.environ.get("GOVERNOR_NEO4J_DATABASE", "neo4j")

        if not uri or not user or not password:
            missing = []
            if not uri:
                missing.append("uri / GOVERNOR_NEO4J_URI")
            if not user:
                missing.append("user / GOVERNOR_NEO4J_USER")
            if not password:
                missing.append("password / GOVERNOR_NEO4J_PASSWORD")
            raise ValueError(
                f"Missing required Neo4j configuration: {', '.join(missing)}. "
                "Pass as arguments or set environment variables."
            )

        self._driver = _Neo4jDriver.driver(
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
        self._query_rate_limiter: Optional[_QueryRateLimiter] = None
        if query_rate_limit is not None:
            max_queries, window_seconds = query_rate_limit
            self._query_rate_limiter = _QueryRateLimiter(max_queries, window_seconds)

        if verify_connectivity:
            try:
                self._driver.verify_connectivity()
            except Exception as exc:
                self._driver.close()
                raise ConnectionError(
                    f"Failed to connect to Neo4j at {uri}: {exc}"
                ) from exc

        if auto_schema:
            self.ensure_schema()

    def close(self) -> None:
        """Close the driver connection."""
        self._driver.close()

    def __enter__(self) -> "Neo4jBackend":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def health_check(self) -> Dict[str, Any]:
        """Return Neo4j connection health and server info."""
        try:
            info = self._driver.get_server_info()
            return {
                "healthy": True,
                "server_address": str(info.address),
                "server_version": getattr(info, "agent", "unknown"),
                "protocol_version": getattr(info, "protocol_version", None),
                "database": self._database,
            }
        except Exception as exc:
            return {"healthy": False, "error": str(exc), "database": self._database}

    def ensure_schema(self) -> Dict[str, Any]:
        """Apply Neo4j schema constraints and indexes (idempotent).

        Reads the bundled ``neo4j_schema.cypher`` file and executes each
        statement.  All statements use ``IF NOT EXISTS`` so this is safe
        to call on every startup.

        Returns:
            Dict with ``success`` and ``statements_applied`` count.
        """
        from importlib import resources as _res

        try:
            ref = _res.files("governor.schema").joinpath("neo4j_schema.cypher")
            content = ref.read_text(encoding="utf-8")
        except Exception:
            schema_path = os.path.join(
                os.path.dirname(__file__), "..", "schema", "neo4j_schema.cypher",
            )
            with open(schema_path, encoding="utf-8") as fh:
                content = fh.read()

        statements = []
        for line in content.split(";"):
            stripped = line.strip()
            # Skip empty lines and comments
            cleaned = "\n".join(
                ln for ln in stripped.splitlines()
                if ln.strip() and not ln.strip().startswith("//")
            ).strip()
            if cleaned:
                statements.append(cleaned)

        applied = 0
        errors: List[Dict[str, str]] = []
        for idx, stmt in enumerate(statements):
            try:
                self._run_write_query(stmt, {})
                applied += 1
            except Exception as e:
                errors.append({"statement_index": str(idx), "error": str(e)})
                logger.error("Schema statement %d failed: %s", idx, e)

        result: Dict[str, Any] = {
            "success": len(errors) == 0,
            "statements_applied": applied,
        }
        if errors:
            result["errors"] = errors
        return result

    # ------------------------------------------------------------------
    # GovernorBackend interface
    # ------------------------------------------------------------------

    def get_task(self, task_id: str) -> Dict[str, Any]:
        """Fetch a task and its relationships from Neo4j.

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
        records = self._run_read_query(
            query,
            {"task_id": task_id, "fetch_limit": fetch_limit},
        )

        if not records:
            raise ValueError(f"Task not found: {task_id}")

        record = records[0]
        task = record["task"]
        out_rels = record.get("out_rels") or []
        in_rels = record.get("in_rels") or []

        # Detect whether any direction was truncated, then trim to the
        # configured limit so callers always see at most relationship_limit.
        truncated = len(out_rels) > self._relationship_limit or len(in_rels) > self._relationship_limit
        out_rels = out_rels[: self._relationship_limit]
        in_rels = in_rels[: self._relationship_limit]

        relationships = [r for r in out_rels + in_rels if r.get("type") is not None]

        result: Dict[str, Any] = {"task": task, "relationships": relationships}
        if truncated:
            total_count = len(record.get("out_rels") or []) + len(record.get("in_rels") or [])
            result["relationships_truncated"] = True
            result["total_relationship_count"] = total_count
            result["relationship_limit"] = self._relationship_limit
            logger.warning(
                "Task %s has more relationships (%d) than the configured limit (%d). "
                "Increase relationship_limit for full results.",
                task_id,
                total_count,
                self._relationship_limit,
            )
        return result

    def update_task(
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

        records = self._run_write_query(query, params)
        if not records:
            if expected_current_status is not None and self.task_exists(task_id):
                current = self._run_read_query(
                    "MATCH (t:Task {task_id: $task_id}) RETURN t.status AS status",
                    {"task_id": task_id},
                )
                actual = current[0].get("status") if current else None
                return {
                    "success": False,
                    "error_code": "STATE_CONFLICT",
                    "task_id": task_id,
                    "expected_current_status": expected_current_status,
                    "actual_current_status": actual,
                }
            raise ValueError(f"Task not found during update: {task_id}")

        return {"success": True, "task_id": task_id, "new_status": records[0].get("status")}

    def apply_transition(
        self,
        task_id: str,
        updates: Dict[str, Any],
        event: Dict[str, Any],
        expected_current_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Atomically update task state and persist transition event."""
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
        WITH t, te
        CALL {{
          WITH te
          UNWIND $guard_results AS gr
          CREATE (ge:GuardEvaluation {{
            eval_id: randomUUID(),
            guard_id: gr.guard_id,
            passed: gr.passed,
            warning: coalesce(gr.warning, false),
            reason: coalesce(gr.reason, ""),
            fix_hint: coalesce(gr.fix_hint, "")
          }})
          CREATE (te)-[:HAS_GUARD_EVALUATION]->(ge)
        }}
        RETURN t.task_id AS task_id, t.status AS status, te.event_id AS event_id
        """
        records = self._run_write_query(query, params)
        if not records:
            if expected_current_status is not None and self.task_exists(task_id):
                current = self._run_read_query(
                    "MATCH (t:Task {task_id: $task_id}) RETURN t.status AS status",
                    {"task_id": task_id},
                )
                actual = current[0].get("status") if current else None
                return {
                    "success": False,
                    "error_code": "STATE_CONFLICT",
                    "task_id": task_id,
                    "expected_current_status": expected_current_status,
                    "actual_current_status": actual,
                }
            raise ValueError(f"Task not found during update: {task_id}")
        return {
            "success": True,
            "task_id": records[0].get("task_id"),
            "new_status": records[0].get("status"),
            "event_id": records[0].get("event_id"),
        }

    def task_exists(self, task_id: str) -> bool:
        query = "MATCH (t:Task {task_id: $task_id}) RETURN count(t) AS cnt"
        records = self._run_read_query(query, {"task_id": task_id})
        return bool(records and records[0].get("cnt", 0) > 0)

    def get_reviews_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        query = """
        MATCH (t:Task {task_id: $task_id})-[:HAS_REVIEW]->(r:Review)
        RETURN properties(r) AS review
        """
        records = self._run_read_query(query, {"task_id": task_id})
        return [r["review"] for r in records]

    def get_reports_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        query = """
        MATCH (r:Report)-[:REPORTS_ON]->(t:Task {task_id: $task_id})
        RETURN properties(r) AS report
        """
        records = self._run_read_query(query, {"task_id": task_id})
        return [r["report"] for r in records]

    def create_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Task node (convenience API parity with MemoryBackend).

        Uses MERGE with ON CREATE/ON MATCH to avoid TOCTOU race between
        an existence check and the CREATE. If the task already exists the
        MERGE matches the existing node and the ``already_existed`` flag
        lets us raise a clean ValueError without a separate round-trip.
        """
        task_id = str(task_data["task_id"])

        props = {k: _normalize_task_field(k, v) for k, v in task_data.items()}
        now_iso = datetime.now(timezone.utc).isoformat()
        props.setdefault("created_date", now_iso[:10])
        props.setdefault("last_updated", now_iso)

        query = """
        MERGE (t:Task {task_id: $task_id})
        ON CREATE SET t = $props
        ON MATCH SET t._merge_hit = true
        WITH t, t._merge_hit IS NOT NULL AS already_existed
        REMOVE t._merge_hit
        RETURN properties(t) AS task, already_existed
        """
        records = self._run_write_query(query, {"task_id": task_id, "props": props})
        if not records:
            raise ValueError(f"Failed to create task: {task_id}")
        if records[0].get("already_existed"):
            raise ValueError(f"Task already exists: {task_id}")
        return records[0]["task"]

    def add_review(self, task_id: str, review: Dict[str, Any]) -> None:
        """Create a Review node and link it via HAS_REVIEW."""
        if not self.task_exists(task_id):
            raise ValueError(f"Task not found: {task_id}")
        review_props = {k: v for k, v in review.items()}
        review_props.setdefault("review_id", f"review::{uuid4()}")
        query = """
        MATCH (t:Task {task_id: $task_id})
        CREATE (r:Review)
        SET r = $review
        CREATE (t)-[:HAS_REVIEW]->(r)
        """
        self._run_write_query(query, {"task_id": task_id, "review": review_props})

    def add_report(self, task_id: str, report: Dict[str, Any]) -> None:
        """Create a Report node and link it via REPORTS_ON."""
        if not self.task_exists(task_id):
            raise ValueError(f"Task not found: {task_id}")
        report_props = {k: v for k, v in report.items()}
        report_props.setdefault("report_id", f"report::{uuid4()}")
        query = """
        MATCH (t:Task {task_id: $task_id})
        CREATE (r:Report)
        SET r = $report
        CREATE (r)-[:REPORTS_ON]->(t)
        """
        self._run_write_query(query, {"task_id": task_id, "report": report_props})

    def add_handoff(self, task_id: str, handoff: Dict[str, Any]) -> None:
        """Create a Handoff node and link it via HANDOFF_TO."""
        if not self.task_exists(task_id):
            raise ValueError(f"Task not found: {task_id}")
        handoff_props = {k: v for k, v in handoff.items()}
        handoff_props.setdefault("handoff_id", f"handoff::{uuid4()}")
        query = """
        MATCH (t:Task {task_id: $task_id})
        CREATE (h:Handoff)
        SET h = $handoff
        CREATE (t)-[:HANDOFF_TO]->(h)
        """
        self._run_write_query(query, {"task_id": task_id, "handoff": handoff_props})

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a raw read-only Cypher query.

        .. deprecated::
            This method bypasses Governor's parameterization guardrails.
            Prefer typed backend methods instead.
        """
        import warnings

        warnings.warn(
            "execute_query() bypasses Governor's safety guardrails and will be "
            "removed in a future version. Use typed backend methods instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._run_read_query(query, params or {})

    def record_transition_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        query = """
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
        """
        params = {
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
        }
        records = self._run_write_query(query, params)
        if not records:
            return {"success": False, "error_code": "EVENT_WRITE_FAILED"}
        return {"success": True, "event_id": records[0].get("event_id")}

    def get_task_audit_trail(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        query = """
        MATCH (:Task {task_id: $task_id})-[:HAS_TRANSITION_EVENT]->(te:TransitionEvent)
        OPTIONAL MATCH (te)-[:HAS_GUARD_EVALUATION]->(ge:GuardEvaluation)
        WITH te, collect(properties(ge)) AS guard_results
        RETURN properties(te) AS event, guard_results
        ORDER BY te.occurred_at DESC
        LIMIT $limit
        """
        rows = self._run_read_query(query, {"task_id": task_id, "limit": max(1, int(limit))})
        return [{**row["event"], "guard_results": row.get("guard_results", [])} for row in rows]

    def get_guard_failure_hotspots(self, limit: int = 10) -> List[Dict[str, Any]]:
        query = """
        MATCH (ge:GuardEvaluation)
        RETURN ge.guard_id AS guard_id,
               count(ge) AS evaluations,
               sum(CASE WHEN ge.passed THEN 0 ELSE 1 END) AS failures
        ORDER BY failures DESC, evaluations DESC
        LIMIT $limit
        """
        return self._run_read_query(query, {"limit": max(1, int(limit))})

    def get_policy_coverage(self, since: Optional[str] = None) -> Dict[str, Any]:
        """Return guard evaluation coverage and pass/fail totals.

        Args:
            since: Optional ISO-8601 date string (e.g. ``"2026-01-01"``).
                When provided, only GuardEvaluations linked to
                TransitionEvents with ``occurred_at >= since`` are counted.
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

        guards = self._run_read_query(per_guard_query, params)
        totals_rows = self._run_read_query(totals_query, params)
        totals = totals_rows[0] if totals_rows else {"evaluations": 0, "passes": 0, "fails": 0}
        return {"guards": guards, "totals": totals}

    def get_rework_lineage(self, task_id: str) -> Dict[str, Any]:
        query = """
        MATCH (:Task {task_id: $task_id})-[:HAS_TRANSITION_EVENT]->(te:TransitionEvent)
        WHERE te.result = 'PASS'
        RETURN te.transition_id AS transition_id,
               te.from_state AS from_state,
               te.to_state AS to_state,
               te.result AS result,
               te.occurred_at AS occurred_at
        ORDER BY te.occurred_at ASC
        """
        lineage = self._run_read_query(query, {"task_id": task_id})
        rework_count = sum(1 for item in lineage if item.get("to_state") == "REWORK")
        return {"task_id": task_id, "rework_count": rework_count, "lineage": lineage}

    # ------------------------------------------------------------------
    # Retention / TTL
    # ------------------------------------------------------------------

    def purge_old_events(
        self, older_than_days: int = 90, dry_run: bool = True, batch_size: int = 500,
    ) -> Dict[str, Any]:
        """Delete TransitionEvent and GuardEvaluation nodes older than a threshold.

        Implements TTL for audit trail nodes to prevent unbounded graph growth.

        Uses a snapshot-safe two-phase approach to avoid race conditions with
        concurrent readers:

        1. **Collect phase** — A single read transaction collects event IDs
           that match the cutoff date, producing a deterministic set.
        2. **Delete phase** — Batched write transactions delete by explicit
           ``event_id``, ensuring that only the pre-identified events are
           removed (no open-ended WHERE scans during writes).

        This prevents the previous issue where a ``MATCH … LIMIT`` delete
        loop could race with concurrent reads, causing inconsistent counts
        or partial audit trails.

        Args:
            older_than_days: Delete events with ``occurred_at`` older than
                this many days ago. Default 90.
            dry_run: If True (default), count matching events but do not delete.
            batch_size: Number of events to delete per batch. Default 500.

        Returns:
            Dict with ``events_matched``, ``evaluations_matched``, and
            ``deleted`` (True only when ``dry_run=False``).
        """
        from datetime import timedelta

        days = max(1, int(older_than_days))
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()[:10]

        # Phase 1: Snapshot — collect event IDs and eval counts in a single read tx.
        # Each row represents one TransitionEvent with its count of linked
        # GuardEvaluation nodes.
        snapshot_query = """
        MATCH (te:TransitionEvent)
        WHERE te.occurred_at < $cutoff
        OPTIONAL MATCH (te)-[:HAS_GUARD_EVALUATION]->(ge:GuardEvaluation)
        RETURN te.event_id AS event_id,
               count(ge) AS eval_count
        """
        snapshot_rows = self._run_read_query(snapshot_query, {"cutoff": cutoff_date})

        event_ids = [row["event_id"] for row in snapshot_rows if row.get("event_id")]
        event_count = len(event_ids)
        eval_count = sum(row.get("eval_count", 0) for row in snapshot_rows)

        result: Dict[str, Any] = {
            "events_matched": event_count,
            "evaluations_matched": eval_count,
            "older_than_days": days,
            "cutoff_date": cutoff_date,
            "deleted": False,
            "dry_run": True,
        }

        if dry_run or event_count == 0:
            return result

        # Phase 2: Delete by explicit ID in batches (deterministic, no races)
        total_deleted = 0
        safe_batch = max(1, min(batch_size, 1000))

        for i in range(0, len(event_ids), safe_batch):
            batch_ids = event_ids[i : i + safe_batch]
            delete_query = """
            UNWIND $event_ids AS eid
            MATCH (te:TransitionEvent {event_id: eid})
            OPTIONAL MATCH (te)-[:HAS_GUARD_EVALUATION]->(ge:GuardEvaluation)
            DETACH DELETE ge
            WITH te
            DETACH DELETE te
            RETURN count(te) AS deleted_count
            """
            records = self._run_write_query(
                delete_query, {"event_ids": batch_ids}
            )
            batch_deleted = records[0].get("deleted_count", 0) if records else 0
            total_deleted += batch_deleted

        result["deleted"] = True
        result["dry_run"] = False
        result["events_deleted"] = total_deleted
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_read_query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self._run_query(query, params, mode="read")

    def _run_write_query(self, query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        return self._run_query(query, params, mode="write")

    def _run_query(self, query: str, params: Dict[str, Any], mode: str) -> List[Dict[str, Any]]:
        if self._query_rate_limiter is not None and not self._query_rate_limiter.check():
            raise RuntimeError(
                "Neo4j backend query rate limit exceeded. "
                "Reduce query volume or increase the rate limit."
            )
        attempt = 0
        while True:
            attempt += 1
            start = time.perf_counter()
            try:
                session_kwargs: Dict[str, Any] = {"database": self._database}
                with self._driver.session(**session_kwargs) as session:
                    def _run_in_tx(tx, _timeout=None):
                        run_kwargs: Dict[str, Any] = {}
                        if _timeout is not None:
                            run_kwargs["timeout"] = _timeout
                        return [dict(record) for record in tx.run(query, params, **run_kwargs)]

                    tx_kwargs: Dict[str, Any] = {}
                    if self._query_timeout_seconds is not None:
                        tx_kwargs["_timeout"] = self._query_timeout_seconds
                    if mode == "write":
                        records = session.execute_write(
                            _run_in_tx,
                            **tx_kwargs,
                        )
                    else:
                        records = session.execute_read(
                            _run_in_tx,
                            **tx_kwargs,
                        )

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
                return records
            except Exception as exc:
                neo4j_code = getattr(exc, "code", None)
                retryable = self._is_retryable(exc)
                if not retryable or attempt > self._query_max_retries:
                    if neo4j_code:
                        logger.error(
                            "Neo4j error [%s]: %s", neo4j_code, exc,
                            extra={"ctx": {"mode": mode, "neo4j_code": neo4j_code}},
                        )
                    elapsed_ms = int((time.perf_counter() - start) * 1000)
                    if self._query_observer is not None:
                        self._query_observer(
                            {
                                "mode": mode,
                                "attempt": attempt,
                                "elapsed_ms": elapsed_ms,
                                "params_keys": sorted(params.keys()),
                                "error": str(exc),
                                "neo4j_code": neo4j_code,
                            }
                        )
                    raise
                base_delay = self._query_retry_backoff_seconds * (2 ** (attempt - 1))
                jitter = random.uniform(0, base_delay * 0.5)
                sleep_seconds = base_delay + jitter
                logger.warning(
                    "Neo4j query failed (attempt %s/%s), retrying in %.3fs: %s",
                    attempt,
                    self._query_max_retries + 1,
                    sleep_seconds,
                    exc,
                    extra={"ctx": {"mode": mode, "attempt": attempt, "max_retries": self._query_max_retries + 1, "neo4j_code": neo4j_code}},
                )
                time.sleep(sleep_seconds)

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


# Allowlist of Task property names accepted in SET clauses.
# Prevents Cypher injection via crafted property names that pass
# Python's str.isidentifier() but are invalid or dangerous in Cypher.
_ALLOWED_TASK_PROPERTIES: frozenset = frozenset({
    "task_id", "task_name", "task_type", "role", "status", "priority",
    "content", "footer", "deliverables", "metadata",
    "revision_count", "submitted_date", "completed_date",
    "blocked_date", "failed_date", "blocking_reason", "failure_reason",
    "unblock_reason", "created_date", "last_updated",
    "rework_summary", "rework_asks",
})

_MAX_FIELD_SIZE = 1_000_000  # 1 MB per field


def _validate_property_name(key: str) -> None:
    """Validate that a property name is safe for Cypher SET clauses.

    Uses a strict allowlist rather than ``str.isidentifier()``, which
    accepts Python-valid identifiers that may conflict with Cypher
    reserved words or contain unexpected characters.

    Raises:
        ValueError: If the key is not in the allowlist.
    """
    if key not in _ALLOWED_TASK_PROPERTIES:
        raise ValueError(
            f"Property name '{key}' is not in the allowed set. "
            f"Allowed: {sorted(_ALLOWED_TASK_PROPERTIES)}"
        )


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
