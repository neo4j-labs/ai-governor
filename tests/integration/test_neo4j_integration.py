"""Integration tests for Neo4jBackend against a real Neo4j instance.

These tests exercise the full backend lifecycle — schema creation, task CRUD,
transition events, guard evaluation persistence, and audit trail queries.

Requires GOVERNOR_NEO4J_URI to be set. See conftest.py for details.
"""

import uuid
from datetime import datetime, timezone

import pytest

from tests.integration.conftest import requires_neo4j


def _unique_id(prefix: str = "TASK") -> str:
    """Generate a unique task/event ID to avoid collisions across test runs."""
    return f"{prefix}_TEST_{uuid.uuid4().hex[:12]}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_neo4j
class TestSchemaIdempotent:
    """Calling ensure_schema() multiple times must not fail or duplicate indexes."""

    def test_schema_idempotent(self, neo4j_backend):
        # First call happened in the fixture. Call again — should be a no-op.
        neo4j_backend.ensure_schema()

        # A third time for good measure.
        neo4j_backend.ensure_schema()

        # If we get here without an exception, the schema is idempotent.


@requires_neo4j
class TestCreateAndGetTask:
    """Round-trip: create a task and read it back."""

    def test_create_and_get_task(self, neo4j_backend):
        task_id = _unique_id()
        task_data = {
            "task_id": task_id,
            "task_name": "Integration test task",
            "task_type": "IMPLEMENTATION",
            "role": "DEVELOPER",
            "status": "ACTIVE",
            "priority": "HIGH",
            "content": "Test content for integration test.",
        }

        neo4j_backend.create_task(task_data)

        # get_task returns a nested envelope: {"task": {...}, "relationships": [...]}
        retrieved = neo4j_backend.get_task(task_id)
        assert retrieved is not None
        task = retrieved["task"]
        assert task["task_id"] == task_id
        assert task["task_name"] == "Integration test task"
        assert task["status"] == "ACTIVE"
        assert task["task_type"] == "IMPLEMENTATION"
        assert task["role"] == "DEVELOPER"
        assert task["priority"] == "HIGH"

    def test_get_nonexistent_task_raises(self, neo4j_backend):
        # The backend contract raises ValueError for a missing task (matching
        # MemoryBackend and what the transition engine relies on), rather than
        # returning None.
        with pytest.raises(ValueError):
            neo4j_backend.get_task("TASK_DOES_NOT_EXIST_99999")


@requires_neo4j
class TestFullLifecycle:
    """Drive a task through ACTIVE -> READY_FOR_REVIEW -> COMPLETED."""

    def test_full_lifecycle(self, neo4j_backend):
        task_id = _unique_id()

        # 1. Create task in ACTIVE state
        neo4j_backend.create_task({
            "task_id": task_id,
            "task_name": "Lifecycle test",
            "task_type": "IMPLEMENTATION",
            "role": "DEVELOPER",
            "status": "ACTIVE",
            "priority": "MEDIUM",
            "content": "Full lifecycle integration test.",
        })

        task = neo4j_backend.get_task(task_id)["task"]
        assert task["status"] == "ACTIVE"

        # 2. Transition to READY_FOR_REVIEW
        neo4j_backend.update_task(
            task_id,
            {"status": "READY_FOR_REVIEW", "submitted_date": datetime.now(timezone.utc).isoformat()},
            expected_current_status="ACTIVE",
        )

        task = neo4j_backend.get_task(task_id)["task"]
        assert task["status"] == "READY_FOR_REVIEW"

        # 3. Transition to COMPLETED
        neo4j_backend.update_task(
            task_id,
            {"status": "COMPLETED", "completed_date": datetime.now(timezone.utc).isoformat()},
            expected_current_status="READY_FOR_REVIEW",
        )

        task = neo4j_backend.get_task(task_id)["task"]
        assert task["status"] == "COMPLETED"


@requires_neo4j
class TestAuditTrailOrdering:
    """Transition events must be returned in chronological order."""

    def test_audit_trail_ordering(self, neo4j_backend):
        task_id = _unique_id()

        neo4j_backend.create_task({
            "task_id": task_id,
            "task_name": "Audit trail test",
            "task_type": "INVESTIGATION",
            "role": "ANALYST",
            "status": "ACTIVE",
            "priority": "LOW",
            "content": "Testing audit trail ordering.",
        })

        # Record multiple transition events in order
        events = [
            {
                "event_id": _unique_id("EVT"),
                "task_id": task_id,
                "transition_id": "T01",
                "from_state": "ACTIVE",
                "to_state": "READY_FOR_REVIEW",
                "calling_role": "ANALYST",
                "result": "PASS",
                "dry_run": False,
                "occurred_at": "2026-03-01T10:00:00Z",
                "guard_results": [],
            },
            {
                "event_id": _unique_id("EVT"),
                "task_id": task_id,
                "transition_id": "T03",
                "from_state": "READY_FOR_REVIEW",
                "to_state": "REWORK",
                "calling_role": "REVIEWER",
                "result": "PASS",
                "dry_run": False,
                "occurred_at": "2026-03-01T11:00:00Z",
                "guard_results": [],
            },
            {
                "event_id": _unique_id("EVT"),
                "task_id": task_id,
                "transition_id": "T04",
                "from_state": "REWORK",
                "to_state": "READY_FOR_REVIEW",
                "calling_role": "ANALYST",
                "result": "PASS",
                "dry_run": False,
                "occurred_at": "2026-03-01T12:00:00Z",
                "guard_results": [],
            },
        ]

        for event in events:
            neo4j_backend.record_transition_event(event)

        # Retrieve audit trail
        trail = neo4j_backend.get_task_audit_trail(task_id)
        assert len(trail) >= 3

        # The audit trail is returned newest-first (ORDER BY occurred_at DESC),
        # matching MemoryBackend and the engine's expectations.
        timestamps = [e["occurred_at"] for e in trail]
        assert timestamps == sorted(timestamps, reverse=True), \
            "Audit trail must be in reverse-chronological order (newest first)"

        # Verify transition IDs are correct (newest first)
        transition_ids = [e["transition_id"] for e in trail[:3]]
        assert transition_ids == ["T04", "T03", "T01"]


@requires_neo4j
class TestGuardEvaluationPersisted:
    """Guard evaluations attached to transition events must round-trip."""

    def test_guard_evaluation_persisted(self, neo4j_backend):
        task_id = _unique_id()

        neo4j_backend.create_task({
            "task_id": task_id,
            "task_name": "Guard eval test",
            "task_type": "DEPLOY",
            "role": "SRE",
            "status": "ACTIVE",
            "priority": "CRITICAL",
            "content": "Testing guard evaluation persistence.",
        })

        event_id = _unique_id("EVT")
        guard_evals = [
            {
                "evaluation_id": _unique_id("GE"),
                "guard_id": "EG-01",
                "passed": True,
                "reason": "Self-review exists",
                "fix_hint": "",
                "warning": False,
            },
            {
                "evaluation_id": _unique_id("GE"),
                "guard_id": "EG-06",
                "passed": False,
                "reason": "No rollback plan found",
                "fix_hint": "Add a rollback plan to your deploy report",
                "warning": False,
            },
            {
                "evaluation_id": _unique_id("GE"),
                "guard_id": "EG-05",
                "passed": True,
                "reason": "No secrets detected",
                "fix_hint": "",
                "warning": True,
            },
        ]

        neo4j_backend.record_transition_event({
            "event_id": event_id,
            "task_id": task_id,
            "transition_id": "T01",
            "from_state": "ACTIVE",
            "to_state": "READY_FOR_REVIEW",
            "calling_role": "SRE",
            "result": "FAIL",
            "dry_run": False,
            "occurred_at": "2026-03-01T09:00:00Z",
            "guard_results": guard_evals,
        })

        # Retrieve the audit trail and check guard evaluations
        trail = neo4j_backend.get_task_audit_trail(task_id)
        assert len(trail) >= 1

        event = trail[0]
        assert event["result"] == "FAIL"
        assert event["transition_id"] == "T01"

        # Check that guard evaluations are attached
        persisted_evals = event.get("guard_results", [])
        assert len(persisted_evals) == 3

        # Verify specific guard results
        guard_ids = {ge["guard_id"] for ge in persisted_evals}
        assert "EG-01" in guard_ids
        assert "EG-06" in guard_ids
        assert "EG-05" in guard_ids

        # Verify the failed guard
        eg06 = next(ge for ge in persisted_evals if ge["guard_id"] == "EG-06")
        assert eg06["passed"] is False
        assert "rollback" in eg06["reason"].lower()
        assert eg06["fix_hint"] != ""

        # Verify the warning guard
        eg05 = next(ge for ge in persisted_evals if ge["guard_id"] == "EG-05")
        assert eg05["passed"] is True
        assert eg05["warning"] is True
