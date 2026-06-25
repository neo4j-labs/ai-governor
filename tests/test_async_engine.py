"""Tests for AsyncTransitionEngine."""

from __future__ import annotations

import copy
from typing import Any, Dict, List

import pytest

from governor.backend.async_base import AsyncGovernorBackend
from governor.engine.async_engine import AsyncTransitionEngine


class AsyncMemoryBackend(AsyncGovernorBackend):
    """Thin async wrapper around dict storage for testing."""

    def __init__(self) -> None:
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._reviews: Dict[str, List[Dict[str, Any]]] = {}
        self._reports: Dict[str, List[Dict[str, Any]]] = {}

    def create_task(self, task_data: Dict[str, Any]) -> None:
        self._tasks[task_data["task_id"]] = {**task_data}

    def add_review(self, task_id: str, review: Dict[str, Any]) -> None:
        self._reviews.setdefault(task_id, []).append(review)

    def add_report(self, task_id: str, report: Dict[str, Any]) -> None:
        self._reports.setdefault(task_id, []).append(report)

    async def get_task(self, task_id: str) -> Dict[str, Any]:
        if task_id not in self._tasks:
            raise ValueError(f"Task not found: {task_id}")
        task = copy.deepcopy(self._tasks[task_id])
        rels: List[Dict[str, Any]] = []
        for r in self._reviews.get(task_id, []):
            rels.append({"type": "HAS_REVIEW", "node": r, "node_labels": ["Review"]})
        for r in self._reports.get(task_id, []):
            rels.append({"type": "REPORTS_ON", "node": r, "node_labels": ["Report"]})
        return {"task": task, "relationships": rels}

    async def update_task(
        self,
        task_id: str,
        updates: Dict[str, Any],
        expected_current_status: str | None = None,
    ) -> Dict[str, Any]:
        if task_id not in self._tasks:
            raise ValueError(f"Task not found: {task_id}")
        if expected_current_status is not None and self._tasks[task_id].get("status") != expected_current_status:
            return {
                "success": False,
                "error_code": "STATE_CONFLICT",
                "actual_current_status": self._tasks[task_id].get("status"),
            }
        for k, v in updates.items():
            if v is None:
                self._tasks[task_id].pop(k, None)
            else:
                self._tasks[task_id][k] = v
        return {"success": True, "task_id": task_id, "new_status": self._tasks[task_id].get("status")}

    async def task_exists(self, task_id: str) -> bool:
        return task_id in self._tasks

    async def get_reviews_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        return copy.deepcopy(self._reviews.get(task_id, []))

    async def get_reports_for_task(self, task_id: str) -> List[Dict[str, Any]]:
        return copy.deepcopy(self._reports.get(task_id, []))

    async def record_transition_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        return {"success": True, "event_id": "EVT_ASYNC_TEST"}


def _make_async_engine():
    backend = AsyncMemoryBackend()
    engine = AsyncTransitionEngine(backend=backend)
    return engine, backend


def _create_active_task(backend, task_id="TASK_001"):
    backend.create_task({
        "task_id": task_id,
        "task_name": "Test Task",
        "task_type": "IMPLEMENTATION",
        "role": "DEVELOPER",
        "status": "ACTIVE",
        "priority": "HIGH",
        "content": "Implement feature. Tests verify correctness.",
    })
    backend.add_review(task_id, {"review_type": "SELF_REVIEW", "rating": 8.0})
    backend.add_report(task_id, {"report_type": "IMPLEMENTATION", "content": "Done."})


class TestAsyncTransitionEngine:

    @pytest.mark.asyncio
    async def test_submit_active_task(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "PASS"
        assert result["to_state"] == "READY_FOR_REVIEW"
        assert result["guard_results"]

    @pytest.mark.asyncio
    async def test_state_machine_version_is_exposed(self):
        engine, _ = _make_async_engine()
        assert isinstance(engine.state_machine_version, str)
        assert len(engine.state_machine_version) > 0

    @pytest.mark.asyncio
    async def test_dry_run(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True)
        assert result["result"] == "PASS"
        assert result["dry_run"] is True
        task = await backend.get_task("TASK_001")
        assert task["task"]["status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_full_lifecycle(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)

        r1 = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
        assert r1["result"] == "PASS"

        r2 = await engine.transition_task("TASK_001", "COMPLETED", "REVIEWER")
        assert r2["result"] == "PASS"

        task = await backend.get_task("TASK_001")
        assert task["task"]["status"] == "COMPLETED"

    @pytest.mark.asyncio
    async def test_task_not_found(self):
        engine, _ = _make_async_engine()
        result = await engine.transition_task("GHOST", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "FAIL"
        assert result["error_code"] == "TASK_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_get_available_transitions(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        result = await engine.get_available_transitions("TASK_001", "EXECUTOR")
        assert result["current_state"] == "ACTIVE"
        assert len(result["transitions"]) >= 1
        t01 = result["transitions"][0]
        assert t01["target_state"] == "READY_FOR_REVIEW"

    @pytest.mark.asyncio
    async def test_role_not_authorized(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "REVIEWER")
        assert result["result"] == "FAIL"
        assert result["error_code"] == "ROLE_NOT_AUTHORIZED"

    @pytest.mark.asyncio
    async def test_lowercase_role_is_normalized(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "executor")
        assert result["result"] == "PASS"

    @pytest.mark.asyncio
    async def test_lowercase_target_state_is_normalized(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        result = await engine.transition_task("TASK_001", "ready_for_review", "EXECUTOR")
        assert result["result"] == "PASS"

    @pytest.mark.asyncio
    async def test_rate_limit_blocks_excessive_attempts(self):
        backend = AsyncMemoryBackend()
        backend.create_task(
            {
                "task_id": "TASK_RATE",
                "task_name": "Rate limit",
                "task_type": "IMPLEMENTATION",
                "role": "DEVELOPER",
                "status": "ACTIVE",
                "priority": "HIGH",
                "content": "Run tests to verify correctness.",
            }
        )
        engine = AsyncTransitionEngine(backend=backend, rate_limit=(1, 60))

        first = await engine.transition_task("TASK_RATE", "READY_FOR_REVIEW", "EXECUTOR")
        assert first["result"] == "FAIL"  # missing required guards
        second = await engine.transition_task("TASK_RATE", "READY_FOR_REVIEW", "EXECUTOR")
        assert second["result"] == "FAIL"
        assert second["error_code"] == "RATE_LIMITED"

    @pytest.mark.asyncio
    async def test_available_transitions_exposes_warning_guards(self):
        engine, backend = _make_async_engine()
        _create_active_task(backend)
        # Remove report so EG-02 warning path is visible.
        backend._reports["TASK_001"] = []
        result = await engine.get_available_transitions("TASK_001", "EXECUTOR")
        t01 = next(t for t in result["transitions"] if t["target_state"] == "READY_FOR_REVIEW")
        assert t01["ready"] is True
        assert t01["warnings_count"] >= 1
        warning_ids = {w["guard_id"] for w in t01["guard_warnings"]}
        assert "EG-02" in warning_ids

    @pytest.mark.asyncio
    async def test_async_event_callback_is_awaited(self):
        callback_calls: List[str] = []

        async def cb(event_type, config, task_id, task, transition_params):
            callback_calls.append(f"{event_type}:{task_id}")

        custom_sm = {
            "states": {
                "ACTIVE": {"terminal": False},
                "DONE": {"terminal": True},
            },
            "transitions": [
                {
                    "id": "T01",
                    "from_state": "ACTIVE",
                    "to_state": "DONE",
                    "allowed_roles": ["EXECUTOR"],
                    "guards": [],
                    "events": [{"event_id": "E1", "type": "custom", "config": {}}],
                }
            ],
        }

        backend = AsyncMemoryBackend()
        backend.create_task(
            {
                "task_id": "TASK_EVENT_001",
                "task_name": "Event task",
                "task_type": "IMPLEMENTATION",
                "role": "DEVELOPER",
                "status": "ACTIVE",
                "priority": "HIGH",
                "content": "Test content.",
            }
        )

        import tempfile
        import json
        import os

        fd, path = tempfile.mkstemp(suffix=".json")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(custom_sm, f)
            engine = AsyncTransitionEngine(
                backend=backend, state_machine_path=path, event_callbacks=[cb]
            )
            result = await engine.transition_task("TASK_EVENT_001", "DONE", "EXECUTOR")
            assert result["result"] == "PASS"
            assert callback_calls == ["custom:TASK_EVENT_001"]
        finally:
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_state_conflict_is_normalized(self):
        class ConflictAsyncBackend(AsyncMemoryBackend):
            async def update_task(
                self,
                task_id: str,
                updates: Dict[str, Any],
                expected_current_status: str | None = None,
            ) -> Dict[str, Any]:
                return {
                    "success": False,
                    "error_code": "STATE_CONFLICT",
                    "actual_current_status": "REWORK",
                }

        backend = ConflictAsyncBackend()
        backend.create_task(
            {
                "task_id": "TASK_001",
                "task_name": "Conflict task",
                "task_type": "IMPLEMENTATION",
                "role": "DEVELOPER",
                "status": "ACTIVE",
                "priority": "HIGH",
                "content": "Has verification tests.",
            }
        )
        backend.add_review("TASK_001", {"review_type": "SELF_REVIEW"})
        backend.add_report("TASK_001", {"report_type": "IMPLEMENTATION"})

        engine = AsyncTransitionEngine(backend=backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "FAIL"
        assert result["error_code"] == "STATE_CONFLICT"

    @pytest.mark.asyncio
    async def test_backend_read_error_is_normalized(self):
        class BrokenAsyncBackend(AsyncMemoryBackend):
            async def get_task(self, task_id: str) -> Dict[str, Any]:
                raise RuntimeError("driver timeout")

        engine = AsyncTransitionEngine(backend=BrokenAsyncBackend())
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "FAIL"
        assert result["error_code"] == "BACKEND_ERROR"

        available = await engine.get_available_transitions("TASK_001", "EXECUTOR")
        assert available["error"] == "BACKEND_ERROR"

    @pytest.mark.asyncio
    async def test_atomic_apply_event_write_failure_returns_fail(self):
        class EventFailAsyncBackend(AsyncMemoryBackend):
            async def apply_transition(
                self,
                task_id: str,
                updates: Dict[str, Any],
                event: Dict[str, Any],
                expected_current_status: str | None = None,
            ) -> Dict[str, Any]:
                return {"success": False, "error_code": "EVENT_WRITE_FAILED"}

        backend = EventFailAsyncBackend()
        _create_active_task(backend)
        engine = AsyncTransitionEngine(backend=backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "FAIL"
        assert result["error_code"] == "EVENT_WRITE_FAILED"

    @pytest.mark.asyncio
    async def test_default_apply_transition_rolls_back_on_event_failure(self):
        class DefaultEventFailAsyncBackend(AsyncMemoryBackend):
            async def record_transition_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
                return {"success": False, "error_code": "EVENT_WRITE_FAILED"}

        backend = DefaultEventFailAsyncBackend()
        _create_active_task(backend, task_id="TASK_ROLLBACK")
        engine = AsyncTransitionEngine(backend=backend)
        result = await engine.transition_task("TASK_ROLLBACK", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "FAIL"
        assert result["error_code"] == "EVENT_WRITE_FAILED"
        task = await backend.get_task("TASK_ROLLBACK")
        assert task["task"]["status"] == "ACTIVE"

    @pytest.mark.asyncio
    async def test_callback_reload_failure_is_logged_not_silent(self):
        """Transition should succeed even if post-transition task reload fails (Bug 2)."""

        class ReloadFailAsyncBackend(AsyncMemoryBackend):
            def __init__(self):
                super().__init__()
                self._get_count = 0

            async def get_task(self, task_id: str) -> Dict[str, Any]:
                self._get_count += 1
                if self._get_count > 1:
                    raise RuntimeError("simulated reload failure")
                return await super().get_task(task_id)

        backend = ReloadFailAsyncBackend()
        _create_active_task(backend)
        engine = AsyncTransitionEngine(backend=backend)
        result = await engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
        assert result["result"] == "PASS"
        assert result["to_state"] == "READY_FOR_REVIEW"
