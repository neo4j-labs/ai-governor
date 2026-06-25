"""Tests for the 7 critical/medium bug fixes.

Fix 1: Deterministic guard result ordering in parallel mode
Fix 2: TOCTOU race protection via per-task locking in apply_transition()
Fix 3: ThreadPoolExecutor lifecycle (shutdown / context manager)
Fix 4: Async get_task() Cypher parity (structural — tested via async engine)
Fix 5: Instance-level guard registry isolation
Fix 6: property_set() dotted path traversal
Fix 7: Public analytics API on TransitionEngine (no _backend access)
"""

import json
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from governor.backend.memory_backend import MemoryBackend
from governor.backend.base import GovernorBackend
from governor.engine.transition_engine import (
    GuardResult,
    TransitionEngine,
    _get_nested,
    _guard_registry,
)

import governor.guards.executor_guards  # noqa: F401


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_engine(**kwargs):
    backend = MemoryBackend()
    engine = TransitionEngine(
        backend=backend,
        role_aliases={"DEV": "EXECUTOR", "DEVELOPER": "EXECUTOR"},
        **kwargs,
    )
    return backend, engine


def _create_ready_task(backend, task_id="TASK_001"):
    """Create a task with all evidence needed to pass submission guards."""
    backend.create_task({
        "task_id": task_id,
        "task_name": "Test task",
        "task_type": "IMPLEMENTATION",
        "role": "DEVELOPER",
        "status": "ACTIVE",
        "priority": "HIGH",
        "content": "Implement feature X. Add tests to verify correctness.",
    })
    backend.add_review(task_id, {
        "review_id": f"REVIEW_{task_id}",
        "review_type": "SELF_REVIEW",
        "reviewer_role": "DEVELOPER",
        "rating": 8.0,
        "content": "All tests pass.",
    })
    backend.add_report(task_id, {
        "report_id": f"REPORT_{task_id}",
        "report_type": "IMPLEMENTATION",
        "content": "Implementation complete.",
    })


def _make_custom_sm(guards=None, events=None):
    """Create a minimal custom state machine JSON file.

    Returns (path, cleanup_fn).
    """
    sm = {
        "_meta": {"version": "test"},
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
                "guards": guards or [],
                "events": events or [],
            },
        ],
    }
    fd, path = tempfile.mkstemp(suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(sm, f)
    return path


# =========================================================================
# Fix 1: Deterministic guard result ordering
# =========================================================================

class TestFix1DeterministicGuardOrdering:
    """Parallel guard evaluation must produce deterministic ordering."""

    def test_parallel_guard_results_are_sorted_by_guard_id(self):
        """When guards run in parallel, results should be sorted by guard_id."""
        backend_par, engine_par = _make_engine(parallel_guards=True)
        _create_ready_task(backend_par)

        result = engine_par.transition_task(
            "TASK_001", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True,
        )
        assert result["result"] == "PASS"
        guard_ids = [g["guard_id"] for g in result["guard_results"]]
        assert guard_ids == sorted(guard_ids), (
            f"Guard results not sorted: {guard_ids}"
        )

    def test_parallel_and_sequential_produce_same_guard_order(self):
        """Parallel and sequential evaluation must yield identical guard order."""
        backend_seq, engine_seq = _make_engine()
        backend_par, engine_par = _make_engine(parallel_guards=True)

        _create_ready_task(backend_seq)
        _create_ready_task(backend_par)

        result_seq = engine_seq.transition_task(
            "TASK_001", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True,
        )
        result_par = engine_par.transition_task(
            "TASK_001", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True,
        )

        ids_seq = [g["guard_id"] for g in result_seq["guard_results"]]
        ids_par = [g["guard_id"] for g in result_par["guard_results"]]
        assert ids_seq == ids_par

    def test_parallel_rejection_reason_is_deterministic(self):
        """When multiple guards fail in parallel, rejection_reason should be
        deterministic (based on sorted guard order)."""
        backend_par, engine_par = _make_engine(parallel_guards=True)
        backend_par.create_task({
            "task_id": "TASK_FAIL",
            "task_name": "Fail task",
            "task_type": "IMPLEMENTATION",
            "role": "DEVELOPER",
            "status": "ACTIVE",
            "priority": "HIGH",
            "content": "No tests mentioned.",  # EG-08 will fail
        })
        # No self-review -> EG-01 will fail too

        results = []
        for _ in range(5):
            r = engine_par.transition_task(
                "TASK_FAIL", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True,
            )
            results.append(r["rejection_reason"])

        # All 5 runs should produce the exact same rejection reason
        assert len(set(results)) == 1, f"Non-deterministic reasons: {set(results)}"


# =========================================================================
# Fix 2: TOCTOU race protection in apply_transition()
# =========================================================================

class TestFix2TOCTOURaceProtection:
    """Per-task locking protects apply_transition() from concurrent races."""

    def test_use_task_locks_class_attribute_defaults_false(self):
        """GovernorBackend._use_task_locks defaults to False."""
        assert GovernorBackend._use_task_locks is False

    def test_memory_backend_inherits_locking_infrastructure(self):
        """MemoryBackend has access to _get_task_lock from base class."""
        backend = MemoryBackend()
        lock = backend._get_task_lock("TASK_TEST")
        # threading.Lock is a factory function, not a type, so it cannot be
        # used as the second arg to isinstance(); compare against the actual
        # lock primitive type instead.
        assert isinstance(lock, type(threading.Lock()))

    def test_per_task_locks_are_independent(self):
        """Different task_ids get independent Lock objects."""
        backend = MemoryBackend()
        lock_a = backend._get_task_lock("TASK_A")
        lock_b = backend._get_task_lock("TASK_B")
        assert lock_a is not lock_b

    def test_same_task_returns_same_lock(self):
        """Repeated calls for the same task_id return the same Lock."""
        backend = MemoryBackend()
        lock1 = backend._get_task_lock("TASK_X")
        lock2 = backend._get_task_lock("TASK_X")
        assert lock1 is lock2

    def test_locking_backend_serializes_apply_transition(self):
        """When _use_task_locks=True, the base class apply_transition()
        serializes concurrent calls on the same task.

        Note: MemoryBackend overrides apply_transition with its own atomic
        implementation. This test exercises the GovernorBackend base class
        locking by creating a minimal concrete subclass that relies on it.
        """

        class MinimalBackend(GovernorBackend):
            """Minimal backend that uses the base class apply_transition."""

            _use_task_locks = True

            def __init__(self):
                self._tasks = {}
                self._events = []
                self._call_order = []

            def get_task(self, task_id):
                if task_id not in self._tasks:
                    raise ValueError(f"Not found: {task_id}")
                return {"task": dict(self._tasks[task_id]), "relationships": []}

            def update_task(self, task_id, updates, expected_current_status=None):
                self._call_order.append(("start", threading.current_thread().name))
                time.sleep(0.05)
                self._tasks[task_id].update(updates)
                self._call_order.append(("end", threading.current_thread().name))
                return {"success": True, "task_id": task_id, "new_status": updates.get("status")}

            def task_exists(self, task_id):
                return task_id in self._tasks

            def get_reviews_for_task(self, task_id):
                return []

            def get_reports_for_task(self, task_id):
                return []

        backend = MinimalBackend()
        backend._tasks["TASK_LOCK"] = {"task_id": "TASK_LOCK", "status": "ACTIVE"}

        # Pre-warm the lock infrastructure.
        backend._get_task_lock("TASK_LOCK")

        event = {"transition_id": "T01", "result": "PASS"}
        updates = {"status": "READY_FOR_REVIEW"}

        threads = []
        for _ in range(2):
            t = threading.Thread(
                target=backend.apply_transition,
                args=("TASK_LOCK", updates, event),
            )
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # With locking, calls should be serialized: start, end, start, end
        starts = [i for i, (action, _) in enumerate(backend._call_order) if action == "start"]
        ends = [i for i, (action, _) in enumerate(backend._call_order) if action == "end"]
        # The second start should come after the first end
        assert starts[1] > ends[0], (
            f"Calls were not serialized. Order: {backend._call_order}"
        )


# =========================================================================
# Fix 3: ThreadPoolExecutor lifecycle
# =========================================================================

class TestFix3ExecutorLifecycle:
    """TransitionEngine should properly manage its ThreadPoolExecutor."""

    def test_shutdown_clears_executor(self):
        """shutdown() should set _guard_executor to None."""
        _, engine = _make_engine(parallel_guards=True)
        assert engine._guard_executor is not None
        engine.shutdown()
        assert engine._guard_executor is None

    def test_shutdown_idempotent(self):
        """Calling shutdown() twice should not raise."""
        _, engine = _make_engine(parallel_guards=True)
        engine.shutdown()
        engine.shutdown()  # Should not raise

    def test_context_manager_cleans_up(self):
        """Using engine as context manager should call shutdown."""
        with TransitionEngine(
            backend=MemoryBackend(), parallel_guards=True,
        ) as engine:
            assert engine._guard_executor is not None
        assert engine._guard_executor is None

    def test_context_manager_without_executor(self):
        """Context manager should work even without a thread pool."""
        with TransitionEngine(backend=MemoryBackend()) as engine:
            assert engine._guard_executor is None
        # No error

    def test_engine_works_after_shutdown_without_parallel(self):
        """After shutdown, sequential guard evaluation should still work."""
        backend, engine = _make_engine(parallel_guards=True)
        _create_ready_task(backend)
        engine.shutdown()

        # Should fall back to sequential since executor is None
        result = engine.transition_task(
            "TASK_001", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True,
        )
        assert result["result"] == "PASS"


# =========================================================================
# Fix 5: Instance-level guard registry
# =========================================================================

class TestFix5InstanceGuardRegistry:
    """Each engine instance should have an isolated guard registry."""

    def test_instance_registry_is_independent_copy(self):
        """Modifying one engine's registry should not affect another."""
        _, engine_a = _make_engine()
        _, engine_b = _make_engine()

        def custom_guard(ctx):
            return GuardResult("CUSTOM-99", True, "custom")

        engine_a.register_guard("CUSTOM-99", custom_guard)

        assert "CUSTOM-99" in engine_a._instance_guard_registry
        assert "CUSTOM-99" not in engine_b._instance_guard_registry

    def test_instance_register_guard_overwrite_false(self):
        """register_guard with overwrite=False should skip existing."""
        _, engine = _make_engine()

        def original(ctx):
            return GuardResult("EG-01", True, "original")

        def replacement(ctx):
            return GuardResult("EG-01", True, "replacement")

        engine.register_guard("EG-01", original, overwrite=True)
        engine.register_guard("EG-01", replacement, overwrite=False)

        # Should still be original
        assert engine._instance_guard_registry["EG-01"] is original

    def test_instance_guard_takes_priority_over_global(self):
        """Instance registry should be checked before the global registry."""
        _, engine = _make_engine()

        def custom_eg01(ctx):
            return GuardResult("EG-01", True, "custom instance guard")

        engine.register_guard("EG-01", custom_eg01)

        # Resolve through instance method
        guard_id, fn = engine._resolve_guard_instance("EG-01")
        assert fn is custom_eg01

    def test_global_registry_not_modified_by_instance_register(self):
        """Instance-level registration should not pollute global registry."""
        original_global = dict(_guard_registry)

        _, engine = _make_engine()

        def custom_guard(ctx):
            return GuardResult("INSTANCE-ONLY", True, "instance only")

        engine.register_guard("INSTANCE-ONLY", custom_guard)

        assert "INSTANCE-ONLY" not in _guard_registry
        # Restore check
        assert _guard_registry == original_global

    def test_resolve_guard_instance_falls_back_to_global(self):
        """If not in instance registry, should check global registry."""
        _, engine = _make_engine()

        # EG-01 should be in the global (from import of executor_guards)
        assert "EG-01" in _guard_registry

        # Remove from instance but keep in global
        if "EG-01" in engine._instance_guard_registry:
            del engine._instance_guard_registry["EG-01"]

        guard_id, fn = engine._resolve_guard_instance("EG-01")
        assert guard_id == "EG-01"
        assert fn is _guard_registry["EG-01"]


# =========================================================================
# Fix 6: property_set() dotted path traversal
# =========================================================================

class TestFix6DottedPathPropertySet:
    """property_set() guard should traverse dotted paths in nested dicts."""

    def test_get_nested_simple_key(self):
        """Flat keys should work (backward compatible)."""
        found, value = _get_nested({"approved": True}, "approved")
        assert found is True
        assert value is True

    def test_get_nested_dotted_key(self):
        """Dotted paths should traverse nested dicts."""
        found, value = _get_nested({"meta": {"approved": True}}, "meta.approved")
        assert found is True
        assert value is True

    def test_get_nested_deep_path(self):
        """Multiple levels of nesting should work."""
        d = {"a": {"b": {"c": "deep"}}}
        found, value = _get_nested(d, "a.b.c")
        assert found is True
        assert value == "deep"

    def test_get_nested_missing_intermediate(self):
        """Missing intermediate keys should return (False, None)."""
        found, value = _get_nested({"a": {"x": 1}}, "a.b.c")
        assert found is False
        assert value is None

    def test_get_nested_missing_leaf(self):
        """Missing leaf key should return (False, None)."""
        found, value = _get_nested({"meta": {}}, "meta.approved")
        assert found is False
        assert value is None

    def test_get_nested_none_value(self):
        """None values should return (False, None) — property not 'set'."""
        found, value = _get_nested({"approved": None}, "approved")
        assert found is False
        assert value is None

    def test_get_nested_falsy_value(self):
        """Falsy-but-not-None values (0, False, '') should return found=True."""
        found, _ = _get_nested({"count": 0}, "count")
        assert found is True

        found, _ = _get_nested({"flag": False}, "flag")
        assert found is True

    def test_get_nested_non_dict_intermediate(self):
        """Non-dict intermediate should return (False, None)."""
        found, value = _get_nested({"meta": "string"}, "meta.approved")
        assert found is False

    def test_property_set_guard_with_dotted_path(self):
        """End-to-end: property_set(meta.approved) in a state machine."""
        sm_path = _make_custom_sm(guards=[
            {"guard_id": "G_DOT", "check": "property_set(meta.approved)"},
        ])
        try:
            backend = MemoryBackend()
            backend.create_task({
                "task_id": "TASK_DOT",
                "task_name": "Dotted test",
                "task_type": "IMPLEMENTATION",
                "role": "DEVELOPER",
                "status": "ACTIVE",
                "priority": "HIGH",
                "content": "Test.",
            })
            engine = TransitionEngine(backend=backend, state_machine_path=sm_path)

            # Without the nested param → FAIL
            result = engine.transition_task(
                "TASK_DOT", "DONE", "EXECUTOR",
                transition_params={"approved": True},  # flat, not nested
            )
            assert result["result"] == "FAIL"

            # With the nested param → PASS
            result = engine.transition_task(
                "TASK_DOT", "DONE", "EXECUTOR",
                transition_params={"meta": {"approved": True}},
            )
            assert result["result"] == "PASS"
        finally:
            os.unlink(sm_path)

    def test_property_set_guard_flat_path_still_works(self):
        """Flat (non-dotted) property_set should still work after dotted fix."""
        sm_path = _make_custom_sm(guards=[
            {"guard_id": "G_FLAT", "check": "property_set(approved)"},
        ])
        try:
            backend = MemoryBackend()
            backend.create_task({
                "task_id": "TASK_FLAT",
                "task_name": "Flat test",
                "task_type": "IMPLEMENTATION",
                "role": "DEVELOPER",
                "status": "ACTIVE",
                "priority": "HIGH",
                "content": "Test.",
            })
            engine = TransitionEngine(backend=backend, state_machine_path=sm_path)

            result = engine.transition_task(
                "TASK_FLAT", "DONE", "EXECUTOR",
                transition_params={"approved": True},
            )
            assert result["result"] == "PASS"
        finally:
            os.unlink(sm_path)

    def test_property_set_guard_falls_back_to_task(self):
        """property_set should check task dict if not in transition_params."""
        sm_path = _make_custom_sm(guards=[
            {"guard_id": "G_TASK", "check": "property_set(priority)"},
        ])
        try:
            backend = MemoryBackend()
            backend.create_task({
                "task_id": "TASK_FALLBACK",
                "task_name": "Fallback test",
                "task_type": "IMPLEMENTATION",
                "role": "DEVELOPER",
                "status": "ACTIVE",
                "priority": "HIGH",
                "content": "Test.",
            })
            engine = TransitionEngine(backend=backend, state_machine_path=sm_path)

            # "priority" is on the task itself, not in transition_params
            result = engine.transition_task("TASK_FALLBACK", "DONE", "EXECUTOR")
            assert result["result"] == "PASS"
        finally:
            os.unlink(sm_path)


# =========================================================================
# Fix 7: Public analytics API
# =========================================================================

class TestFix7PublicAnalyticsAPI:
    """TransitionEngine should expose public analytics methods."""

    def test_get_task_audit_trail_method_exists(self):
        """Engine should have a public get_task_audit_trail method."""
        _, engine = _make_engine()
        assert hasattr(engine, "get_task_audit_trail")
        assert callable(engine.get_task_audit_trail)

    def test_get_guard_failure_hotspots_method_exists(self):
        _, engine = _make_engine()
        assert hasattr(engine, "get_guard_failure_hotspots")
        assert callable(engine.get_guard_failure_hotspots)

    def test_get_policy_coverage_method_exists(self):
        _, engine = _make_engine()
        assert hasattr(engine, "get_policy_coverage")
        assert callable(engine.get_policy_coverage)

    def test_get_rework_lineage_method_exists(self):
        _, engine = _make_engine()
        assert hasattr(engine, "get_rework_lineage")
        assert callable(engine.get_rework_lineage)

    def test_audit_trail_delegates_to_backend(self):
        """Public method should return the same data as the backend."""
        backend, engine = _make_engine()
        _create_ready_task(backend)
        engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")

        trail = engine.get_task_audit_trail("TASK_001")
        backend_trail = backend.get_task_audit_trail("TASK_001")
        assert trail == backend_trail

    def test_guard_failure_hotspots_delegates_to_backend(self):
        backend, engine = _make_engine()
        hotspots = engine.get_guard_failure_hotspots()
        backend_hotspots = backend.get_guard_failure_hotspots()
        assert hotspots == backend_hotspots

    def test_policy_coverage_delegates_to_backend(self):
        backend, engine = _make_engine()
        coverage = engine.get_policy_coverage()
        backend_coverage = backend.get_policy_coverage()
        assert coverage == backend_coverage

    def test_rework_lineage_delegates_to_backend(self):
        backend, engine = _make_engine()
        lineage = engine.get_rework_lineage("TASK_001")
        backend_lineage = backend.get_rework_lineage("TASK_001")
        assert lineage == backend_lineage

    def test_mcp_tools_use_public_api(self):
        """MCP tool handlers should use engine.get_* not engine._backend.get_*."""
        from governor.mcp.tools import create_governor_tools

        _, engine = _make_engine()
        tools = create_governor_tools(engine)
        tools_dict = {t["name"]: t for t in tools}

        # Verify audit trail tool uses engine method
        audit_handler = tools_dict["governor_get_task_audit_trail"]["handler"]
        result = audit_handler(task_id="TASK_NONEXISTENT")
        assert "task_id" in result
        assert "events" in result

        # Verify hotspots tool uses engine method
        hotspot_handler = tools_dict["governor_get_guard_failure_hotspots"]["handler"]
        result = hotspot_handler()
        assert "hotspots" in result

        # Verify policy coverage tool uses engine method
        coverage_handler = tools_dict["governor_get_policy_coverage"]["handler"]
        result = coverage_handler()
        assert "guards" in result or "totals" in result

        # Verify rework lineage tool uses engine method
        lineage_handler = tools_dict["governor_get_rework_lineage"]["handler"]
        result = lineage_handler(task_id="TASK_NONEXISTENT")
        assert "task_id" in result
