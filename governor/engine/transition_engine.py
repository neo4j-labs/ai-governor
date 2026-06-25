"""Transition Engine — Unified Task State Machine enforcement.

Loads state_machine.json and enforces all transitions through a single API.
Guards are evaluated per-transition; all status updates go through the
backend abstraction layer.

Usage::

    from governor.backend.memory_backend import MemoryBackend
    from governor.engine.transition_engine import TransitionEngine

    engine = TransitionEngine(backend=MemoryBackend())

    # Check what transitions are possible
    available = engine.get_available_transitions(task_id="TASK_001", calling_role="EXECUTOR")

    # Dry run: check if transition would succeed
    result = engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR", dry_run=True)

    # Execute: validate, guard, transition, fire callbacks
    result = engine.transition_task("TASK_001", "READY_FOR_REVIEW", "EXECUTOR")
"""

import collections
import concurrent.futures
import json
import logging
import os
import re
import sys
import threading
import time
from importlib import import_module, reload
from importlib import resources
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple, Union

from governor.backend.base import GovernorBackend

if TYPE_CHECKING:
    from governor.backend.async_base import AsyncGovernorBackend
from governor.engine.enums import ErrorCode, TransitionResult
from governor.engine.telemetry import get_tracer
from governor.engine.validation import validate_state_machine

logger = logging.getLogger("governor.engine")


# ---------------------------------------------------------------------------
# Guard Infrastructure
# ---------------------------------------------------------------------------


class GuardContext:
    """Shared context passed to all guards within a single transition.

    Builds expensive resources (task data) once and shares them
    across all guard evaluations for the same transition.
    """

    def __init__(
        self,
        task_id: str,
        task_data: Dict[str, Any],
        transition_params: Optional[Dict[str, Any]] = None,
        backend: Optional[Union[GovernorBackend, "AsyncGovernorBackend"]] = None,
    ):
        self.task_id = task_id
        self.task = task_data["task"]
        self.relationships = task_data["relationships"]
        self.relationships_truncated = bool(task_data.get("relationships_truncated"))
        self.task_data = task_data
        self.transition_params = transition_params or {}
        self.backend = backend

        if self.relationships_truncated:
            logger.warning(
                "Task %s has truncated relationships (%s returned, more exist). "
                "Guards that depend on complete relationship data may produce "
                "inaccurate results. Increase backend relationship_limit if needed.",
                task_id,
                len(self.relationships),
            )


class GuardResult:
    """Result of a single guard evaluation.

    Guards produce binary PASS/FAIL. WARNING mode: a guard may pass with
    ``warning=True`` to indicate a non-blocking advisory.
    """

    __slots__ = ("guard_id", "passed", "reason", "fix_hint", "warning")

    def __init__(
        self,
        guard_id: str,
        passed: bool,
        reason: str = "",
        fix_hint: str = "",
        warning: bool = False,
    ):
        if not isinstance(passed, bool):
            raise TypeError(
                f"GuardResult.passed must be bool, got {type(passed).__name__}: {passed!r}"
            )
        self.guard_id = guard_id
        self.passed = passed
        self.reason = reason
        self.fix_hint = fix_hint
        self.warning = warning

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GuardResult):
            return NotImplemented
        return (
            self.guard_id == other.guard_id
            and self.passed == other.passed
            and self.reason == other.reason
            and self.fix_hint == other.fix_hint
            and self.warning == other.warning
        )

    def __repr__(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        parts = [f"GuardResult({self.guard_id!r}, {status}"]
        if self.reason:
            parts.append(f", reason={self.reason!r}")
        if self.warning:
            parts.append(", warning=True")
        parts.append(")")
        return "".join(parts)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "guard_id": self.guard_id,
            "passed": self.passed,
            "reason": self.reason,
            "fix_hint": self.fix_hint,
            "warning": self.warning,
        }


# Guard callable type: (GuardContext) -> GuardResult
GuardCallable = Callable[[GuardContext], GuardResult]


# ---------------------------------------------------------------------------
# Guard Registry — maps guard_id to callable
# ---------------------------------------------------------------------------

_guard_registry: Dict[str, GuardCallable] = {}
_guard_registry_lock = threading.Lock()
_BUILTIN_GUARD_MODULES: Dict[str, str] = {
    "EG-": "governor.guards.executor_guards",
}


def _load_state_machine(state_machine_path: Optional[str]) -> Dict[str, Any]:
    """Load state machine from an explicit path or bundled package data."""
    if state_machine_path is not None:
        with open(state_machine_path, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        bundled_path = resources.files("governor").joinpath("schema/state_machine.json")
        if bundled_path.is_file():
            with bundled_path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        # Fallback below handles editable/local source layouts.
        pass

    fallback_path = os.path.join(
        os.path.dirname(__file__), os.pardir, "schema", "state_machine.json"
    )
    with open(fallback_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _extract_guard_ids(state_machine: Dict[str, Any]) -> List[str]:
    """Collect string guard IDs referenced by transitions."""
    guard_ids: List[str] = []
    for transition in state_machine.get("transitions", []):
        for guard_ref in transition.get("guards", []):
            if isinstance(guard_ref, str):
                guard_ids.append(guard_ref)
            elif isinstance(guard_ref, dict):
                guard_id = guard_ref.get("guard_id")
                if isinstance(guard_id, str):
                    guard_ids.append(guard_id)
    return guard_ids


def _ensure_builtin_guards_loaded(state_machine: Dict[str, Any]) -> None:
    """Auto-import built-in guard modules required by the state machine.

    If the module is already imported but the guards are missing from the
    global registry (e.g. because the registry was cleared), the module is
    reloaded so that ``@register_guard`` decorators re-run.  This is safe
    because built-in guards use ``overwrite=False`` and therefore will not
    clobber user-provided overrides registered earlier.
    """
    guard_ids = _extract_guard_ids(state_machine)
    with _guard_registry_lock:
        missing = [gid for gid in guard_ids if gid not in _guard_registry]
    if not missing:
        return

    for prefix, module_name in _BUILTIN_GUARD_MODULES.items():
        if not any(gid.startswith(prefix) for gid in missing):
            continue
        try:
            if module_name in sys.modules:
                reload(sys.modules[module_name])
            else:
                import_module(module_name)
        except Exception as e:
            logger.warning(f"Failed to auto-load guard module '{module_name}': {e}", extra={"ctx": {"module": module_name}})


def register_guard(guard_id: str, *, overwrite: bool = True):
    """Decorator to register a guard callable.

    Args:
        guard_id: Guard identifier string (e.g. "EG-01", "CUSTOM-01").
        overwrite: If False, do not overwrite an existing registration for
            this guard_id. This is useful for built-in guard modules so that
            user-provided overrides registered earlier are not clobbered by
            auto-loading.
    """

    def decorator(fn: GuardCallable) -> GuardCallable:
        with _guard_registry_lock:
            if not overwrite and guard_id in _guard_registry:
                return fn
            _guard_registry[guard_id] = fn
        return fn

    return decorator


# ===========================================================================
# Property Guard Factories (inline from state machine JSON)
# ===========================================================================


def _get_nested(d: Dict[str, Any], dotted_path: str) -> Tuple[bool, Any]:
    """Walk a dotted path (e.g. ``"meta.approved"``) into a nested dict.

    Returns ``(found, value)`` where *found* is True when the full path
    resolved to a non-None value.
    """
    parts = dotted_path.split(".")
    current: Any = d
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            return (False, None)
        current = current[part]
    return (current is not None, current)


def _make_property_set_guard(guard_def: Dict[str, Any]) -> GuardCallable:
    """Create a guard for simple property_set checks defined inline in JSON.

    Supports dotted paths such as ``property_set(meta.approved)`` which
    will traverse nested dictionaries in *transition_params* and *task*.
    """
    guard_id = guard_def["guard_id"]
    check = guard_def.get("check", "")
    fix_hint = guard_def.get("fix_hint", "")

    match = re.match(r"property_set\(([a-zA-Z_][a-zA-Z0-9_.]*)\)", check)
    if not match:
        def _stub(ctx: GuardContext) -> GuardResult:
            return GuardResult(guard_id, True, f"Guard {guard_id} not implemented (non-property_set check)")
        return _stub

    prop_name = match.group(1)

    def _check(ctx: GuardContext) -> GuardResult:
        found, _ = _get_nested(ctx.transition_params, prop_name)
        if not found:
            found, _ = _get_nested(ctx.task, prop_name)

        if found:
            return GuardResult(guard_id, True, f"{prop_name} provided")
        return GuardResult(
            guard_id, False, f"Required property '{prop_name}' not provided",
            fix_hint=fix_hint,
        )
    return _check



# ---------------------------------------------------------------------------
# Guard Resolution
# ---------------------------------------------------------------------------


def _resolve_guard(guard_ref: Any, strict: bool = False) -> Tuple[str, GuardCallable]:
    """Resolve a guard reference (string ID or inline dict) to (guard_id, callable).

    Args:
        guard_ref: Guard ID string or inline guard definition dict.
        strict: If True, raise ValueError on unregistered guards instead of
            falling back to a pass-through stub.
    """
    if isinstance(guard_ref, str):
        guard_id = guard_ref
        with _guard_registry_lock:
            fn = _guard_registry.get(guard_id)
        if fn is None:
            if strict:
                raise ValueError(
                    f"Guard '{guard_id}' not found in registry. "
                    "Register it with @register_guard or import the module that defines it."
                )
            logger.warning(f"Guard {guard_id} not found in registry, returning pass-through", extra={"ctx": {"guard_id": guard_id}})
            def _passthrough(ctx: GuardContext) -> GuardResult:
                return GuardResult(guard_id, True, f"Guard {guard_id} not implemented (pass-through)")
            return guard_id, _passthrough
        return guard_id, fn

    if isinstance(guard_ref, dict):
        guard_id = guard_ref["guard_id"]
        check = guard_ref.get("check", "")

        if check.startswith("property_set("):
            return guard_id, _make_property_set_guard(guard_ref)
        else:
            if strict:
                raise ValueError(f"Unknown inline guard check: '{check}' for guard '{guard_id}'")
            logger.warning(f"Unknown inline guard check: {check}", extra={"ctx": {"guard_id": guard_id, "check": check}})
            def _stub(ctx: GuardContext) -> GuardResult:
                return GuardResult(guard_id, True, f"Inline guard '{check}' not implemented (pass-through)")
            return guard_id, _stub

    raise ValueError(f"Invalid guard reference: {guard_ref}")


# ---------------------------------------------------------------------------
# Template Rendering
# ---------------------------------------------------------------------------


def _render_template(template: str, task_id: str, task: Dict[str, Any], params: Dict[str, Any]) -> str:
    """Render a template string by replacing $variables."""
    result = template
    replacements = {
        "$task_id": task.get("task_id", task_id),
        "$task_name": task.get("task_name", task_id),
        "$task_role": task.get("role", "UNKNOWN"),
        "$task_priority": task.get("priority", "MEDIUM"),
    }
    for var, val in replacements.items():
        result = result.replace(var, str(val))
    return result


# ---------------------------------------------------------------------------
# Error Response Helper
# ---------------------------------------------------------------------------


def _error_response(error_code: str, message: str, **extra) -> Dict[str, Any]:
    """Build a standardized error response."""
    return {
        "result": TransitionResult.FAIL,
        "error_code": error_code,
        "message": message,
        "guard_results": [],
        "dry_run": False,
        "events_fired": [],
        "temporal_updates": {},
        "rejection_reason": message,
        **extra,
    }


def _normalize_state(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


# ===========================================================================
# Rate Limiter — per-task sliding-window counter
# ===========================================================================


class _RateLimiter:
    """Thread-safe sliding-window rate limiter keyed by task_id.

    Uses an OrderedDict with LRU eviction to prevent unbounded memory
    growth when many unique task IDs are seen over time.

    Args:
        max_attempts: Maximum transition attempts allowed per window.
        window_seconds: Length of the sliding window in seconds.
        max_keys: Maximum number of task_id keys to track. When exceeded,
            the least-recently-used entries are evicted. Default 10_000.
    """

    def __init__(
        self, max_attempts: int, window_seconds: float, max_keys: int = 10_000,
    ) -> None:
        self._max = max_attempts
        self._window = window_seconds
        self._max_keys = max(1, max_keys)
        self._lock = threading.Lock()
        self._attempts: collections.OrderedDict[str, collections.deque] = (
            collections.OrderedDict()
        )

    def check(self, task_id: str) -> bool:
        """Return True if the attempt is allowed, False if rate-limited."""
        now = time.monotonic()
        with self._lock:
            if task_id in self._attempts:
                # Move to end (most-recently-used)
                self._attempts.move_to_end(task_id)
                dq = self._attempts[task_id]
            else:
                # Evict least-recently-used keys BEFORE adding to maintain
                # the capacity invariant (never exceed max_keys).
                while len(self._attempts) >= self._max_keys:
                    self._attempts.popitem(last=False)
                dq = collections.deque()
                self._attempts[task_id] = dq

            # Purge entries outside the window
            cutoff = now - self._window
            while dq and dq[0] < cutoff:
                dq.popleft()
            if len(dq) >= self._max:
                return False
            dq.append(now)
            return True


# ===========================================================================
# TransitionEngine — the main class
# ===========================================================================


class TransitionEngine:
    """State machine enforcement engine.

    Args:
        backend: A ``GovernorBackend`` implementation for task persistence.
        state_machine_path: Path to the state machine JSON file.
            Defaults to bundled ``governor/schema/state_machine.json``.
        role_aliases: Optional mapping of short role names to canonical names
            used in the state machine (e.g. ``{"DEV": "EXECUTOR"}``).
        event_callbacks: Optional list of callables invoked after each
            successful (non-dry-run) transition. Each callback receives
            ``(event_type, event_config, task_id, task, transition_params)``.
        strict: If True, raise ``ValueError`` on unregistered guards instead
            of falling back to pass-through stubs. Default is True.
        guard_timeout_seconds: Per-guard execution timeout in seconds.
            ``None`` disables guard timeouts (default).  When set, a guard
            that exceeds the timeout is recorded as FAIL.

            **Timeout semantics (fail-closed):**

            - A timed-out guard produces ``GuardResult(passed=False)`` with
              reason ``"Guard timed out after Xs"``.  The transition is
              treated identically to any other guard failure — it **blocks**
              the state change (fail-closed, never fail-open).
            - All other guards still run to completion; the engine does not
              short-circuit on the first timeout so callers receive the full
              set of guard results.
            - The timed-out guard's thread (sync) or coroutine (async)
              continues to run in the background — Python cannot forcibly
              kill threads.  Ensure guards are non-blocking or use the async
              engine with ``asyncio.wait_for`` for true cancellation.
            - **Retry guidance:** A timeout does *not* automatically retry.
              Callers should inspect the ``guard_results`` list in the
              response for ``"Guard timed out"`` reasons and decide whether
              to retry the transition.  Idempotent retries are safe — the
              backend uses optimistic concurrency control.

        parallel_guards: If True, submit all guards for a transition to a
            ``ThreadPoolExecutor`` concurrently instead of evaluating them
            sequentially. Automatically enabled when ``guard_timeout_seconds``
            is set. Default is False.
        rate_limit: Optional tuple ``(max_attempts, window_seconds)`` to
            throttle transition attempts per task. For example
            ``(10, 60)`` allows at most 10 attempts per task per minute.
            ``None`` disables rate limiting (default).
    """

    def __init__(
        self,
        backend: GovernorBackend,
        state_machine_path: Optional[str] = None,
        role_aliases: Optional[Dict[str, str]] = None,
        event_callbacks: Optional[List[Callable]] = None,
        strict: bool = True,
        guard_timeout_seconds: Optional[float] = None,
        parallel_guards: bool = False,
        rate_limit: Optional[Tuple[int, float]] = None,
        guard_max_workers: int = 4,
    ) -> None:
        self._backend = backend
        self._role_aliases = role_aliases or {}
        self._strict = strict
        self._event_callbacks = event_callbacks or []
        self._guard_timeout_seconds = guard_timeout_seconds
        self._zombie_thread_count: int = 0
        self._total_timeout_count: int = 0
        self._zombie_lock = threading.Lock()
        # Default to 60s timeout when parallel guards are enabled without
        # an explicit timeout to prevent indefinite hangs.
        if parallel_guards and guard_timeout_seconds is None:
            import warnings
            self._guard_timeout_seconds = 60.0
            warnings.warn(
                "parallel_guards=True without explicit guard_timeout_seconds; "
                "defaulting to 60.0s. Set guard_timeout_seconds explicitly to "
                "silence this warning.",
                stacklevel=2,
            )
            logger.warning(
                "parallel_guards=True without explicit timeout; "
                "defaulting guard_timeout_seconds to 60.0",
            )
        self._guard_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        if self._guard_timeout_seconds is not None or parallel_guards:
            self._guard_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=max(1, guard_max_workers),
                thread_name_prefix="governor-guard",
            )

        self._rate_limiter: Optional[_RateLimiter] = None
        if rate_limit is not None:
            max_attempts, window_seconds = rate_limit
            self._rate_limiter = _RateLimiter(max_attempts, window_seconds)

        self._state_machine = _load_state_machine(state_machine_path)
        self._state_machine_version: str = (
            self._state_machine.get("_meta", {}).get("version", "unknown")
        )
        _ensure_builtin_guards_loaded(self._state_machine)

        # Instance-level guard registry: copy from global so each engine
        # is isolated from other engines' guard registrations.
        self._instance_guard_registry: Dict[str, GuardCallable] = dict(_guard_registry)

        errors = validate_state_machine(self._state_machine)
        if errors:
            raise ValueError(f"Invalid state machine: {'; '.join(errors)}")

        # Validate that all *string* guard IDs referenced by transitions
        # are registered.  In strict mode, surface missing guards at engine
        # construction time rather than deferring to the first transition
        # attempt — fail fast, fail loudly.
        #
        # Inline guard defs (dicts with "check" keys like property_set())
        # are excluded because they are resolved dynamically and don't
        # require registry entries.
        if self._strict:
            missing_guards = []
            for transition in self._state_machine.get("transitions", []):
                for guard_ref in transition.get("guards", []):
                    if isinstance(guard_ref, str) and guard_ref not in self._instance_guard_registry:
                        missing_guards.append(guard_ref)
            if missing_guards:
                raise ValueError(
                    f"strict=True: {len(missing_guards)} guard(s) referenced in "
                    f"state machine but not registered: {sorted(set(missing_guards))}. "
                    "Register them with @register_guard or import the module that "
                    "defines them before constructing the engine."
                )

        self._tracer = get_tracer()

    @property
    def state_machine_version(self) -> str:
        """Return the version string from the loaded state machine ``_meta``."""
        return self._state_machine_version

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @property
    def zombie_thread_count(self) -> int:
        """Number of guard threads that timed out and are **still running**.

        This counter increments when a guard times out and decrements when
        the orphaned thread eventually finishes. Use
        :attr:`total_timeout_count` for a monotonic counter suitable for
        metrics dashboards.

        Python cannot forcibly kill threads. When a guard exceeds
        ``guard_timeout_seconds``, the transition proceeds (fail-closed)
        but the orphaned thread continues in the background. Monitor this
        counter to detect resource leaks. For true cancellation, use
        :class:`AsyncTransitionEngine` with ``asyncio.wait_for``.
        """
        with self._zombie_lock:
            return self._zombie_thread_count

    @property
    def total_timeout_count(self) -> int:
        """Total number of guard timeouts since engine creation (monotonic).

        Unlike :attr:`zombie_thread_count` which tracks *active* orphans,
        this counter never decrements and is suitable for Prometheus-style
        monotonic counters and alerting thresholds.
        """
        with self._zombie_lock:
            return self._total_timeout_count

    def shutdown(self, wait: bool = True) -> None:
        """Shut down the guard thread pool, if any.

        After shutdown with ``wait=True``, all orphaned threads have
        finished so :attr:`zombie_thread_count` is reset to zero.

        Args:
            wait: If *True* (default), block until running guards finish.
        """
        if self._guard_executor is not None:
            self._guard_executor.shutdown(wait=wait)
            self._guard_executor = None
        if wait:
            with self._zombie_lock:
                self._zombie_thread_count = 0

    def __enter__(self) -> "TransitionEngine":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.shutdown(wait=True)

    def __del__(self) -> None:
        # Safety net: attempt non-blocking shutdown if caller forgot.
        try:
            executor = getattr(self, "_guard_executor", None)
            if executor is not None:
                executor.shutdown(wait=False)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Instance Guard Registry
    # ------------------------------------------------------------------

    def register_guard(
        self, guard_id: str, fn: GuardCallable, *, overwrite: bool = True
    ) -> None:
        """Register a guard on this engine instance.

        This does **not** affect the global registry or other engine
        instances.

        Args:
            guard_id: Guard identifier string (e.g. ``"EG-01"``).
            fn: Guard callable.
            overwrite: If *False*, skip if *guard_id* already registered.
        """
        if not overwrite and guard_id in self._instance_guard_registry:
            return
        self._instance_guard_registry[guard_id] = fn

    def _resolve_guard_instance(self, guard_ref: Any) -> Tuple[str, GuardCallable]:
        """Resolve a guard reference using the instance registry.

        Checks the instance registry first, then the global registry.
        Falls back to pass-through (non-strict) or raises (strict).
        """
        if isinstance(guard_ref, str):
            guard_id = guard_ref
            fn = self._instance_guard_registry.get(guard_id) or _guard_registry.get(guard_id)
            if fn is None:
                if self._strict:
                    raise ValueError(
                        f"Guard '{guard_id}' not found in registry. "
                        "Register it with @register_guard or engine.register_guard()."
                    )
                logger.warning(
                    f"Guard {guard_id} not found in registry, returning pass-through",
                    extra={"ctx": {"guard_id": guard_id}},
                )

                def _passthrough(ctx: GuardContext) -> GuardResult:
                    return GuardResult(guard_id, True, f"Guard {guard_id} not implemented (pass-through)")

                return guard_id, _passthrough
            return guard_id, fn

        if isinstance(guard_ref, dict):
            guard_id = guard_ref["guard_id"]
            check = guard_ref.get("check", "")
            if check.startswith("property_set("):
                return guard_id, _make_property_set_guard(guard_ref)
            if self._strict:
                raise ValueError(f"Unknown inline guard check: '{check}' for guard '{guard_id}'")
            logger.warning(
                f"Unknown inline guard check: {check}",
                extra={"ctx": {"guard_id": guard_id, "check": check}},
            )

            def _stub(ctx: GuardContext) -> GuardResult:
                return GuardResult(guard_id, True, f"Inline guard '{check}' not implemented (pass-through)")

            return guard_id, _stub

        raise ValueError(f"Invalid guard reference: {guard_ref}")

    # ------------------------------------------------------------------
    # State Machine Helpers
    # ------------------------------------------------------------------

    def _normalize_calling_role(self, calling_role: str) -> str:
        normalized = calling_role.strip().upper()
        return self._role_aliases.get(normalized, normalized)

    def _find_transition(self, from_state: str, to_state: str) -> Optional[Dict[str, Any]]:
        for t in self._state_machine["transitions"]:
            if t["from_state"] == from_state and t["to_state"] == to_state:
                return t
        return None

    def _get_all_transitions_from(self, from_state: str) -> List[Dict[str, Any]]:
        return [t for t in self._state_machine["transitions"] if t["from_state"] == from_state]

    # ------------------------------------------------------------------
    # Audit Event Persistence
    # ------------------------------------------------------------------

    def _persist_audit_event(
        self,
        event_payload: Dict[str, Any],
        task_id: str,
        transition_def: Dict[str, Any],
        *,
        max_retries: int = 2,
    ) -> Optional[str]:
        """Persist a transition audit event with retry.

        Returns *None* on success, or an error string on failure.
        """
        for attempt in range(1, max_retries + 1):
            try:
                self._backend.record_transition_event(event_payload)
                return None
            except Exception as e:
                if attempt < max_retries:
                    time.sleep(0.05 * attempt)
                    continue
                logger.warning(
                    "Failed to record transition event for task '%s' "
                    "(attempt %d/%d): %s",
                    task_id, attempt, max_retries, e,
                    extra={"ctx": {
                        "task_id": task_id,
                        "transition_id": transition_def.get("id"),
                    }},
                )
                return str(e)
        return None  # unreachable, but keeps mypy happy

    # ------------------------------------------------------------------
    # Event Dispatch (callbacks)
    # ------------------------------------------------------------------

    def _fire_events(
        self,
        transition_def: Dict[str, Any],
        task_id: str,
        task: Dict[str, Any],
        transition_params: Dict[str, Any],
    ) -> List[str]:
        """Fire post-transition event callbacks. Returns list of event IDs."""
        events_fired: List[str] = []
        for event in transition_def.get("events", []):
            event_id = event.get("event_id", "unknown")
            event_type = event.get("type")
            config = event.get("config", {})

            try:
                if event_type == "notification":
                    template = config.get("template", "")
                    rendered = _render_template(template, task_id, task, transition_params)
                    severity = config.get("severity", "INFO")
                    logger.info(f"[EVENT:{event_id}] [{severity}] {rendered}", extra={"ctx": {"event_id": event_id, "severity": severity, "task_id": task_id}})
                    events_fired.append(event_id)
                else:
                    all_ok = True
                    for callback in self._event_callbacks:
                        try:
                            callback(event_type, config, task_id, task, transition_params)
                        except Exception as cb_err:
                            logger.error(f"Event callback error for {event_id}: {cb_err}", extra={"ctx": {"event_id": event_id, "task_id": task_id}})
                            all_ok = False
                    if all_ok:
                        events_fired.append(event_id)
            except Exception as e:
                logger.error(f"Event {event_id} ({event_type}) failed: {e}", extra={"ctx": {"event_id": event_id, "event_type": event_type, "task_id": task_id}})

        return events_fired

    # ------------------------------------------------------------------
    # Guard Evaluation Helpers
    # ------------------------------------------------------------------

    def _evaluate_single_guard(
        self,
        guard_id: str,
        guard_fn: GuardCallable,
        ctx: GuardContext,
        task_id: str,
        transition_def: Dict[str, Any],
    ) -> GuardResult:
        """Run a single guard with optional timeout.

        Timeout behaviour is **fail-closed**: a guard that exceeds
        ``guard_timeout_seconds`` produces a FAIL result (never silently
        passes).  The orphaned thread continues in the background — see
        ``_evaluate_guards_parallel`` for details.
        """
        with self._tracer.start_as_current_span(f"guard.{guard_id}") as span:
            span.set_attribute("governor.guard_id", guard_id)
            span.set_attribute("governor.task_id", task_id)
            try:
                if self._guard_executor is not None and self._guard_timeout_seconds is not None:
                    future = self._guard_executor.submit(guard_fn, ctx)
                    result = future.result(timeout=self._guard_timeout_seconds)
                else:
                    result = guard_fn(ctx)
                span.set_attribute("governor.guard_passed", result.passed)
                return result
            except concurrent.futures.TimeoutError:
                logger.error(
                    "Guard %s timed out after %.1fs",
                    guard_id, self._guard_timeout_seconds,
                    extra={"ctx": {"guard_id": guard_id, "task_id": task_id, "transition_id": transition_def.get("id")}},
                )
                span.set_attribute("governor.guard_passed", False)
                return GuardResult(guard_id, False, f"Guard timed out after {self._guard_timeout_seconds}s")
            except Exception as e:
                logger.error(f"Guard {guard_id} raised exception: {e}", exc_info=True, extra={"ctx": {"guard_id": guard_id, "task_id": task_id, "transition_id": transition_def.get("id")}})
                span.record_exception(e)
                span.set_attribute("governor.guard_passed", False)
                return GuardResult(guard_id, False, f"Guard error: {e}")

    def _evaluate_guards_parallel(
        self,
        resolved_guards: List[Tuple[str, GuardCallable]],
        ctx: GuardContext,
        task_id: str,
        transition_def: Dict[str, Any],
    ) -> List[GuardResult]:
        """Submit all guards to the ThreadPoolExecutor concurrently.

        Returns results in the same order as ``resolved_guards``.

        .. note::
            Python's ``ThreadPoolExecutor`` cannot forcibly kill a running
            thread.  If a guard hangs on blocking I/O, the timeout fires
            and the transition completes, but the orphaned thread continues
            until it finishes or the process exits.  For true cancellation,
            consider ``multiprocessing``-based guards or ensure guards are
            non-blocking.
        """
        assert self._guard_executor is not None  # caller checks
        futures: List[Tuple[str, concurrent.futures.Future]] = []
        for guard_id, guard_fn in resolved_guards:
            futures.append((guard_id, self._guard_executor.submit(guard_fn, ctx)))

        results: List[GuardResult] = []
        for guard_id, future in futures:
            try:
                if self._guard_timeout_seconds is not None:
                    result = future.result(timeout=self._guard_timeout_seconds)
                else:
                    result = future.result()
            except concurrent.futures.TimeoutError:
                future.cancel()  # Best-effort; won't stop already-running thread
                with self._zombie_lock:
                    self._zombie_thread_count += 1
                    self._total_timeout_count += 1
                    _current_zombies = self._zombie_thread_count

                # When the orphaned thread eventually finishes, decrement.
                def _on_zombie_done(
                    _fut: concurrent.futures.Future,
                    _lock: threading.Lock = self._zombie_lock,
                    _engine: "TransitionEngine" = self,
                ) -> None:
                    with _lock:
                        _engine._zombie_thread_count = max(0, _engine._zombie_thread_count - 1)

                future.add_done_callback(_on_zombie_done)

                logger.error(
                    "Guard %s timed out after %.1fs (zombie_threads=%d, total_timeouts=%d). "
                    "The orphaned thread continues in the background. "
                    "Ensure guards are non-blocking or use AsyncTransitionEngine.",
                    guard_id, self._guard_timeout_seconds, _current_zombies, self._total_timeout_count,
                    extra={"ctx": {"guard_id": guard_id, "task_id": task_id, "transition_id": transition_def.get("id"), "zombie_threads": _current_zombies}},
                )
                result = GuardResult(guard_id, False, f"Guard timed out after {self._guard_timeout_seconds}s")
            except Exception as e:
                logger.error(
                    f"Guard {guard_id} raised exception: {e}", exc_info=True,
                    extra={"ctx": {"guard_id": guard_id, "task_id": task_id, "transition_id": transition_def.get("id")}},
                )
                result = GuardResult(guard_id, False, f"Guard error: {e}")
            results.append(result)
        return results

    # ------------------------------------------------------------------
    # Core API: transition_task
    # ------------------------------------------------------------------

    def transition_task(
        self,
        task_id: str,
        target_state: str,
        calling_role: str,
        dry_run: bool = False,
        transition_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute (or dry-run) a state transition for a task.

        Args:
            task_id: Task identifier.
            target_state: Target state (e.g. "ACTIVE", "READY_FOR_REVIEW").
            calling_role: Role attempting the transition.
            dry_run: If True, evaluate guards but do not apply state change.
            transition_params: Additional parameters for the transition.

        Returns:
            Dict with keys: result, transition_id, from_state, to_state,
            guard_results, dry_run, events_fired, temporal_updates,
            rejection_reason.
        """
        transition_params = transition_params or {}

        with self._tracer.start_as_current_span("governor.transition") as _root_span:
            _root_span.set_attribute("governor.task_id", task_id)
            _root_span.set_attribute("governor.target_state", target_state)
            _root_span.set_attribute("governor.calling_role", calling_role)
            _root_span.set_attribute("governor.dry_run", dry_run)
            return self._do_transition(
                task_id, target_state, calling_role, dry_run, transition_params, _root_span,
            )

    def _do_transition(
        self,
        task_id: str,
        target_state: str,
        calling_role: str,
        dry_run: bool,
        transition_params: Dict[str, Any],
        _span: Any,
    ) -> Dict[str, Any]:
        """Inner transition logic — extracted to avoid re-indenting the entire method."""

        # 0. Rate-limit check (before any backend call)
        if self._rate_limiter is not None and not self._rate_limiter.check(task_id):
            _span.set_attribute("governor.result", "RATE_LIMITED")
            return _error_response(
                ErrorCode.RATE_LIMITED,
                f"Too many transition attempts for task '{task_id}'. Try again later.",
            )

        # 1. Load task
        with self._tracer.start_as_current_span("governor.load_task") as load_span:
            try:
                task_data = self._backend.get_task(task_id)
                load_span.set_attribute("governor.task_found", True)
            except ValueError as e:
                load_span.set_attribute("governor.task_found", False)
                _span.set_attribute("governor.result", "TASK_NOT_FOUND")
                return _error_response(ErrorCode.TASK_NOT_FOUND, str(e))
            except Exception as e:
                load_span.set_attribute("governor.task_found", False)
                load_span.record_exception(e)
                _span.set_attribute("governor.result", "BACKEND_ERROR")
                logger.error(f"Backend read failed for task '{task_id}': {e}", exc_info=True, extra={"ctx": {"task_id": task_id}})
                return _error_response(ErrorCode.BACKEND_ERROR, f"Backend read failed: {e}")

        task = task_data["task"]
        from_state = _normalize_state(task.get("status"))
        target_state = _normalize_state(target_state)

        # 2. Find transition definition
        transition_def = self._find_transition(from_state, target_state)
        if transition_def is None:
            return _error_response(
                ErrorCode.ILLEGAL_TRANSITION,
                f"No transition defined from '{from_state}' to '{target_state}'",
                from_state=from_state,
                to_state=target_state,
            )

        # 3. Role check
        effective_role = self._normalize_calling_role(calling_role)
        allowed_roles = transition_def.get("allowed_roles", [])
        if effective_role not in allowed_roles:
            return _error_response(
                ErrorCode.ROLE_NOT_AUTHORIZED,
                f"Role '{calling_role}' not authorized for {from_state} -> {target_state}. "
                f"Allowed: {allowed_roles}",
                from_state=from_state,
                to_state=target_state,
                allowed_roles=allowed_roles,
            )

        # 4. Build guard context
        ctx = GuardContext(task_id, task_data, transition_params, backend=self._backend)

        # 5. Resolve all guards first (fail fast on missing guards)
        resolved_guards: List[Tuple[str, GuardCallable]] = []
        for guard_ref in transition_def.get("guards", []):
            try:
                guard_id, guard_fn = self._resolve_guard_instance(guard_ref)
            except ValueError as e:
                return _error_response(
                    ErrorCode.GUARD_NOT_FOUND,
                    str(e),
                    transition_id=transition_def.get("id"),
                    from_state=from_state,
                    to_state=target_state,
                )
            resolved_guards.append((guard_id, guard_fn))

        # 6. Evaluate guards — parallel when executor is available, else sequential
        with self._tracer.start_as_current_span("governor.evaluate_guards") as guards_span:
            guards_span.set_attribute("governor.guard_count", len(resolved_guards))
            guards_span.set_attribute("governor.parallel", self._guard_executor is not None)
            guard_results: List[GuardResult] = []
            if self._guard_executor is not None and len(resolved_guards) > 1:
                guard_results = self._evaluate_guards_parallel(
                    resolved_guards, ctx, task_id, transition_def,
                )
            else:
                for guard_id, guard_fn in resolved_guards:
                    result = self._evaluate_single_guard(
                        guard_id, guard_fn, ctx, task_id, transition_def,
                    )
                    guard_results.append(result)

            # Ensure deterministic ordering for parallel evaluation so that
            # rejection_reason always reports the same failing guard.
            if self._guard_executor is not None and len(guard_results) > 1:
                guard_results.sort(key=lambda gr: gr.guard_id)
            guards_span.set_attribute("governor.guards_passed", sum(1 for g in guard_results if g.passed))
            guards_span.set_attribute("governor.guards_failed", sum(1 for g in guard_results if not g.passed))

        # 7. Compute overall PASS/FAIL (supports AND/OR guard composition)
        guard_mode = transition_def.get("guard_mode", "AND").upper()
        rejection_reason = None
        if guard_mode == "OR" and guard_results:
            # OR mode: at least one guard must pass
            any_passed = any(gr.passed for gr in guard_results)
            overall_result = TransitionResult.PASS if any_passed else TransitionResult.FAIL
            if not any_passed:
                rejection_reason = "; ".join(
                    gr.reason for gr in guard_results if not gr.passed
                )
        else:
            # AND mode (default): all guards must pass
            overall_result = TransitionResult.PASS
            for gr in guard_results:
                if not gr.passed:
                    overall_result = TransitionResult.FAIL
                    if not rejection_reason:
                        rejection_reason = gr.reason

        # 8. Build response
        response: Dict[str, Any] = {
            "result": overall_result,
            "transition_id": transition_def["id"],
            "from_state": from_state,
            "to_state": target_state,
            "guard_results": [gr.to_dict() for gr in guard_results],
            "dry_run": dry_run,
            "events_fired": [],
            "temporal_updates": {},
            "rejection_reason": rejection_reason,
        }

        event_payload: Dict[str, Any] = {
            "task_id": task_id,
            "transition_id": transition_def["id"],
            "from_state": from_state,
            "to_state": target_state,
            "calling_role": effective_role,
            "result": overall_result,
            "dry_run": bool(dry_run),
            "rejection_reason": rejection_reason,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            "guard_results": [gr.to_dict() for gr in guard_results],
            "state_machine_version": self._state_machine_version,
        }

        _span.set_attribute("governor.result", overall_result)
        _span.set_attribute("governor.from_state", from_state)

        if dry_run or overall_result == TransitionResult.FAIL:
            audit_error = self._persist_audit_event(
                event_payload, task_id, transition_def,
            )
            if audit_error:
                response["audit_trail_error"] = audit_error
            return response

        # 9. Apply state change via backend
        with self._tracer.start_as_current_span("governor.apply_transition") as apply_span:
            updates: Dict[str, Any] = {"status": target_state}

            temporal = transition_def.get("temporal_fields", {})
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

            for field in temporal.get("set", []):
                updates[field] = now_iso
                response["temporal_updates"][field] = now_iso

            for field in temporal.get("clear", []):
                updates[field] = None
                response["temporal_updates"][field] = None

            # Used by state machines with increment/reset temporal operations
            if "increment" in temporal:
                for field in temporal["increment"]:
                    current_val = int(task.get(field) or 0)
                    updates[field] = current_val + 1
                    response["temporal_updates"][field] = current_val + 1

            if "reset" in temporal:
                for field in temporal["reset"]:
                    updates[field] = 0
                    response["temporal_updates"][field] = 0

            event_payload["result"] = TransitionResult.PASS
            try:
                apply_result = self._backend.apply_transition(
                    task_id=task_id,
                    updates=updates,
                    event=event_payload,
                    expected_current_status=from_state,
                )
                if not apply_result.get("success"):
                    apply_span.set_attribute("governor.apply_success", False)
                    if apply_result.get("error_code") == ErrorCode.STATE_CONFLICT:
                        return _error_response(
                            ErrorCode.STATE_CONFLICT,
                            (
                                "Task state changed concurrently during transition. "
                                f"Expected '{from_state}', found '{apply_result.get('actual_current_status')}'."
                            ),
                            from_state=from_state,
                            to_state=target_state,
                        )
                    if apply_result.get("error_code") == ErrorCode.EVENT_WRITE_FAILED:
                        return _error_response(
                            ErrorCode.EVENT_WRITE_FAILED,
                            "Transition event persistence failed; transition aborted.",
                            from_state=from_state,
                            to_state=target_state,
                        )
                    return _error_response(ErrorCode.CRUD_FAILED, f"Backend update failed: {apply_result}")
                apply_span.set_attribute("governor.apply_success", True)
            except Exception as e:
                apply_span.record_exception(e)
                apply_span.set_attribute("governor.apply_success", False)
                logger.error(f"Atomic transition apply failed: {e}", exc_info=True, extra={"ctx": {"task_id": task_id, "transition_id": transition_def.get("id"), "from_state": from_state, "to_state": target_state}})
                return _error_response(ErrorCode.CRUD_FAILED, f"Transition apply failed: {e}")

        # 10. Fire post-transition events
        with self._tracer.start_as_current_span("governor.fire_callbacks") as cb_span:
            updated_task = task
            try:
                updated_task_data = self._backend.get_task(task_id)
                updated_task = updated_task_data.get("task", task)
            except Exception as e:
                logger.warning(f"Failed to reload task '{task_id}' for callbacks (non-fatal): {e}", extra={"ctx": {"task_id": task_id, "transition_id": transition_def.get("id")}})
            event_params = {**transition_params, "calling_role": effective_role}
            events_fired = self._fire_events(transition_def, task_id, updated_task, event_params)
            response["events_fired"] = events_fired
            cb_span.set_attribute("governor.events_fired_count", len(events_fired))

        return response

    # ------------------------------------------------------------------
    # Core API: get_available_transitions
    # ------------------------------------------------------------------

    def get_available_transitions(
        self,
        task_id: str,
        calling_role: str,
    ) -> Dict[str, Any]:
        """Return available transitions for a task and role.

        Args:
            task_id: Task identifier.
            calling_role: Role querying.

        Returns:
            Dict with keys: task_id, current_state, transitions (list).
        """
        try:
            task_data = self._backend.get_task(task_id)
        except ValueError as e:
            return {"error": ErrorCode.TASK_NOT_FOUND, "message": str(e)}
        except Exception as e:
            logger.error(f"Backend read failed for task '{task_id}': {e}", exc_info=True, extra={"ctx": {"task_id": task_id}})
            return {"error": ErrorCode.BACKEND_ERROR, "message": f"Backend read failed: {e}"}

        task = task_data["task"]
        current_state = _normalize_state(task.get("status"))
        effective_role = self._normalize_calling_role(calling_role)

        all_transitions = self._get_all_transitions_from(current_state)
        ctx = GuardContext(task_id, task_data, backend=self._backend)

        transitions_out = []
        for tdef in all_transitions:
            role_authorized = effective_role in tdef.get("allowed_roles", [])

            guards_met = 0
            guards_total = len(tdef.get("guards", []))
            guards_missing = []
            guard_warnings = []

            for guard_ref in tdef.get("guards", []):
                try:
                    guard_id, guard_fn = self._resolve_guard_instance(guard_ref)
                except ValueError as e:
                    # Strict mode: a missing guard is a configuration error.
                    # Surface it as a blocking guard gap rather than raising.
                    guard_id = (
                        guard_ref
                        if isinstance(guard_ref, str)
                        else str((guard_ref or {}).get("guard_id") or "UNKNOWN")
                    )
                    guards_missing.append(
                        {
                            "guard_id": guard_id,
                            "reason": str(e),
                            "fix_hint": "Register/import the guard implementation before engine initialization.",
                        }
                    )
                    continue
                try:
                    result = guard_fn(ctx)
                except Exception as e:
                    result = GuardResult(guard_id, False, f"Guard error: {e}")

                if result.passed:
                    guards_met += 1
                    if result.warning:
                        guard_warnings.append({
                            "guard_id": result.guard_id,
                            "reason": result.reason,
                            "fix_hint": result.fix_hint,
                        })
                else:
                    guards_missing.append({
                        "guard_id": result.guard_id,
                        "reason": result.reason,
                        "fix_hint": result.fix_hint,
                    })

            guard_mode = tdef.get("guard_mode", "AND").upper()
            if guard_mode == "OR" and guards_total > 0:
                guards_satisfied = guards_met >= 1
            else:
                guards_satisfied = guards_met == guards_total

            transitions_out.append({
                "transition_id": tdef["id"],
                "target_state": tdef["to_state"],
                "description": tdef.get("description", ""),
                "allowed_roles": tdef.get("allowed_roles", []),
                "role_authorized": role_authorized,
                "guards_total": guards_total,
                "guards_met": guards_met,
                "guards_missing": guards_missing,
                "guard_warnings": guard_warnings,
                "guard_mode": guard_mode,
                "warnings_count": len(guard_warnings),
                "ready": role_authorized and guards_satisfied,
            })

        return {
            "task_id": task_id,
            "current_state": current_state,
            "transitions": transitions_out,
        }

    # ------------------------------------------------------------------
    # Core API: transition_tasks (batch convenience)
    # ------------------------------------------------------------------

    def transition_tasks(
        self,
        task_ids: List[str],
        target_state: str,
        calling_role: str,
        dry_run: bool = False,
        transition_params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute transitions for multiple tasks sequentially.

        Each transition has its own guard evaluation and atomic state change.
        If one task fails, the remaining tasks are still attempted.

        Args:
            task_ids: List of task identifiers.
            target_state: Target state for all tasks.
            calling_role: Role attempting the transitions.
            dry_run: If True, evaluate guards but do not apply.
            transition_params: Optional params passed to each transition.

        Returns:
            List of transition results, one per task_id (same order).
        """
        return [
            self.transition_task(
                tid, target_state, calling_role, dry_run, transition_params,
            )
            for tid in task_ids
        ]

    # ------------------------------------------------------------------
    # Analytics API (delegates to backend)
    # ------------------------------------------------------------------

    def get_task_audit_trail(self, task_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Return transition events for a task.

        Args:
            task_id: Task identifier.
            limit: Maximum events to return.
        """
        return self._backend.get_task_audit_trail(task_id=task_id, limit=limit)

    def get_guard_failure_hotspots(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Return guards ranked by failure count.

        Args:
            limit: Maximum guards to return.
        """
        return self._backend.get_guard_failure_hotspots(limit=limit)

    def get_policy_coverage(self) -> Dict[str, Any]:
        """Return guard evaluation coverage and pass/fail breakdown."""
        return self._backend.get_policy_coverage()

    def get_rework_lineage(self, task_id: str) -> Dict[str, Any]:
        """Return rework-oriented lineage for a task.

        Args:
            task_id: Task identifier.
        """
        return self._backend.get_rework_lineage(task_id=task_id)


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

_default_engine: Optional[TransitionEngine] = None


def configure(
    backend: GovernorBackend,
    state_machine_path: Optional[str] = None,
    role_aliases: Optional[Dict[str, str]] = None,
    event_callbacks: Optional[List[Callable]] = None,
    strict: bool = True,
) -> TransitionEngine:
    """Configure and return the default TransitionEngine singleton.

    Call this once at application startup::

        from governor.backend.memory_backend import MemoryBackend
        from governor.engine.transition_engine import configure

        engine = configure(backend=MemoryBackend())
    """
    global _default_engine
    _default_engine = TransitionEngine(
        backend=backend,
        state_machine_path=state_machine_path,
        role_aliases=role_aliases,
        event_callbacks=event_callbacks,
        strict=strict,
    )
    return _default_engine


def _get_engine() -> TransitionEngine:
    if _default_engine is None:
        raise RuntimeError(
            "Governor not configured. Call governor.configure(backend=...) before "
            "using module-level functions like transition_task() and "
            "get_available_transitions(). Alternatively, use "
            "TransitionEngine(backend=...) directly for instance-based usage. "
            "See README for examples of both patterns."
        )
    return _default_engine


def transition_task(
    task_id: str,
    target_state: str,
    calling_role: str,
    dry_run: bool = False,
    transition_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Module-level convenience: delegates to the default engine."""
    return _get_engine().transition_task(
        task_id, target_state, calling_role, dry_run, transition_params,
    )


def get_available_transitions(task_id: str, calling_role: str) -> Dict[str, Any]:
    """Module-level convenience: delegates to the default engine."""
    return _get_engine().get_available_transitions(task_id, calling_role)
