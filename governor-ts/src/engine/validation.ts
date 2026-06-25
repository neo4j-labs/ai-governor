/**
 * State machine JSON validation.
 *
 * Validates the structure and integrity of a state machine definition
 * before the TransitionEngine accepts it. Catches configuration errors
 * early instead of at runtime.
 */

import type { StateMachineDef, TransitionDef } from "../types.js";

export function validateStateMachine(sm: StateMachineDef): string[] {
  const errors: string[] = [];

  // 1. Required top-level keys
  if (!sm.states) errors.push("Missing required key: 'states'");
  if (!sm.transitions) errors.push("Missing required key: 'transitions'");
  if (errors.length > 0) return errors;

  const states = sm.states;
  const transitions = sm.transitions;

  if (typeof states !== "object" || Object.keys(states).length === 0) {
    errors.push("'states' must be a non-empty object");
    return errors;
  }

  if (!Array.isArray(transitions)) {
    errors.push("'transitions' must be a list");
    return errors;
  }

  const stateNames = new Set(Object.keys(states));

  // Validate state definitions
  for (const [name, defn] of Object.entries(states)) {
    if (!name.trim()) {
      errors.push("State names must be non-empty strings");
    }
    if (typeof defn !== "object" || defn == null) {
      errors.push(`State '${name}' definition must be an object`);
      continue;
    }
    if ("terminal" in defn && typeof defn.terminal !== "boolean") {
      errors.push(`State '${name}': 'terminal' must be boolean`);
    }
  }

  // 3. At least one terminal state
  const terminalStates = new Set<string>();
  for (const [name, defn] of Object.entries(states)) {
    if (typeof defn === "object" && defn?.terminal) {
      terminalStates.add(name);
    }
  }
  if (terminalStates.size === 0) {
    errors.push(
      "No terminal state defined (need at least one state with 'terminal': true)",
    );
  }

  // 6. Each transition has required keys + 5. No duplicate IDs
  const requiredKeys = new Set(["id", "from_state", "to_state", "allowed_roles"]);
  const seenIds = new Set<string>();
  const fromStates = new Set<string>();
  const toStates = new Set<string>();

  for (let i = 0; i < transitions.length; i++) {
    const t = transitions[i] as unknown as Record<string, unknown>;
    if (typeof t !== "object" || t == null) {
      errors.push(`Transition at index ${i} is not an object`);
      continue;
    }

    const keys = new Set(Object.keys(t));
    const missing = [...requiredKeys].filter((k) => !keys.has(k));
    if (missing.length > 0) {
      errors.push(
        `Transition at index ${i} missing keys: ${missing.sort().join(", ")}`,
      );
      continue;
    }

    const tid = t.id as string;
    if (typeof tid !== "string" || !tid.trim()) {
      errors.push(
        `Transition at index ${i}: 'id' must be a non-empty string`,
      );
      continue;
    }

    if (seenIds.has(tid)) {
      errors.push(`Duplicate transition ID: '${tid}'`);
    }
    seenIds.add(tid);

    // 2. Valid state references
    const fs = t.from_state as string;
    const ts = t.to_state as string;

    if (typeof fs !== "string" || !fs.trim()) {
      errors.push(
        `Transition '${tid}': 'from_state' must be a non-empty string`,
      );
    }
    if (typeof ts !== "string" || !ts.trim()) {
      errors.push(
        `Transition '${tid}': 'to_state' must be a non-empty string`,
      );
    }
    if (!stateNames.has(fs)) {
      errors.push(
        `Transition '${tid}': from_state '${fs}' not in defined states`,
      );
    }
    if (!stateNames.has(ts)) {
      errors.push(
        `Transition '${tid}': to_state '${ts}' not in defined states`,
      );
    }

    const allowedRoles = t.allowed_roles;
    if (!Array.isArray(allowedRoles) || allowedRoles.length === 0) {
      errors.push(
        `Transition '${tid}': 'allowed_roles' must be a non-empty list`,
      );
    } else {
      const badRoles = allowedRoles.filter(
        (r: unknown) => typeof r !== "string" || !(r as string).trim(),
      );
      if (badRoles.length > 0) {
        errors.push(
          `Transition '${tid}': all 'allowed_roles' must be non-empty strings`,
        );
      }
    }

    // Guards validation
    const guards = (t.guards ?? []) as unknown[];
    if (!Array.isArray(guards)) {
      errors.push(`Transition '${tid}': 'guards' must be a list`);
    } else {
      for (const g of guards) {
        if (typeof g === "string") continue;
        if (typeof g === "object" && g != null) {
          const gObj = g as Record<string, unknown>;
          const guardId = gObj.guard_id;
          if (typeof guardId !== "string" || !guardId.trim()) {
            errors.push(
              `Transition '${tid}': inline guard missing string 'guard_id'`,
            );
          }
          const check = gObj.check;
          if (check !== undefined && typeof check !== "string") {
            errors.push(
              `Transition '${tid}': inline guard 'check' must be a string`,
            );
          }
          continue;
        }
        errors.push(
          `Transition '${tid}': each guard must be string or object`,
        );
      }
    }

    // Temporal fields validation
    const temporalFields = t.temporal_fields;
    if (temporalFields != null) {
      if (typeof temporalFields !== "object") {
        errors.push(
          `Transition '${tid}': 'temporal_fields' must be an object`,
        );
      } else {
        const tf = temporalFields as Record<string, unknown>;
        for (const key of ["set", "clear", "increment", "reset"]) {
          const values = tf[key];
          if (values == null) continue;
          if (
            !Array.isArray(values) ||
            values.some(
              (v: unknown) => typeof v !== "string" || !(v as string).trim(),
            )
          ) {
            errors.push(
              `Transition '${tid}': temporal_fields.${key} must be a list of non-empty strings`,
            );
          }
        }
      }
    }

    // Events validation
    const events = t.events;
    if (events != null) {
      if (!Array.isArray(events)) {
        errors.push(`Transition '${tid}': 'events' must be a list`);
      } else {
        for (let idx = 0; idx < events.length; idx++) {
          const event = events[idx] as Record<string, unknown>;
          if (typeof event !== "object" || event == null) {
            errors.push(
              `Transition '${tid}': event at index ${idx} must be an object`,
            );
            continue;
          }
          if ("type" in event && typeof event.type !== "string") {
            errors.push(
              `Transition '${tid}': event.type must be a string`,
            );
          }
          if ("event_id" in event && typeof event.event_id !== "string") {
            errors.push(
              `Transition '${tid}': event.event_id must be a string`,
            );
          }
          if ("config" in event && typeof event.config !== "object") {
            errors.push(
              `Transition '${tid}': event.config must be an object`,
            );
          }
        }
      }
    }

    fromStates.add(fs);
    toStates.add(ts);
  }

  // 7. Terminal states have no outbound transitions
  for (const ts of terminalStates) {
    if (fromStates.has(ts)) {
      errors.push(
        `Terminal state '${ts}' has outbound transitions (terminals must be sinks)`,
      );
    }
  }

  // 4. Orphan state detection
  const connectedStates = new Set([...fromStates, ...toStates]);
  for (const name of stateNames) {
    if (!connectedStates.has(name) && !terminalStates.has(name)) {
      errors.push(
        `Orphan state '${name}': no inbound or outbound transitions and not terminal`,
      );
    }
  }

  return errors;
}
