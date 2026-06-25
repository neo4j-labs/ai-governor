/**
 * Neo4j backend for Governor — production graph database integration.
 *
 * Requires the `neo4j-driver` package:
 *
 *   npm install neo4j-driver
 *
 * Usage:
 *
 *   import { Neo4jBackend } from "@governor/core";
 *
 *   const backend = new Neo4jBackend({
 *     uri: "neo4j://localhost:7687",
 *     user: "neo4j",
 *     password: "password",
 *   });
 *
 * Or from environment variables:
 *
 *   const backend = Neo4jBackend.fromEnv();
 */

import { GovernorBackend } from "./base.js";
import type {
  TaskData,
  TaskDict,
  TransitionEventDict,
  UpdateResult,
} from "../types.js";

// Lazy-load neo4j-driver to keep the core zero-dep.
let neo4j: typeof import("neo4j-driver") | null = null;

async function loadNeo4j(): Promise<typeof import("neo4j-driver")> {
  if (neo4j) return neo4j;
  try {
    neo4j = await import("neo4j-driver");
    return neo4j;
  } catch {
    throw new Error(
      "The 'neo4j-driver' package is required for Neo4jBackend. " +
        "Install it with: npm install neo4j-driver",
    );
  }
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

export interface Neo4jBackendOptions {
  uri: string;
  user: string;
  password: string;
  database?: string;
  relationshipLimit?: number;
  maxRetries?: number;
  retryBackoffMs?: number;
  queryTimeoutMs?: number;
}

// ---------------------------------------------------------------------------
// Field normalization
// ---------------------------------------------------------------------------

const NORMALIZE_FIELDS = new Set(["task_type", "status", "role", "priority"]);
const MAX_FIELD_SIZE = 1_000_000;

function normalizeTaskField(key: string, value: unknown): unknown {
  if (value == null) return null;
  if (typeof value === "string" && value.length > MAX_FIELD_SIZE) {
    throw new Error(
      `Field '${key}' exceeds maximum size (${value.length} > ${MAX_FIELD_SIZE} chars)`,
    );
  }
  if (NORMALIZE_FIELDS.has(key) && typeof value === "string") {
    return value.trim().toUpperCase();
  }
  return value;
}

// ---------------------------------------------------------------------------
// Neo4jBackend
// ---------------------------------------------------------------------------

export class Neo4jBackend extends GovernorBackend {
  private _driver: import("neo4j-driver").Driver;
  private _database: string;
  private _relationshipLimit: number;
  private _maxRetries: number;
  private _retryBackoffMs: number;
  private _queryTimeoutMs: number | undefined;

  private constructor(
    driver: import("neo4j-driver").Driver,
    opts: Required<
      Pick<Neo4jBackendOptions, "database" | "relationshipLimit" | "maxRetries" | "retryBackoffMs">
    > & { queryTimeoutMs?: number },
  ) {
    super();
    this._driver = driver;
    this._database = opts.database;
    this._relationshipLimit = opts.relationshipLimit;
    this._maxRetries = opts.maxRetries;
    this._retryBackoffMs = opts.retryBackoffMs;
    this._queryTimeoutMs = opts.queryTimeoutMs;
  }

  /**
   * Create a Neo4jBackend from explicit options.
   */
  static async create(opts: Neo4jBackendOptions): Promise<Neo4jBackend> {
    const driver = await Neo4jBackend._createDriver(opts);
    return new Neo4jBackend(driver, {
      database: opts.database ?? "neo4j",
      relationshipLimit: Math.max(1, opts.relationshipLimit ?? 200),
      maxRetries: Math.max(0, opts.maxRetries ?? 2),
      retryBackoffMs: Math.max(0, opts.retryBackoffMs ?? 100),
      queryTimeoutMs: opts.queryTimeoutMs,
    });
  }

  /**
   * Create a Neo4jBackend from environment variables.
   *
   * Reads GOVERNOR_NEO4J_URI, GOVERNOR_NEO4J_USER, GOVERNOR_NEO4J_PASSWORD,
   * and GOVERNOR_NEO4J_DATABASE from the environment.
   */
  static async fromEnv(
    overrides: Partial<Neo4jBackendOptions> = {},
  ): Promise<Neo4jBackend> {
    const uri = overrides.uri ?? process.env.GOVERNOR_NEO4J_URI;
    const user = overrides.user ?? process.env.GOVERNOR_NEO4J_USER;
    const password = overrides.password ?? process.env.GOVERNOR_NEO4J_PASSWORD;
    const database =
      overrides.database ?? process.env.GOVERNOR_NEO4J_DATABASE ?? "neo4j";

    const missing: string[] = [];
    if (!uri) missing.push("GOVERNOR_NEO4J_URI");
    if (!user) missing.push("GOVERNOR_NEO4J_USER");
    if (!password) missing.push("GOVERNOR_NEO4J_PASSWORD");
    if (missing.length > 0) {
      throw new Error(
        `Missing required Neo4j configuration. Set environment variable(s): ${missing.join(", ")}`,
      );
    }

    return Neo4jBackend.create({
      uri: uri!,
      user: user!,
      password: password!,
      database,
      ...overrides,
    });
  }

  private static async _createDriver(
    opts: Neo4jBackendOptions,
  ): Promise<import("neo4j-driver").Driver> {
    const mod = await loadNeo4j();
    return mod.default.driver(opts.uri, mod.default.auth.basic(opts.user, opts.password));
  }

  /** Close the driver connection. */
  async close(): Promise<void> {
    await this._driver.close();
  }

  // ------------------------------------------------------------------
  // GovernorBackend interface — required methods
  // ------------------------------------------------------------------

  async getTask(taskId: string): Promise<TaskData> {
    const fetchLimit = this._relationshipLimit + 1;

    const query = `
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
    `;

    const records = await this._runReadQuery(query, {
      task_id: taskId,
      fetch_limit: this._int(fetchLimit),
    });

    if (records.length === 0) {
      throw new Error(`Task not found: ${taskId}`);
    }

    const record = records[0];
    const task = record.task as TaskDict;
    let outRels = (record.out_rels ?? []) as Record<string, unknown>[];
    let inRels = (record.in_rels ?? []) as Record<string, unknown>[];

    const truncated =
      outRels.length > this._relationshipLimit ||
      inRels.length > this._relationshipLimit;

    outRels = outRels.slice(0, this._relationshipLimit);
    inRels = inRels.slice(0, this._relationshipLimit);

    const relationships = [...outRels, ...inRels].filter(
      (r) => r.type != null,
    ) as unknown as TaskData["relationships"];

    const result: TaskData & Record<string, unknown> = { task, relationships };
    if (truncated) {
      result.relationships_truncated = true;
      result.total_relationship_count =
        ((record.out_rels as unknown[]) ?? []).length +
        ((record.in_rels as unknown[]) ?? []).length;
      result.relationship_limit = this._relationshipLimit;
    }
    return result;
  }

  async updateTask(
    taskId: string,
    updates: Record<string, unknown>,
    expectedCurrentStatus?: string,
  ): Promise<UpdateResult> {
    const setClauses: string[] = [];
    const params: Record<string, unknown> = {
      task_id: taskId,
      expected_current_status: expectedCurrentStatus ?? null,
    };

    for (const [key, value] of Object.entries(updates)) {
      if (!/^[a-zA-Z_]\w*$/.test(key)) {
        throw new Error(`Invalid property name: ${key}`);
      }
      const paramName = `upd_${key}`;
      if (value == null) {
        setClauses.push(`t.${key} = null`);
      } else {
        setClauses.push(`t.${key} = $${paramName}`);
        params[paramName] = normalizeTaskField(key, value);
      }
    }

    params.last_updated = new Date().toISOString();
    setClauses.push("t.last_updated = $last_updated");

    const query = `
      MATCH (t:Task {task_id: $task_id})
      WHERE $expected_current_status IS NULL OR t.status = $expected_current_status
      SET ${setClauses.join(", ")}
      RETURN t.task_id AS task_id, t.status AS status
    `;

    const records = await this._runWriteQuery(query, params);
    if (records.length === 0) {
      if (expectedCurrentStatus != null && (await this.taskExists(taskId))) {
        const current = await this._runReadQuery(
          "MATCH (t:Task {task_id: $task_id}) RETURN t.status AS status",
          { task_id: taskId },
        );
        const actual = current[0]?.status ?? null;
        return {
          success: false,
          error_code: "STATE_CONFLICT",
          task_id: taskId,
          expected_current_status: expectedCurrentStatus,
          actual_current_status: actual,
        };
      }
      throw new Error(`Task not found during update: ${taskId}`);
    }
    return {
      success: true,
      task_id: taskId,
      new_status: records[0].status as string,
    };
  }

  async taskExists(taskId: string): Promise<boolean> {
    const records = await this._runReadQuery(
      "MATCH (t:Task {task_id: $task_id}) RETURN count(t) AS cnt",
      { task_id: taskId },
    );
    return Boolean(records.length > 0 && (records[0].cnt as number) > 0);
  }

  async getReviewsForTask(
    taskId: string,
  ): Promise<Record<string, unknown>[]> {
    const records = await this._runReadQuery(
      `MATCH (t:Task {task_id: $task_id})-[:HAS_REVIEW]->(r:Review)
       RETURN properties(r) AS review`,
      { task_id: taskId },
    );
    return records.map((r) => r.review as Record<string, unknown>);
  }

  async getReportsForTask(
    taskId: string,
  ): Promise<Record<string, unknown>[]> {
    const records = await this._runReadQuery(
      `MATCH (r:Report)-[:REPORTS_ON]->(t:Task {task_id: $task_id})
       RETURN properties(r) AS report`,
      { task_id: taskId },
    );
    return records.map((r) => r.report as Record<string, unknown>);
  }

  // ------------------------------------------------------------------
  // Lifecycle helpers
  // ------------------------------------------------------------------

  async createTask(taskData: TaskDict): Promise<TaskDict> {
    const taskId = taskData.task_id;
    if (await this.taskExists(taskId)) {
      throw new Error(`Task already exists: ${taskId}`);
    }

    const props: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(taskData)) {
      props[key] = normalizeTaskField(key, value);
    }
    const now = new Date().toISOString();
    props.created_date ??= now.slice(0, 10);
    props.last_updated ??= now;

    const records = await this._runWriteQuery(
      "CREATE (t:Task) SET t = $props RETURN properties(t) AS task",
      { props },
    );
    if (records.length === 0) {
      throw new Error(`Failed to create task: ${taskId}`);
    }
    return records[0].task as TaskDict;
  }

  async addReview(
    taskId: string,
    review: Record<string, unknown>,
  ): Promise<void> {
    if (!(await this.taskExists(taskId))) {
      throw new Error(`Task not found: ${taskId}`);
    }
    const reviewProps = { ...review };
    reviewProps.review_id ??= `review::${crypto.randomUUID()}`;
    await this._runWriteQuery(
      `MATCH (t:Task {task_id: $task_id})
       CREATE (r:Review) SET r = $review
       CREATE (t)-[:HAS_REVIEW]->(r)`,
      { task_id: taskId, review: reviewProps },
    );
  }

  async addReport(
    taskId: string,
    report: Record<string, unknown>,
  ): Promise<void> {
    if (!(await this.taskExists(taskId))) {
      throw new Error(`Task not found: ${taskId}`);
    }
    const reportProps = { ...report };
    reportProps.report_id ??= `report::${crypto.randomUUID()}`;
    await this._runWriteQuery(
      `MATCH (t:Task {task_id: $task_id})
       CREATE (r:Report) SET r = $report
       CREATE (r)-[:REPORTS_ON]->(t)`,
      { task_id: taskId, report: reportProps },
    );
  }

  async addHandoff(
    taskId: string,
    handoff: Record<string, unknown>,
  ): Promise<void> {
    if (!(await this.taskExists(taskId))) {
      throw new Error(`Task not found: ${taskId}`);
    }
    const handoffProps = { ...handoff };
    handoffProps.handoff_id ??= `handoff::${crypto.randomUUID()}`;
    await this._runWriteQuery(
      `MATCH (t:Task {task_id: $task_id})
       CREATE (h:Handoff) SET h = $handoff
       CREATE (t)-[:HANDOFF_TO]->(h)`,
      { task_id: taskId, handoff: handoffProps },
    );
  }

  async applyTransition(
    taskId: string,
    updates: Record<string, unknown>,
    event: TransitionEventDict,
    expectedCurrentStatus?: string,
  ): Promise<UpdateResult> {
    const setClauses: string[] = [];
    const params: Record<string, unknown> = {
      task_id: taskId,
      expected_current_status: expectedCurrentStatus ?? null,
      transition_id: event.transition_id ?? null,
      from_state: event.from_state ?? null,
      to_state: event.to_state ?? null,
      calling_role: event.calling_role ?? null,
      result: event.result ?? null,
      dry_run: Boolean(event.dry_run),
      rejection_reason: event.rejection_reason ?? null,
      occurred_at: event.occurred_at ?? null,
      guard_results: event.guard_results ?? [],
    };

    for (const [key, value] of Object.entries(updates)) {
      if (!/^[a-zA-Z_]\w*$/.test(key)) {
        throw new Error(`Invalid property name: ${key}`);
      }
      const paramName = `upd_${key}`;
      if (value == null) {
        setClauses.push(`t.${key} = null`);
      } else {
        setClauses.push(`t.${key} = $${paramName}`);
        params[paramName] = normalizeTaskField(key, value);
      }
    }

    params.last_updated = new Date().toISOString();
    setClauses.push("t.last_updated = $last_updated");

    const query = `
      MATCH (t:Task {task_id: $task_id})
      WHERE $expected_current_status IS NULL OR t.status = $expected_current_status
      SET ${setClauses.join(", ")}
      WITH t
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
      WITH t, te
      CALL {
        WITH te
        UNWIND $guard_results AS gr
        CREATE (ge:GuardEvaluation {
          eval_id: randomUUID(),
          guard_id: gr.guard_id,
          passed: gr.passed,
          warning: coalesce(gr.warning, false),
          reason: coalesce(gr.reason, ""),
          fix_hint: coalesce(gr.fix_hint, "")
        })
        CREATE (te)-[:HAS_GUARD_EVALUATION]->(ge)
      }
      RETURN t.task_id AS task_id, t.status AS status, te.event_id AS event_id
    `;

    const records = await this._runWriteQuery(query, params);
    if (records.length === 0) {
      if (expectedCurrentStatus != null && (await this.taskExists(taskId))) {
        const current = await this._runReadQuery(
          "MATCH (t:Task {task_id: $task_id}) RETURN t.status AS status",
          { task_id: taskId },
        );
        const actual = current[0]?.status ?? null;
        return {
          success: false,
          error_code: "STATE_CONFLICT",
          task_id: taskId,
          expected_current_status: expectedCurrentStatus,
          actual_current_status: actual,
        };
      }
      throw new Error(`Task not found during update: ${taskId}`);
    }

    return {
      success: true,
      task_id: records[0].task_id as string,
      new_status: records[0].status as string,
      event_id: records[0].event_id as string,
    };
  }

  async recordTransitionEvent(
    event: TransitionEventDict,
  ): Promise<UpdateResult> {
    const query = `
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
    `;
    const params = {
      task_id: event.task_id ?? null,
      transition_id: event.transition_id ?? null,
      from_state: event.from_state ?? null,
      to_state: event.to_state ?? null,
      calling_role: event.calling_role ?? null,
      result: event.result ?? null,
      dry_run: Boolean(event.dry_run),
      rejection_reason: event.rejection_reason ?? null,
      occurred_at: event.occurred_at ?? null,
      guard_results: event.guard_results ?? [],
    };

    const records = await this._runWriteQuery(query, params);
    if (records.length === 0) {
      return { success: false, error_code: "EVENT_WRITE_FAILED" };
    }
    return { success: true, event_id: records[0].event_id as string };
  }

  async healthCheck(): Promise<Record<string, unknown>> {
    try {
      const info = await this._driver.getServerInfo();
      return {
        healthy: true,
        server_address: info.address,
        server_version: info.agent ?? "unknown",
        protocol_version: info.protocolVersion ?? null,
        database: this._database,
      };
    } catch (e) {
      return {
        healthy: false,
        error: String(e),
        database: this._database,
      };
    }
  }

  async getTaskAuditTrail(
    taskId: string,
    limit = 50,
  ): Promise<TransitionEventDict[]> {
    const query = `
      MATCH (:Task {task_id: $task_id})-[:HAS_TRANSITION_EVENT]->(te:TransitionEvent)
      OPTIONAL MATCH (te)-[:HAS_GUARD_EVALUATION]->(ge:GuardEvaluation)
      WITH te, collect(properties(ge)) AS guard_results
      RETURN properties(te) AS event, guard_results
      ORDER BY te.occurred_at DESC
      LIMIT $limit
    `;
    const rows = await this._runReadQuery(query, {
      task_id: taskId,
      limit: this._int(Math.max(1, limit)),
    });
    return rows.map((row) => ({
      ...(row.event as TransitionEventDict),
      guard_results: (row.guard_results ?? []) as TransitionEventDict["guard_results"],
    }));
  }

  async getGuardFailureHotspots(
    limit = 10,
  ): Promise<Record<string, unknown>[]> {
    const query = `
      MATCH (ge:GuardEvaluation)
      RETURN ge.guard_id AS guard_id,
             count(ge) AS evaluations,
             sum(CASE WHEN ge.passed THEN 0 ELSE 1 END) AS failures
      ORDER BY failures DESC, evaluations DESC
      LIMIT $limit
    `;
    return this._runReadQuery(query, {
      limit: this._int(Math.max(1, limit)),
    });
  }

  async getPolicyCoverage(): Promise<Record<string, unknown>> {
    const matchClause = "MATCH (ge:GuardEvaluation)";
    const aggCols =
      "count(ge) AS evaluations, " +
      "sum(CASE WHEN ge.passed THEN 1 ELSE 0 END) AS passes, " +
      "sum(CASE WHEN ge.passed THEN 0 ELSE 1 END) AS fails";

    const guards = await this._runReadQuery(
      `${matchClause} RETURN ge.guard_id AS guard_id, ${aggCols} ORDER BY guard_id`,
      {},
    );
    const totalsRows = await this._runReadQuery(
      `${matchClause} RETURN ${aggCols}`,
      {},
    );
    const totals = totalsRows[0] ?? { evaluations: 0, passes: 0, fails: 0 };
    return { guards, totals };
  }

  async getReworkLineage(
    taskId: string,
  ): Promise<Record<string, unknown>> {
    const query = `
      MATCH (:Task {task_id: $task_id})-[:HAS_TRANSITION_EVENT]->(te:TransitionEvent)
      WHERE te.result = 'PASS'
      RETURN te.transition_id AS transition_id,
             te.from_state AS from_state,
             te.to_state AS to_state,
             te.result AS result,
             te.occurred_at AS occurred_at
      ORDER BY te.occurred_at ASC
    `;
    const lineage = await this._runReadQuery(query, { task_id: taskId });
    const reworkCount = lineage.filter(
      (item) => item.to_state === "REWORK",
    ).length;
    return { task_id: taskId, rework_count: reworkCount, lineage };
  }

  // ------------------------------------------------------------------
  // Schema
  // ------------------------------------------------------------------

  async ensureSchema(statements: string[]): Promise<Record<string, unknown>> {
    let applied = 0;
    const errors: Array<{ statement_index: string; error: string }> = [];
    for (let i = 0; i < statements.length; i++) {
      try {
        await this._runWriteQuery(statements[i], {});
        applied++;
      } catch (e) {
        errors.push({ statement_index: String(i), error: String(e) });
      }
    }
    const result: Record<string, unknown> = {
      success: errors.length === 0,
      statements_applied: applied,
    };
    if (errors.length > 0) result.errors = errors;
    return result;
  }

  // ------------------------------------------------------------------
  // Public query helpers (used by GovernorAnalytics)
  // ------------------------------------------------------------------

  async runReadQuery(
    query: string,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>[]> {
    return this._runQuery(query, params, "read");
  }

  async runWriteQuery(
    query: string,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>[]> {
    return this._runQuery(query, params, "write");
  }

  // ------------------------------------------------------------------
  // Internal query helpers
  // ------------------------------------------------------------------

  private async _runReadQuery(
    query: string,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>[]> {
    return this._runQuery(query, params, "read");
  }

  private async _runWriteQuery(
    query: string,
    params: Record<string, unknown>,
  ): Promise<Record<string, unknown>[]> {
    return this._runQuery(query, params, "write");
  }

  private async _runQuery(
    query: string,
    params: Record<string, unknown>,
    mode: "read" | "write",
  ): Promise<Record<string, unknown>[]> {
    let attempt = 0;
    while (true) {
      attempt++;
      try {
        const session = this._driver.session({ database: this._database });
        try {
          const txConfig: Record<string, unknown> = {};
          if (this._queryTimeoutMs != null) {
            txConfig.timeout = this._queryTimeoutMs;
          }
          let records: Record<string, unknown>[];
          if (mode === "write") {
            records = await session.executeWrite(
              async (tx) => {
                const result = await tx.run(query, params);
                return result.records.map((r) => r.toObject());
              },
              txConfig,
            );
          } else {
            records = await session.executeRead(
              async (tx) => {
                const result = await tx.run(query, params);
                return result.records.map((r) => r.toObject());
              },
              txConfig,
            );
          }
          return records as Record<string, unknown>[];
        } finally {
          await session.close();
        }
      } catch (err) {
        if (!this._isRetryable(err) || attempt > this._maxRetries) {
          throw err;
        }
        const baseDelay = this._retryBackoffMs * 2 ** (attempt - 1);
        const jitter = Math.random() * baseDelay * 0.5;
        await new Promise((r) => setTimeout(r, baseDelay + jitter));
      }
    }
  }

  private _isRetryable(err: unknown): boolean {
    if (!(err instanceof Error)) return false;
    const name = err.constructor.name;
    if (
      ["TransientError", "ServiceUnavailable", "SessionExpired"].includes(name)
    ) {
      return true;
    }
    const code = (err as { code?: string }).code ?? "";
    if (typeof code === "string" && code.startsWith("Neo.TransientError.")) {
      return true;
    }
    return false;
  }

  /** Convert a number to a Neo4j integer for LIMIT/SKIP params. */
  private _int(n: number): unknown {
    if (neo4j) return neo4j.default.int(n);
    throw new Error("Neo4j driver not loaded. Ensure the backend is initialized before querying.");
  }
}
