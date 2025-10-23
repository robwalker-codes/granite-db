import type { ExplainPayload } from "../lib/planTypes";
import { call, type Result } from "../lib/tauri/gateway";

export interface DatabaseColumn {
  name: string;
  type: string;
  notNull: boolean;
  pk: boolean;
}

export interface DatabaseIndex {
  name: string;
  columns: string[];
  unique: boolean;
}

export interface DatabaseForeignKey {
  name: string;
  columns: string[];
  refTable: string;
  refColumns: string[];
}

export interface DatabaseTable {
  name: string;
  rowCount: number;
  columns: DatabaseColumn[];
  indexes: DatabaseIndex[];
  fks: DatabaseForeignKey[];
}

export interface DatabaseMetadata {
  tables: DatabaseTable[];
}

export interface QueryResult {
  columns: string[];
  rows: string[][];
  durationMs: number;
  rowsAffected?: number;
  message?: string;
}

interface ExecResponse {
  format: "table" | "csv" | "jsonRows";
  output?: string;
  result?: QueryResult;
}

export async function createDatabase(path: string): Promise<Result<void>> {
  if (!path) {
    return { ok: false, error: "Database path is required" };
  }
  return call("create_db", { path });
}

export async function openDatabase(path: string): Promise<Result<void>> {
  if (!path) {
    return { ok: false, error: "Database path is required" };
  }
  return call("open_db", { path });
}

export async function executeQuery(path: string, sql: string): Promise<Result<QueryResult>> {
  if (!path) {
    return { ok: false, error: "Database path is required" };
  }
  if (!sql.trim()) {
    return { ok: false, error: "SQL must not be empty" };
  }
  const response = await call<ExecResponse>("exec_sql", { path, sql, format: "jsonRows" });
  if (!response.ok) {
    return response;
  }
  const payload = response.value;
  if (!payload || payload.format !== "jsonRows" || !payload.result) {
    return { ok: false, error: "Unexpected response from engine" };
  }
  return { ok: true, value: payload.result };
}

export async function explainQuery(path: string, sql: string): Promise<Result<ExplainPayload>> {
  if (!path) {
    return { ok: false, error: "Database path is required" };
  }
  if (!sql.trim()) {
    return { ok: false, error: "SQL must not be empty" };
  }
  const response = await call<string>("explain_sql", { path, sql });
  if (!response.ok) {
    return response;
  }
  try {
    return { ok: true, value: JSON.parse(response.value) as ExplainPayload };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { ok: false, error: `Failed to parse explain plan: ${message}` };
  }
}

export async function fetchMetadata(path: string): Promise<Result<DatabaseMetadata>> {
  if (!path) {
    return { ok: false, error: "Database path is required" };
  }
  console.log("[fetchMetadata] called with", path);
  const response = await call<string>("metadata", { path });
  if (!response.ok) {
    return response;
  }
  try {
    return { ok: true, value: JSON.parse(response.value) as DatabaseMetadata };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { ok: false, error: `Failed to parse metadata: ${message}` };
  }
}

export async function exportCsv(path: string, sql: string, outPath: string): Promise<Result<void>> {
  if (!path) {
    return { ok: false, error: "Database path is required" };
  }
  if (!sql.trim()) {
    return { ok: false, error: "SQL must not be empty" };
  }
  if (!outPath) {
    return { ok: false, error: "Export path is required" };
  }
  return call("export_csv", { path, sql, outPath });
}

export function isDdlStatement(sql: string): boolean {
  const trimmed = sql.trim().toLowerCase();
  if (!trimmed) {
    return false;
  }
  const ddlPrefixes = ["create", "drop", "alter", "rename", "truncate"]; // conservative list
  return ddlPrefixes.some((prefix) => trimmed.startsWith(prefix));
}

export function isCommitStatement(sql: string): boolean {
  const trimmed = sql.trim().toLowerCase();
  if (!trimmed) {
    return false;
  }
  return trimmed === "commit" || trimmed.startsWith("commit;") || trimmed.startsWith("commit transaction");
}
