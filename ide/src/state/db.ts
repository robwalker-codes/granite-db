import type { ExplainPayload } from "../lib/planTypes";
import { call, type Result } from "../lib/tauri/gateway";

export interface DatabaseColumn {
  name: string;
  type: string;
  notNull: boolean;
  default: string | null;
  isPrimaryKey: boolean;
}

export interface DatabaseIndex {
  name: string;
  columns: string[];
  unique: boolean;
  type: string;
}

export interface DatabaseForeignKey {
  name: string;
  fromColumns: string[];
  toTable: string;
  toColumns: string[];
  onDelete: string;
  onUpdate: string;
}

export interface DatabaseTable {
  name: string;
  rowCount: number;
  columns: DatabaseColumn[];
  indexes: DatabaseIndex[];
  foreignKeys: DatabaseForeignKey[];
}

export interface DatabaseMetadata {
  database: string;
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
  const { result } = payload;
  const columns = Array.isArray(result.columns) ? result.columns.map((col) => String(col)) : [];
  const rows = Array.isArray(result.rows)
    ? result.rows.map((row) => (Array.isArray(row) ? row.map((cell) => String(cell ?? "")) : []))
    : [];
  const durationMs = typeof result.durationMs === "number" ? result.durationMs : 0;
  const normalized: QueryResult = {
    columns,
    rows,
    durationMs,
    rowsAffected: result.rowsAffected,
    message: result.message
  };
  return { ok: true, value: normalized };
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
    const parsed = JSON.parse(response.value) as Partial<DatabaseMetadata>;
    const database = typeof parsed.database === "string" ? parsed.database : path;
    const tables = Array.isArray(parsed.tables) ? parsed.tables : [];
    const normalisedTables: DatabaseTable[] = tables
      .map((table) => {
        if (!table || typeof table !== "object") {
          return null;
        }
        const name = typeof table.name === "string" ? table.name : "";
        if (!name) {
          return null;
        }
        const rowCount = typeof table.rowCount === "number" ? table.rowCount : -1;
        const columns = Array.isArray(table.columns)
          ? table.columns
              .map((col) => {
                if (!col || typeof col !== "object") {
                  return null;
                }
                const columnName = typeof col.name === "string" ? col.name : "";
                if (!columnName) {
                  return null;
                }
                return {
                  name: columnName,
                  type: typeof col.type === "string" ? col.type : "UNKNOWN",
                  notNull: Boolean(col.notNull),
                  default: typeof col.default === "string" ? col.default : null,
                  isPrimaryKey: Boolean(col.isPrimaryKey)
                } satisfies DatabaseColumn;
              })
              .filter((col): col is DatabaseColumn => Boolean(col))
          : [];
        const indexes = Array.isArray(table.indexes)
          ? table.indexes
              .map((idx) => {
                if (!idx || typeof idx !== "object") {
                  return null;
                }
                const indexName = typeof idx.name === "string" ? idx.name : "";
                if (!indexName) {
                  return null;
                }
                const columns = Array.isArray(idx.columns)
                  ? idx.columns.map((col) => String(col)).filter(Boolean)
                  : [];
                return {
                  name: indexName,
                  columns,
                  unique: Boolean(idx.unique),
                  type: typeof idx.type === "string" ? idx.type : ""
                } satisfies DatabaseIndex;
              })
              .filter((idx): idx is DatabaseIndex => Boolean(idx))
          : [];
        const foreignSource = (table as DatabaseTable).foreignKeys ?? (table as unknown as { fks?: unknown[] }).fks;
        const foreignKeys = Array.isArray(foreignSource)
          ? foreignSource
              .map((fk: unknown) => {
                if (!fk || typeof fk !== "object") {
                  return null;
                }
                const record = fk as Partial<DatabaseForeignKey> & Record<string, unknown>;
                const fkName = typeof record.name === "string" ? record.name : "";
                if (!fkName) {
                  return null;
                }
                const sourceCols = record.fromColumns ?? (record as unknown as { columns?: unknown[] }).columns;
                const fromColumns = Array.isArray(sourceCols)
                  ? sourceCols.map((col: unknown) => String(col)).filter(Boolean)
                  : [];
                const toTableRaw =
                  typeof record.toTable === "string"
                    ? record.toTable
                    : typeof (record as unknown as { refTable?: unknown }).refTable === "string"
                    ? ((record as unknown as { refTable: string }).refTable)
                    : "";
                const targetCols = record.toColumns ?? (record as unknown as { refColumns?: unknown[] }).refColumns;
                const toColumns = Array.isArray(targetCols)
                  ? targetCols.map((col: unknown) => String(col)).filter(Boolean)
                  : [];
                return {
                  name: fkName,
                  fromColumns,
                  toTable: toTableRaw,
                  toColumns,
                  onDelete: typeof record.onDelete === "string" ? record.onDelete : "",
                  onUpdate: typeof record.onUpdate === "string" ? record.onUpdate : ""
                } satisfies DatabaseForeignKey;
              })
              .filter((fk): fk is DatabaseForeignKey => Boolean(fk))
          : [];
        return {
          name,
          rowCount,
          columns,
          indexes,
          foreignKeys
        } satisfies DatabaseTable;
      })
      .filter((table): table is DatabaseTable => Boolean(table));

    return {
      ok: true,
      value: {
        database,
        tables: normalisedTables
      }
    };
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
