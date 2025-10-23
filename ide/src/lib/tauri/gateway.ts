import { invoke } from "@tauri-apps/api/tauri";

const useMockEngine = import.meta.env?.VITE_ENABLE_E2E_MOCKS === "true";

const mockEngine = useMockEngine ? createMockEngine() : null;

interface MockControls {
  failNext(command: string, errorMessage: string): void;
  reset(): void;
}

type GraniteMockWindow = Window & { __graniteMock?: MockControls };

interface MockColumn {
  name: string;
  type: string;
  notNull: boolean;
  pk: boolean;
}

interface MockTable {
  name: string;
  columns: MockColumn[];
  rows: string[][];
}

export type Result<T> = { ok: true; value: T } | { ok: false; error: string };

export async function call<T>(cmd: string, args?: Record<string, unknown>): Promise<Result<T>> {
  if (mockEngine) {
    return mockEngine.handle<T>(cmd, args);
  }
  try {
    const value = await invoke<T>(cmd, args);
    return { ok: true, value };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[Tauri] ${cmd} failed:`, error);
    return { ok: false, error: message };
  }
}

function createMockEngine() {
  const tables = new Map<string, MockTable>();
  let currentPath: string | null = null;
  const pendingErrors = new Map<string, string>();

  function registerControls(): void {
    if (typeof window === "undefined") {
      return;
    }
    const globalWindow = window as GraniteMockWindow;
    const controls: MockControls = {
      failNext(command, errorMessage) {
        pendingErrors.set(command, errorMessage);
      },
      reset() {
        pendingErrors.clear();
        tables.clear();
        currentPath = null;
      }
    };
    globalWindow.__graniteMock = controls;
  }

  registerControls();

  return {
    async handle<T>(cmd: string, args?: Record<string, unknown>): Promise<Result<T>> {
      const forcedError = pendingErrors.get(cmd);
      if (forcedError) {
        pendingErrors.delete(cmd);
        return { ok: false, error: forcedError } as Result<T>;
      }
      switch (cmd) {
        case "granitectl_info":
          return {
            ok: true,
            value: {
              path: "mock/granitectl",
              source: "system",
              exists: true,
              version: "mock-1.0.0"
            } as unknown as T
          };
        case "create_db": {
          const path = String(args?.path ?? "");
          currentPath = path;
          tables.clear();
          return { ok: true, value: undefined as unknown as T };
        }
        case "open_db": {
          const path = String(args?.path ?? "");
          if (!path) {
            return { ok: false, error: "Database path is required" };
          }
          currentPath = path;
          return { ok: true, value: undefined as unknown as T };
        }
        case "metadata": {
          if (!currentPath) {
            return { ok: false, error: "No database open" };
          }
          const payload = {
            database: currentPath,
            tables: Array.from(tables.values()).map((table) => ({
              name: table.name,
              rowCount: table.rows.length,
              columns: table.columns.map((col) => ({
                name: col.name,
                type: col.type,
                notNull: col.notNull,
                default: null,
                isPrimaryKey: col.pk
              })),
              indexes: [],
              foreignKeys: []
            }))
          };
          return { ok: true, value: JSON.stringify(payload) as unknown as T };
        }
        case "exec_sql": {
          const sql = String(args?.sql ?? "");
          const format = String(args?.format ?? "jsonRows");
          if (format !== "jsonRows") {
            return { ok: false, error: `Unsupported mock format ${format}` };
          }
          const result = mockExec(sql, tables);
          if (!result.ok) {
            return result as Result<T>;
          }
          return { ok: true, value: result.value as unknown as T };
        }
        case "explain_sql": {
          const plan = {
            nodeType: "SCAN",
            relationName: "mock",
            executionTimeMs: 0.01
          };
          return { ok: true, value: JSON.stringify(plan) as unknown as T };
        }
        case "export_csv":
          return { ok: true, value: undefined as unknown as T };
        default:
          return { ok: false, error: `Mock engine does not support ${cmd}` };
      }
    }
  };
}

function mockExec(sql: string, tables: Map<string, MockTable>): Result<{ format: string; output?: string; result?: { columns: string[]; rows: string[][]; durationMs: number; rowsAffected?: number; message?: string } }> {
  const trimmed = sql.trim();
  const lower = trimmed.toLowerCase();

  if (lower.startsWith("create table")) {
    const match = trimmed.match(/create\s+table\s+(\w+)\s*\((.+)\)/i);
    if (!match) {
      return { ok: false, error: "Unable to parse CREATE TABLE" };
    }
    const [, tableName, columnBlock] = match;
    const columns: MockColumn[] = columnBlock
      .split(",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map((definition) => {
        const [name, ...rest] = definition.split(/\s+/);
        const type = rest.join(" ").toUpperCase();
        return {
          name,
          type: rest[0] ?? "TEXT",
          notNull: type.includes("NOT NULL"),
          pk: type.includes("PRIMARY KEY")
        };
      });
    tables.set(tableName, { name: tableName, columns, rows: [] });
    return {
      ok: true,
      value: {
        format: "jsonRows",
        result: {
          columns: [],
          rows: [],
          durationMs: 1,
          message: `Table ${tableName} created`
        }
      }
    };
  }

  if (lower.startsWith("insert")) {
    const match = trimmed.match(/insert\s+into\s+(\w+)\s+values\s*\((.+)\)/i);
    if (!match) {
      return { ok: false, error: "Unable to parse INSERT" };
    }
    const [, tableName, valuesBlock] = match;
    const table = tables.get(tableName);
    if (!table) {
      return { ok: false, error: `Table ${tableName} does not exist` };
    }
    const values = valuesBlock
      .split(",")
      .map((part) => part.trim())
      .map((value) => value.replace(/^'(.+)'$/, "$1"));
    table.rows.push(values);
    return {
      ok: true,
      value: {
        format: "jsonRows",
        result: {
          columns: [],
          rows: [],
          durationMs: 1,
          rowsAffected: 1,
          message: "1 row inserted"
        }
      }
    };
  }

  if (lower.startsWith("select")) {
    const match = trimmed.match(/select\s+\*\s+from\s+(\w+)/i);
    if (!match) {
      return { ok: false, error: "Only SELECT * queries are supported in mock" };
    }
    const [, tableName] = match;
    const table = tables.get(tableName);
    if (!table) {
      return { ok: false, error: `Table ${tableName} does not exist` };
    }
    return {
      ok: true,
      value: {
        format: "jsonRows",
        result: {
          columns: table.columns.map((column) => column.name),
          rows: table.rows,
          durationMs: 1,
          rowsAffected: table.rows.length,
          message: `${table.rows.length} row(s)`
        }
      }
    };
  }

  if (lower.startsWith("commit")) {
    return {
      ok: true,
      value: {
        format: "jsonRows",
        result: {
          columns: [],
          rows: [],
          durationMs: 1,
          message: "Committed"
        }
      }
    };
  }

  return { ok: false, error: `Mock engine cannot execute: ${sql}` };
}
