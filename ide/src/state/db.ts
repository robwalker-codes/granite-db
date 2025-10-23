import { invoke } from "@tauri-apps/api/tauri";
import type { ExplainPayload } from "../lib/planTypes";

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

export interface ExecResponse {
  format: "table" | "csv" | "jsonRows";
  output?: string;
  result?: QueryResult;
}

export async function openDatabase(path: string): Promise<void> {
  await invoke("open_db", { path });
}

export async function executeQuery(path: string, sql: string): Promise<QueryResult> {
  const response = await invoke<ExecResponse>("exec_sql", {
    path,
    sql,
    format: "jsonRows"
  });
  if (!response || response.format !== "jsonRows" || !response.result) {
    throw new Error("Unexpected response from engine");
  }
  return response.result;
}

export async function explainQuery(path: string, sql: string): Promise<ExplainPayload> {
  const payload = await invoke<string>("explain_sql", { path, sql });
  return JSON.parse(payload) as ExplainPayload;
}

export async function fetchMetadata(path: string): Promise<DatabaseMetadata> {
  const payload = await invoke<string>("metadata", { path });
  return JSON.parse(payload) as DatabaseMetadata;
}

export async function exportCsv(path: string, sql: string, outPath: string): Promise<void> {
  await invoke("export_csv", { path, sql, outPath });
}
