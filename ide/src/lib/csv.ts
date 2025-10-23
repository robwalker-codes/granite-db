import type { QueryResult } from "../state/db";

function escapeCell(value: string): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (/[,"\n]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function toCsv(result: QueryResult): string {
  const lines: string[] = [];
  if (result.columns.length > 0) {
    lines.push(result.columns.map(escapeCell).join(","));
  }
  for (const row of result.rows) {
    lines.push(row.map(escapeCell).join(","));
  }
  return lines.join("\n");
}
