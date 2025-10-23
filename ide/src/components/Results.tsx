import { useEffect, useMemo, useState } from "react";
import type { QueryResult } from "../state/db";

interface ResultsProps {
  result: QueryResult | null;
  active: boolean;
}

const PAGE_SIZE = 100;

export default function Results({ result, active }: ResultsProps) {
  const [page, setPage] = useState(0);

  useEffect(() => {
    setPage(0);
  }, [result]);

  const totalRows = result?.rows.length ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalRows / PAGE_SIZE));
  const rows = useMemo(() => {
    if (!result) {
      return [];
    }
    const start = page * PAGE_SIZE;
    return result.rows.slice(start, start + PAGE_SIZE);
  }, [page, result]);

  if (!result) {
    return (
      <div className="flex h-full items-center justify-center text-sm text-slate-500">
        Run a query to see results.
      </div>
    );
  }

  if (!active) {
    return null;
  }

  return (
    <div className="flex h-full flex-col overflow-hidden">
      <div className="flex-1 overflow-auto">
        <table className="min-w-full divide-y divide-slate-200 dark:divide-slate-700">
          <thead className="bg-slate-50 dark:bg-slate-800">
            <tr>
              {result.columns.map((col) => (
                <th key={col} className="px-3 py-2 text-left text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-300">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-200 bg-white text-sm dark:divide-slate-700 dark:bg-slate-900 dark:text-slate-100">
            {rows.map((row, rowIndex) => (
              <tr key={rowIndex} className="hover:bg-slate-50 dark:hover:bg-slate-800">
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex} className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-700 dark:text-slate-200">
                    {cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="flex items-center justify-between border-t border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
        <span>
          Showing {Math.min(totalRows, page * PAGE_SIZE + 1)}-{Math.min(totalRows, (page + 1) * PAGE_SIZE)} of {totalRows} rows
        </span>
        <div className="flex items-center gap-2">
          <button
            type="button"
            className="rounded border border-slate-300 px-2 py-1 text-xs font-medium text-slate-600 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
            onClick={() => setPage((value) => Math.max(0, value - 1))}
            disabled={page === 0}
          >
            Previous
          </button>
          <span>
            Page {page + 1} of {totalPages}
          </span>
          <button
            type="button"
            className="rounded border border-slate-300 px-2 py-1 text-xs font-medium text-slate-600 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
            onClick={() => setPage((value) => Math.min(totalPages - 1, value + 1))}
            disabled={page >= totalPages - 1}
          >
            Next
          </button>
        </div>
      </div>
    </div>
  );
}
