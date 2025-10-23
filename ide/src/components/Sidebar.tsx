import { useMemo } from "react";
import type { DatabaseMetadata, DatabaseTable } from "../state/db";

interface SidebarProps {
  metadata: DatabaseMetadata | null;
  search: string;
  onSearchChange(value: string): void;
  onSelectTable(table: DatabaseTable): void;
  onRefresh(): void;
  refreshing: boolean;
}

export default function Sidebar({ metadata, search, onSearchChange, onSelectTable, onRefresh, refreshing }: SidebarProps) {
  const tables = metadata?.tables ?? [];
  const filtered = useMemo(() => {
    if (!search) {
      return tables;
    }
    const lower = search.toLowerCase();
    return tables.filter((table) => {
      if (table.name.toLowerCase().includes(lower)) {
        return true;
      }
      return table.columns.some((col) => col.name.toLowerCase().includes(lower));
    });
  }, [tables, search]);

  return (
    <aside className="flex h-full w-72 flex-col border-r border-slate-200 bg-white dark:border-slate-700 dark:bg-slate-900">
      <div className="flex items-center gap-2 border-b border-slate-200 px-4 py-3 dark:border-slate-700">
        <input
          type="search"
          value={search}
          onChange={(event) => onSearchChange(event.target.value)}
          placeholder="Search tables"
          className="w-full rounded-md border border-slate-300 px-3 py-2 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-brand-500 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100"
        />
        <button
          type="button"
          onClick={onRefresh}
          disabled={refreshing}
          className="inline-flex items-center justify-center rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-600 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-60 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
        >
          {refreshing ? (
            <span className="flex items-center gap-1">
              <span className="h-3 w-3 animate-spin rounded-full border-[1.5px] border-brand-500 border-t-transparent" aria-hidden="true" />
              Refreshing
            </span>
          ) : (
            "Refresh"
          )}
        </button>
      </div>
      <div className="flex-1 overflow-y-auto px-2 py-3 text-sm text-slate-700 dark:text-slate-200">
        {filtered.length === 0 && <p className="px-2 text-xs text-slate-400">No tables found.</p>}
        {filtered.map((table) => (
          <div key={table.name} className="mb-3 rounded-md px-2 py-1 hover:bg-slate-100 dark:hover:bg-slate-800">
            <button
              type="button"
              className="flex w-full items-center justify-between text-left font-semibold text-slate-900 dark:text-slate-100"
              onClick={() => onSelectTable(table)}
            >
              <span>{table.name}</span>
              <span className="text-xs font-normal text-slate-400 dark:text-slate-500">{table.rowCount} row(s)</span>
            </button>
            <div className="mt-1 pl-3 text-xs text-slate-500 dark:text-slate-400">
              <p className="uppercase tracking-wide text-slate-400 dark:text-slate-500">Columns</p>
              <ul className="space-y-0.5">
                {table.columns.map((col) => (
                  <li key={col.name}>
                    <span className="font-medium text-slate-600 dark:text-slate-300">{col.name}</span>
                    <span className="ml-1 text-slate-400">{col.type}</span>
                    {col.pk && <span className="ml-1 text-amber-500">PK</span>}
                    {col.notNull && <span className="ml-1 text-emerald-500">NOT NULL</span>}
                  </li>
                ))}
              </ul>
              {table.indexes.length > 0 && (
                <div className="mt-2">
                  <p className="uppercase tracking-wide text-slate-400 dark:text-slate-500">Indexes</p>
                  <ul className="space-y-0.5">
                    {table.indexes.map((idx) => (
                      <li key={idx.name}>
                        <span className="font-medium text-slate-600 dark:text-slate-300">{idx.name}</span>
                        <span className="ml-1 text-slate-400">({idx.columns.join(", ")})</span>
                        {idx.unique && <span className="ml-1 text-emerald-500">UNIQUE</span>}
                      </li>
                    ))}
                  </ul>
                </div>
              )}
              {table.fks.length > 0 && (
                <div className="mt-2">
                  <p className="uppercase tracking-wide text-slate-400 dark:text-slate-500">Constraints</p>
                  <ul className="space-y-0.5">
                    {table.fks.map((fk) => (
                      <li key={fk.name}>
                        <span className="font-medium text-slate-600 dark:text-slate-300">{fk.name}</span>
                        <span className="ml-1 text-slate-400">
                          {fk.columns.join(", ")} → {fk.refTable}({fk.refColumns.join(", ")})
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          </div>
        ))}
      </div>
    </aside>
  );
}
