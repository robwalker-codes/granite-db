interface StatusBarProps {
  message: string | null;
  durationMs: number | null;
  rowCount: number | null;
  dbPath: string | null;
  theme: "light" | "dark";
}

export default function StatusBar({ message, durationMs, rowCount, dbPath, theme }: StatusBarProps) {
  return (
    <footer className="flex items-center justify-between border-t border-slate-200 bg-white px-4 py-2 text-xs text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
      <div className="flex items-center gap-3">
        {message && <span>{message}</span>}
        {typeof rowCount === "number" && <span>{rowCount} row(s)</span>}
        {typeof durationMs === "number" && <span>{durationMs} ms</span>}
      </div>
      <div className="flex items-center gap-3">
        {dbPath && <span className="truncate" title={dbPath}>{dbPath}</span>}
        <span>{theme === "dark" ? "Dark" : "Light"} theme</span>
      </div>
    </footer>
  );
}
