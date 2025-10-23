interface StatusBarProps {
  message: string | null;
  durationMs: number | null;
  rowCount: number | null;
  dbPath: string | null;
  theme: "light" | "dark";
  isBusy: boolean;
  engineVersion: string | null;
}

export default function StatusBar({ message, durationMs, rowCount, dbPath, theme, isBusy, engineVersion }: StatusBarProps) {
  return (
    <footer className="flex items-center justify-between border-t border-slate-200 bg-white px-4 py-2 text-xs text-slate-600 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
      <div className="flex items-center gap-3 truncate">
        <div className="flex items-center gap-2">
          {isBusy && (
            <span className="h-2.5 w-2.5 animate-spin rounded-full border-[1.5px] border-brand-500 border-t-transparent" aria-hidden="true" />
          )}
          {message && <span className="truncate" title={message}>{message}</span>}
        </div>
        {typeof rowCount === "number" && <span>{rowCount} row(s)</span>}
        {typeof durationMs === "number" && <span>{durationMs} ms</span>}
      </div>
      <div className="flex items-center gap-3">
        {engineVersion && <span className="text-slate-500 dark:text-slate-400">granitectl {engineVersion}</span>}
        {dbPath && <span className="max-w-[18rem] truncate" title={dbPath}>{dbPath}</span>}
        <span>{theme === "dark" ? "Dark" : "Light"} theme</span>
      </div>
    </footer>
  );
}
