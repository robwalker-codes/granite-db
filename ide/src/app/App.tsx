import { useCallback, useState } from "react";
import { open, save } from "@tauri-apps/api/dialog";
import { writeText } from "@tauri-apps/api/clipboard";
import { Toaster, toast } from "react-hot-toast";
import Sidebar from "../components/Sidebar";
import SqlEditor from "../components/Editor";
import Results from "../components/Results";
import PlanView from "../components/PlanView";
import Toolbar from "../components/Toolbar";
import StatusBar from "../components/StatusBar";
import { SessionProvider, useSession } from "../state/session";
import { executeQuery, explainQuery, exportCsv, fetchMetadata, openDatabase, type DatabaseTable } from "../state/db";
import type { QueryResult } from "../state/db";

function AppShell() {
  const { state, setDbPath, setMetadata, setResult, setPlan, setEditorText, setStatus, setRunning, addRecentPath, setTheme, setActivePanel, setError } = useSession();
  const [search, setSearch] = useState("");

  const runQuery = useCallback(
    async (sql: string) => {
      if (!state.dbPath) {
        toast.error("Open a database first.");
        return;
      }
      const trimmed = sql.trim();
      if (!trimmed) {
        toast.error("Enter a statement to execute.");
        return;
      }
      setRunning(true);
      setError(null);
      try {
        const result = await executeQuery(state.dbPath, trimmed);
        setResult(result);
        setPlan(null);
        setActivePanel("results");
        const message = result.message ?? "Query executed";
        setStatus(message, result.durationMs, result.rows.length);
        if (result.columns.length === 0 && result.rowsAffected) {
          toast.success(`${message} (${result.rowsAffected} rows affected)`);
        } else {
          toast.success(message);
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        toast.error(message);
      } finally {
        setRunning(false);
      }
    },
    [setActivePanel, setError, setPlan, setResult, setRunning, setStatus, state.dbPath]
  );

  const explain = useCallback(
    async (sql: string) => {
      if (!state.dbPath) {
        toast.error("Open a database first.");
        return;
      }
      const trimmed = sql.trim();
      if (!trimmed) {
        toast.error("Enter a statement to explain.");
        return;
      }
      setRunning(true);
      setError(null);
      try {
        const plan = await explainQuery(state.dbPath, trimmed);
        setPlan(plan);
        setActivePanel("plan");
        setStatus("Explain plan generated", null, null);
        toast.success("Plan ready");
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        setError(message);
        toast.error(message);
      } finally {
        setRunning(false);
      }
    },
    [setActivePanel, setError, setPlan, setRunning, setStatus, state.dbPath]
  );

  const handleOpen = useCallback(async () => {
    const selected = await open({
      multiple: false,
      filters: [{ name: "Granite Database", extensions: ["gdb", "db"] }]
    });
    const path = typeof selected === "string" ? selected : Array.isArray(selected) ? selected[0] : null;
    if (!path) {
      return;
    }
    try {
      await openDatabase(path);
      const metadata = await fetchMetadata(path);
      setDbPath(path);
      setMetadata(metadata);
      await addRecentPath(path);
      setStatus(`Opened ${path}`, null, null);
      toast.success(`Opened ${path}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    }
  }, [addRecentPath, setDbPath, setMetadata, setStatus]);

  const handleSelectRecent = useCallback(
    async (path: string) => {
      try {
        await openDatabase(path);
        const metadata = await fetchMetadata(path);
        setDbPath(path);
        setMetadata(metadata);
        await addRecentPath(path);
        setStatus(`Opened ${path}`, null, null);
        toast.success(`Opened ${path}`);
      } catch (err) {
        toast.error(err instanceof Error ? err.message : String(err));
      }
    },
    [addRecentPath, setDbPath, setMetadata, setStatus]
  );

  const handleSelectTable = useCallback(
    (table: DatabaseTable) => {
      const sql = `SELECT * FROM ${table.name} LIMIT 100;`;
      setEditorText(sql);
      toast("Sample query inserted", { icon: "ℹ️" });
    },
    [setEditorText]
  );

  const handleExportCsv = useCallback(async () => {
    if (!state.dbPath) {
      toast.error("Open a database first.");
      return;
    }
    const sql = state.editorText.trim();
    if (!sql) {
      toast.error("Enter a statement to export.");
      return;
    }
    const destination = await save({
      filters: [{ name: "CSV", extensions: ["csv"] }],
      defaultPath: "granite-results.csv"
    });
    if (!destination || Array.isArray(destination)) {
      return;
    }
    setRunning(true);
    try {
      await exportCsv(state.dbPath, sql, destination);
      setStatus(`CSV exported to ${destination}`, null, null);
      toast.success(`CSV written to ${destination}`);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : String(err));
    } finally {
      setRunning(false);
    }
  }, [setRunning, state.dbPath, state.editorText]);

  const handleCopyResults = useCallback(async (result: QueryResult | null) => {
    if (!result) {
      toast.error("No results to copy.");
      return;
    }
    const header = result.columns.join("\t");
    const lines = result.rows.map((row) => row.join("\t"));
    await writeText([header, ...lines].join("\n"));
    toast.success("Results copied to clipboard");
  }, []);

  const metadata = state.metadata;
  const editorError = state.error;

  const handleThemeToggle = useCallback(() => {
    setTheme(state.theme === "dark" ? "light" : "dark");
  }, [setTheme, state.theme]);

  return (
    <div className="flex h-screen flex-col">
      <Toolbar
        onRun={() => runQuery(state.editorText)}
        onExplain={() => explain(state.editorText)}
        onExport={handleExportCsv}
        onOpen={handleOpen}
        onSelectRecent={handleSelectRecent}
        isRunning={state.isRunning}
        dbPath={state.dbPath}
        recentFiles={state.recentFiles}
        theme={state.theme}
        onToggleTheme={handleThemeToggle}
      />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar metadata={metadata} search={search} onSearchChange={setSearch} onSelectTable={handleSelectTable} />
        <main className="flex flex-1 flex-col">
          <div className="grid h-full grid-rows-[minmax(0,1fr)_minmax(0,1fr)]">
            <SqlEditor
              value={state.editorText}
              onChange={setEditorText}
              onRun={runQuery}
              onExplain={explain}
              theme={state.theme}
              errorMessage={editorError}
            />
            <div className="grid grid-cols-1 md:grid-cols-2">
              <div className={state.activePanel === "results" ? "block" : "hidden md:block"}>
                <Results result={state.result} active={state.activePanel === "results"} />
                <div className="flex justify-end border-t border-slate-200 bg-white px-3 py-2 text-xs dark:border-slate-700 dark:bg-slate-900">
                  <button
                    type="button"
                    className="rounded border border-slate-300 px-2 py-1 text-xs font-medium text-slate-600 transition hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                    onClick={() => handleCopyResults(state.result)}
                    disabled={!state.result}
                  >
                    Copy
                  </button>
                </div>
              </div>
              <PlanView plan={state.plan} active={state.activePanel === "plan"} />
            </div>
          </div>
        </main>
      </div>
      <StatusBar
        message={state.statusMessage}
        durationMs={state.lastDurationMs}
        rowCount={state.rowCount}
        dbPath={state.dbPath}
        theme={state.theme}
      />
      <Toaster position="bottom-right" toastOptions={{ duration: 3000 }} />
    </div>
  );
}

export default function App() {
  return (
    <SessionProvider>
      <AppShell />
    </SessionProvider>
  );
}
