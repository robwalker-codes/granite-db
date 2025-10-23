import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { flushSync } from "react-dom";

// Debounce automatic schema refreshes to avoid hammering the engine after bursts of events.
const METADATA_REFRESH_DEBOUNCE_MS = 400;
// Delay the spinner reset long enough for the UI to reflect the loading state even when the engine
// responds immediately (React batches synchronous state updates otherwise).
const SCHEMA_REFRESH_RESET_DELAY_MS = 300;
import { open, save } from "@tauri-apps/plugin-dialog";
import { writeText } from "@tauri-apps/plugin-clipboard-manager";
import { Toaster, toast } from "react-hot-toast";
import Sidebar from "../components/Sidebar";
import SqlEditor from "../components/Editor";
import Results from "../components/Results";
import PlanView from "../components/PlanView";
import Toolbar from "../components/Toolbar";
import StatusBar from "../components/StatusBar";
import AppErrorBoundary from "../components/framework/AppErrorBoundary";
import { SessionProvider, useSession } from "../state/session";
import {
  createDatabase,
  executeQuery,
  explainQuery,
  exportCsv,
  fetchMetadata,
  openDatabase,
  isDdlStatement,
  isCommitStatement,
  type DatabaseMetadata,
  type DatabaseTable,
  type QueryResult
} from "../state/db";
import { publish, subscribe } from "../state/events/DomainEvents";

function AppShell() {
  const {
    state,
    setDbPath,
    setMetadata,
    setResult,
    setPlan,
    setEditorText,
    setStatus,
    setRunning,
    addRecentPath,
    setTheme,
    setActivePanel,
    setError,
    setOpening
  } = useSession();
  const [search, setSearch] = useState("");
  const [isSchemaRefreshing, setSchemaRefreshing] = useState(false);
  const [isManualRefreshPending, setManualRefreshPending] = useState(false);
  const refreshTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const schemaResetTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const manualResetTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const disposedRef = useRef(false);
  const isE2EMock = import.meta.env.VITE_ENABLE_E2E_MOCKS === "true";

  useEffect(() => {
    disposedRef.current = false;
    return () => {
      disposedRef.current = true;
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
      }
      if (schemaResetTimerRef.current) {
        clearTimeout(schemaResetTimerRef.current);
      }
      if (manualResetTimerRef.current) {
        clearTimeout(manualResetTimerRef.current);
      }
    };
  }, []);

  const handleMetadataRefresh = useCallback(
    async (path: string): Promise<boolean> => {
      if (!path || disposedRef.current) {
        return false;
      }
      flushSync(() => {
        setSchemaRefreshing(true);
      });
      await new Promise((resolve) => setTimeout(resolve, 0));
      try {
        const result = await fetchMetadata(path);
        if (!result.ok) {
          toast.error(`Could not refresh schema: ${result.error}`);
          console.error("[Schema] Refresh failed", result.error);
          return false;
        }
        if (disposedRef.current) {
          return false;
        }
        setMetadata(result.value);
        return true;
      } finally {
        if (schemaResetTimerRef.current) {
          clearTimeout(schemaResetTimerRef.current);
        }
        schemaResetTimerRef.current = setTimeout(() => {
          if (!disposedRef.current) {
            flushSync(() => {
              setSchemaRefreshing(false);
            });
          }
        }, SCHEMA_REFRESH_RESET_DELAY_MS);
      }
    },
    [setMetadata]
  );

  const scheduleMetadataRefresh = useCallback(
    (path: string) => {
      if (!path) {
        return;
      }
      if (refreshTimerRef.current) {
        clearTimeout(refreshTimerRef.current);
      }
      refreshTimerRef.current = setTimeout(() => {
        refreshTimerRef.current = null;
        void handleMetadataRefresh(path);
      }, METADATA_REFRESH_DEBOUNCE_MS);
    },
    [handleMetadataRefresh]
  );

  useEffect(() => {
    if (!state.dbPath) {
      return undefined;
    }
    const stopDdl = subscribe("DDL_CHANGED", (event) => {
      if (event.db === state.dbPath) {
        scheduleMetadataRefresh(event.db);
      }
    });
    const stopCommit = subscribe("TX_COMMIT", (event) => {
      if (event.db === state.dbPath) {
        scheduleMetadataRefresh(event.db);
      }
    });
    return () => {
      stopDdl();
      stopCommit();
    };
  }, [scheduleMetadataRefresh, state.dbPath]);

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
        if (!result.ok) {
          setError(result.error);
          toast.error(result.error);
          return;
        }
        const payload = result.value;
        setResult(payload);
        setPlan(null);
        setActivePanel("results");
        const message = payload.message ?? "Query executed";
        setStatus(message, payload.durationMs, payload.rows.length);
        if (payload.columns.length === 0 && payload.rowsAffected) {
          const rowsAffected = payload.rowsAffected;
          toast.success(
            `${message} (${rowsAffected} row${rowsAffected === 1 ? "" : "s"} affected)`
          );
        } else {
          toast.success(message);
        }
        if (isDdlStatement(trimmed) && state.dbPath) {
          publish({ type: "DDL_CHANGED", db: state.dbPath });
        } else if (isCommitStatement(trimmed) && state.dbPath) {
          publish({ type: "TX_COMMIT", db: state.dbPath });
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
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
        const planResult = await explainQuery(state.dbPath, trimmed);
        if (!planResult.ok) {
          setError(planResult.error);
          toast.error(planResult.error);
          return;
        }
        setPlan(planResult.value);
        setActivePanel("plan");
        setStatus("Explain plan generated", null, null);
        toast.success("Plan ready");
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        setError(message);
        toast.error(message);
      } finally {
        setRunning(false);
      }
    },
    [setActivePanel, setError, setPlan, setRunning, setStatus, state.dbPath]
  );

  const openDatabaseAtPath = useCallback(
    async (path: string, prefix: "Opened" | "Created") => {
      if (!path) {
        toast.error("Database path is required.");
        return false;
      }
      setOpening(true);
      setStatus(`${prefix === "Created" ? "Creating" : "Opening"} ${path}…`, null, null);
      setError(null);
      try {
        if (prefix === "Created") {
          const createResult = await createDatabase(path);
          if (!createResult.ok) {
            toast.error(createResult.error);
            setStatus(createResult.error, null, null);
            return false;
          }
        }
        const openResult = await openDatabase(path);
        if (!openResult.ok) {
          toast.error(openResult.error);
          setStatus(openResult.error, null, null);
          return false;
        }
        setDbPath(path);
        setResult(null);
        setPlan(null);
        setMetadata(null);
        const metadataLoaded = await handleMetadataRefresh(path);
        if (!metadataLoaded) {
          setStatus(`Unable to load metadata for ${path}`, null, null);
          return false;
        }
        await addRecentPath(path);
        setStatus(`${prefix} ${path}`, null, null);
        toast.success(`${prefix} ${path}`);
        publish({ type: "DB_OPENED", path });
        return true;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        toast.error(message);
        setStatus(message, null, null);
        return false;
      } finally {
        setOpening(false);
      }
    },
    [addRecentPath, handleMetadataRefresh, setDbPath, setError, setMetadata, setOpening, setPlan, setResult, setStatus]
  );

  const handleOpen = useCallback(async () => {
    if (isE2EMock) {
      await openDatabaseAtPath("/mock/sample.gdb", "Opened");
      return;
    }
    const selected = await open({
      multiple: false,
      filters: [{ name: "Granite Database", extensions: ["gdb", "db"] }]
    });
    const path = typeof selected === "string" ? selected : Array.isArray(selected) ? selected[0] : null;
    if (!path) {
      return;
    }
    await openDatabaseAtPath(path, "Opened");
  }, [isE2EMock, openDatabaseAtPath]);

  const handleCreateDatabase = useCallback(async () => {
    if (isE2EMock) {
      await openDatabaseAtPath("/mock/new-db.gdb", "Created");
      return;
    }
    const destination = await save({
      defaultPath: "granite-db.gdb",
      filters: [{ name: "Granite Database", extensions: ["gdb", "db"] }]
    });
    if (!destination || Array.isArray(destination)) {
      return;
    }
    await openDatabaseAtPath(destination, "Created");
  }, [isE2EMock, openDatabaseAtPath]);

  const handleSelectRecent = useCallback(
    async (path: string) => {
      await openDatabaseAtPath(path, "Opened");
    },
    [openDatabaseAtPath]
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
    if (isE2EMock) {
      toast.success("CSV written (mock)");
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
      const result = await exportCsv(state.dbPath, sql, destination);
      if (!result.ok) {
        toast.error(result.error);
        return;
      }
      setStatus(`CSV exported to ${destination}`, null, null);
      toast.success(`CSV written to ${destination}`);
    } finally {
      setRunning(false);
    }
  }, [isE2EMock, setRunning, setStatus, state.dbPath, state.editorText]);

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

  const refreshSchemaNow = useCallback(async () => {
    if (!state.dbPath) {
      toast.error("Open a database first.");
      return;
    }
    flushSync(() => {
      setManualRefreshPending(true);
    });
    void (async () => {
      try {
        await handleMetadataRefresh(state.dbPath);
      } finally {
        if (manualResetTimerRef.current) {
          clearTimeout(manualResetTimerRef.current);
        }
        manualResetTimerRef.current = setTimeout(() => {
          if (!disposedRef.current) {
            flushSync(() => {
              setManualRefreshPending(false);
            });
          }
        }, SCHEMA_REFRESH_RESET_DELAY_MS);
      }
    })();
  }, [handleMetadataRefresh, state.dbPath]);

  const handleThemeToggle = useCallback(() => {
    setTheme(state.theme === "dark" ? "light" : "dark");
  }, [setTheme, state.theme]);

  const engineBanner = useMemo(() => {
    const info = state.engineInfo;
    if (!info) {
      return null;
    }
    if (info.exists && !info.error) {
      return null;
    }
    const guidance =
      "Build the engine (cd engine && go build ./...) or set GRANITECTL_PATH to the granitectl binary.";
    const prefix = info.error ?? `granitectl was not found at ${info.path}.`;
    return `${prefix} ${guidance}`;
  }, [state.engineInfo]);

  const engineVersion = state.engineInfo?.version ?? null;
  const metadata = state.metadata;
  const editorError = state.error;
  const isRefreshingUi = isSchemaRefreshing || isManualRefreshPending;
  const isBusy = state.isRunning || state.isOpening || isRefreshingUi;

  useEffect(() => {
    if (!isE2EMock || typeof window === "undefined") {
      return undefined;
    }
    const globalWindow = window as Window & {
      __graniteTest?: {
        setEditor(value: string): void;
        refreshMetadata(): Promise<boolean>;
        getMetadata(): DatabaseMetadata | null;
      };
    };
    const api = {
      setEditor(value: string) {
        setEditorText(value);
      },
      refreshMetadata() {
        if (state.dbPath) {
          return handleMetadataRefresh(state.dbPath);
        }
        return Promise.resolve(false);
      },
      getMetadata() {
        return state.metadata;
      }
    };
    globalWindow.__graniteTest = api;
    return () => {
      if (globalWindow.__graniteTest === api) {
        delete globalWindow.__graniteTest;
      }
    };
  }, [handleMetadataRefresh, isE2EMock, setEditorText, state.dbPath, state.metadata]);

  return (
    <div className="flex h-screen flex-col">
      {engineBanner && (
        <div className="border-b border-amber-300 bg-amber-100 px-4 py-2 text-sm text-amber-900 dark:border-amber-700 dark:bg-amber-900/40 dark:text-amber-100">
          {engineBanner}
        </div>
      )}
      <Toolbar
        onRun={() => runQuery(state.editorText)}
        onExplain={() => explain(state.editorText)}
        onExport={handleExportCsv}
        onOpen={handleOpen}
        onCreate={handleCreateDatabase}
        onSelectRecent={handleSelectRecent}
        isRunning={state.isRunning}
        isOpening={state.isOpening}
        dbPath={state.dbPath}
        recentFiles={state.recentFiles}
        theme={state.theme}
        onToggleTheme={handleThemeToggle}
      />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar
          metadata={metadata}
          search={search}
          onSearchChange={setSearch}
          onSelectTable={handleSelectTable}
          onRefresh={refreshSchemaNow}
          refreshing={isRefreshingUi}
        />
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
        isBusy={isBusy}
        engineVersion={engineVersion}
      />
      <Toaster position="bottom-right" toastOptions={{ duration: 3000 }} />
    </div>
  );
}

export default function App() {
  return (
    <AppErrorBoundary>
      <SessionProvider>
        <AppShell />
      </SessionProvider>
    </AppErrorBoundary>
  );
}
