import { Store } from "@tauri-apps/plugin-store";
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { DatabaseMetadata, QueryResult } from "./db";
import type { ExplainPayload } from "../lib/planTypes";

type Theme = "light" | "dark";

export interface SessionState {
  ready: boolean;
  dbPath: string | null;
  recentFiles: string[];
  metadata: DatabaseMetadata | null;
  result: QueryResult | null;
  plan: ExplainPayload | null;
  editorText: string;
  statusMessage: string | null;
  lastDurationMs: number | null;
  rowCount: number | null;
  isRunning: boolean;
  theme: Theme;
  activePanel: "results" | "plan";
  error: string | null;
}

interface SessionContextValue {
  state: SessionState;
  setDbPath(path: string | null): void;
  setRecentFiles(paths: string[]): Promise<void>;
  addRecentPath(path: string): Promise<void>;
  setMetadata(meta: DatabaseMetadata | null): void;
  setResult(result: QueryResult | null): void;
  setPlan(plan: ExplainPayload | null): void;
  setEditorText(value: string): void;
  setStatus(message: string | null, durationMs: number | null, rowCount: number | null): void;
  setRunning(running: boolean): void;
  setTheme(theme: Theme): Promise<void>;
  setActivePanel(panel: "results" | "plan"): void;
  setError(message: string | null): void;
}

const defaultState: SessionState = {
  ready: false,
  dbPath: null,
  recentFiles: [],
  metadata: null,
  result: null,
  plan: null,
  editorText: "-- Welcome to Granite IDE\nSELECT 1;",
  statusMessage: null,
  lastDurationMs: null,
  rowCount: null,
  isRunning: false,
  theme: "light",
  activePanel: "results",
  error: null
};

const SessionContext = createContext<SessionContextValue | undefined>(undefined);

const settingsStore = new Store("granite-ide.settings.dat");

function applyTheme(theme: Theme) {
  if (typeof document === "undefined") {
    return;
  }
  const root = document.documentElement;
  if (theme === "dark") {
    root.classList.add("dark");
  } else {
    root.classList.remove("dark");
  }
}

export const SessionProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [state, setState] = useState<SessionState>(defaultState);

  useEffect(() => {
    async function load() {
      const [recent, theme] = await Promise.all([
        settingsStore.get<string[]>("recentFiles"),
        settingsStore.get<Theme>("theme")
      ]);
      const nextTheme: Theme = theme === "dark" ? "dark" : "light";
      applyTheme(nextTheme);
      setState((prev) => ({
        ...prev,
        ready: true,
        recentFiles: recent ?? [],
        theme: nextTheme
      }));
    }
    load().catch((err) => {
      console.error("Failed to load session store", err);
      setState((prev) => ({ ...prev, ready: true }));
    });
  }, []);

  const setDbPath = useCallback((path: string | null) => {
    setState((prev) => ({ ...prev, dbPath: path }));
  }, []);

  const setRecentFiles = useCallback(async (paths: string[]) => {
    const trimmed = paths.slice(0, 10);
    await settingsStore.set("recentFiles", trimmed);
    await settingsStore.save();
    setState((prev) => ({ ...prev, recentFiles: trimmed }));
  }, []);

  const addRecentPath = useCallback(async (path: string) => {
    let updated: string[] = [];
    setState((prev) => {
      const existing = prev.recentFiles.filter((item) => item !== path);
      updated = [path, ...existing].slice(0, 10);
      return { ...prev, recentFiles: updated };
    });
    if (updated.length > 0) {
      await settingsStore.set("recentFiles", updated);
      await settingsStore.save();
    }
  }, []);

  const setMetadata = useCallback((meta: DatabaseMetadata | null) => {
    setState((prev) => ({ ...prev, metadata: meta }));
  }, []);

  const setResult = useCallback((result: QueryResult | null) => {
    setState((prev) => ({ ...prev, result }));
  }, []);

  const setPlan = useCallback((plan: ExplainPayload | null) => {
    setState((prev) => ({ ...prev, plan }));
  }, []);

  const setEditorText = useCallback((value: string) => {
    setState((prev) => ({ ...prev, editorText: value }));
  }, []);

  const setStatus = useCallback((message: string | null, durationMs: number | null, rowCount: number | null) => {
    setState((prev) => ({ ...prev, statusMessage: message, lastDurationMs: durationMs, rowCount }));
  }, []);

  const setRunning = useCallback((running: boolean) => {
    setState((prev) => ({ ...prev, isRunning: running }));
  }, []);

  const setTheme = useCallback(async (theme: Theme) => {
    applyTheme(theme);
    await settingsStore.set("theme", theme);
    await settingsStore.save();
    setState((prev) => ({ ...prev, theme }));
  }, []);

  const setActivePanel = useCallback((panel: "results" | "plan") => {
    setState((prev) => ({ ...prev, activePanel: panel }));
  }, []);

  const setError = useCallback((message: string | null) => {
    setState((prev) => ({ ...prev, error: message }));
  }, []);

  const value = useMemo<SessionContextValue>(
    () => ({
      state,
      setDbPath,
      setRecentFiles,
      addRecentPath,
      setMetadata,
      setResult,
      setPlan,
      setEditorText,
      setStatus,
      setRunning,
      setTheme,
      setActivePanel,
      setError
    }),
    [state, setDbPath, setRecentFiles, addRecentPath, setMetadata, setResult, setPlan, setEditorText, setStatus, setRunning, setTheme, setActivePanel, setError]
  );

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
};

export function useSession(): SessionContextValue {
  const ctx = useContext(SessionContext);
  if (!ctx) {
    throw new Error("useSession must be used within SessionProvider");
  }
  return ctx;
}

export type { Theme };
