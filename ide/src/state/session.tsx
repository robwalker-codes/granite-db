import { Store } from "@tauri-apps/plugin-store";
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import type { DatabaseMetadata, QueryResult } from "./db";
import type { ExplainPayload } from "../lib/planTypes";
import { fetchGraniteCtlInfo, type GraniteCtlInfo as EngineInfo } from "../lib/engine/info";
import { resolveGraniteCtlPath } from "../lib/engine/paths";
import { initialiseTheme, setTheme as updateTheme, subscribe as subscribeToTheme, type Theme } from "./theme/ThemeService";

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
  isOpening: boolean;
  theme: Theme;
  activePanel: "results" | "plan";
  error: string | null;
  engineInfo: EngineInfo | null;
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
  setOpening(opening: boolean): void;
  setTheme(theme: Theme): Promise<void>;
  setActivePanel(panel: "results" | "plan"): void;
  setError(message: string | null): void;
  setEngineInfo(info: EngineInfo | null): void;
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
  isOpening: false,
  theme: "light",
  activePanel: "results",
  error: null,
  engineInfo: null
};

const SessionContext = createContext<SessionContextValue | undefined>(undefined);

const settingsStore = new Store("granite-ide.settings.dat");

export const SessionProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [state, setState] = useState<SessionState>(defaultState);

  useEffect(() => {
    let unsubscribe: (() => void) | undefined;
    async function load() {
      try {
        const [recent, theme] = await Promise.all([
          settingsStore.get<string[]>("recentFiles"),
          initialiseTheme()
        ]);
        setState((prev) => ({
          ...prev,
          ready: true,
          recentFiles: recent ?? [],
          theme
        }));
      } catch (error) {
        console.error("Failed to initialise session", error);
        setState((prev) => ({ ...prev, ready: true }));
      } finally {
        unsubscribe = subscribeToTheme((theme) => {
          setState((prev) => ({ ...prev, theme }));
        });
      }
    }

    load().catch((error) => {
      console.error("Failed to initialise session", error);
    });

    return () => {
      if (unsubscribe) {
        unsubscribe();
      }
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function verify() {
      const info = await fetchGraniteCtlInfo();
      if (cancelled) {
        return;
      }
      if (info.ok) {
        setState((prev) => ({ ...prev, engineInfo: info.value }));
      } else {
        console.error("[Engine] Failed to verify granitectl", info.error);
        setState((prev) => ({
          ...prev,
          engineInfo: {
            path: resolveGraniteCtlPath(),
            source: "unverified",
            exists: false,
            error: info.error
          }
        }));
      }
    }

    verify().catch((error) => {
      console.error("[Engine] Verification threw", error);
    });

    return () => {
      cancelled = true;
    };
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

  const setOpening = useCallback((opening: boolean) => {
    setState((prev) => ({ ...prev, isOpening: opening }));
  }, []);

  const setTheme = useCallback(async (theme: Theme) => {
    await updateTheme(theme);
    setState((prev) => ({ ...prev, theme }));
  }, []);

  const setActivePanel = useCallback((panel: "results" | "plan") => {
    setState((prev) => ({ ...prev, activePanel: panel }));
  }, []);

  const setError = useCallback((message: string | null) => {
    setState((prev) => ({ ...prev, error: message }));
  }, []);

  const setEngineInfo = useCallback((info: EngineInfo | null) => {
    setState((prev) => ({ ...prev, engineInfo: info }));
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
      setOpening,
      setTheme,
      setActivePanel,
      setError,
      setEngineInfo
    }),
    [
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
      setOpening,
      setTheme,
      setActivePanel,
      setError,
      setEngineInfo
    ]
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
