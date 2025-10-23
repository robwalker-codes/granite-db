import { Store } from "@tauri-apps/plugin-store";

export type Theme = "light" | "dark";

type ThemeListener = (theme: Theme) => void;

const THEME_KEY = "ui.theme";
const store = new Store("granite-ide.settings.dat");
const listeners = new Set<ThemeListener>();
let currentTheme: Theme = "light";
let initialised = false;

function applyThemeToDocument(theme: Theme): void {
  if (typeof document === "undefined") {
    return;
  }
  const root = document.documentElement;
  if (theme === "dark") {
    root.classList.add("dark");
  } else {
    root.classList.remove("dark");
  }
  root.setAttribute("data-theme", theme);
}

async function loadPersistedTheme(): Promise<Theme> {
  try {
    const stored = await store.get<string>(THEME_KEY);
    if (stored === "dark" || stored === "light") {
      return stored;
    }
  } catch (error) {
    console.warn("[Theme] Failed to load theme from store", error);
  }
  return "light";
}

function notify(theme: Theme): void {
  for (const listener of listeners) {
    try {
      listener(theme);
    } catch (error) {
      console.error("[Theme] Listener failed", error);
    }
  }
}

export async function initialiseTheme(): Promise<Theme> {
  if (initialised) {
    return currentTheme;
  }
  const theme = await loadPersistedTheme();
  currentTheme = theme;
  applyThemeToDocument(theme);
  initialised = true;
  return theme;
}

export function getTheme(): Theme {
  return currentTheme;
}

export async function setTheme(theme: Theme): Promise<void> {
  if (theme === currentTheme) {
    return;
  }
  currentTheme = theme;
  applyThemeToDocument(theme);
  try {
    await store.set(THEME_KEY, theme);
    await store.save();
  } catch (error) {
    console.warn("[Theme] Failed to persist theme", error);
  }
  notify(theme);
}

export function subscribe(listener: ThemeListener): () => void {
  listeners.add(listener);
  try {
    listener(currentTheme);
  } catch (error) {
    console.error("[Theme] Initial listener notification failed", error);
  }
  return () => {
    listeners.delete(listener);
  };
}
