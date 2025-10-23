import { describe, expect, it, vi } from "vitest";

vi.mock("@tauri-apps/plugin-store", async () => {
  return await import("../../../__mocks__/@tauri-apps/plugin-store");
});

describe("ThemeService", () => {
  it("initialises to light when store is empty", async () => {
    vi.resetModules();
    const storeModule = (await import("@tauri-apps/plugin-store")) as unknown as {
      __resetStores(): void;
    };
    storeModule.__resetStores();
    const themeModule = await import("../ThemeService");
    const theme = await themeModule.initialiseTheme();
    expect(theme).toBe("light");
    expect(themeModule.getTheme()).toBe("light");
  });

  it("persists theme changes and notifies subscribers", async () => {
    vi.resetModules();
    const storeModule = (await import("@tauri-apps/plugin-store")) as unknown as {
      __resetStores(): void;
      __inspectStore(file: string): Map<string, unknown>;
    };
    storeModule.__resetStores();
    const themeModule = await import("../ThemeService");
    await themeModule.initialiseTheme();
    const listener = vi.fn();
    themeModule.subscribe(listener);
    await themeModule.setTheme("dark");
    expect(themeModule.getTheme()).toBe("dark");
    expect(listener).toHaveBeenCalledWith("light");
    expect(listener).toHaveBeenLastCalledWith("dark");
    const settings = storeModule.__inspectStore("granite-ide.settings.dat");
    expect(settings.get("ui.theme")).toBe("dark");
  });

  it("rehydrates persisted theme on initialise", async () => {
    vi.resetModules();
    const storeModule = (await import("@tauri-apps/plugin-store")) as unknown as {
      __resetStores(): void;
    };
    storeModule.__resetStores();
    let themeModule = await import("../ThemeService");
    await themeModule.initialiseTheme();
    await themeModule.setTheme("dark");

    vi.resetModules();
    const themeModuleReloaded = await import("../ThemeService");
    const theme = await themeModuleReloaded.initialiseTheme();
    expect(theme).toBe("dark");
    expect(themeModuleReloaded.getTheme()).toBe("dark");
  });
});
