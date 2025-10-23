import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { configDefaults } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@tauri-apps/api/tauri": "@tauri-apps/api/core"
    }
  },
  build: {
    target: "es2020"
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./vitest.setup.ts",
    exclude: [...configDefaults.exclude, "tests/e2e/**"]
  }
});
