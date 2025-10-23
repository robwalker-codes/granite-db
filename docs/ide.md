# Granite IDE

The Granite IDE is a cross-platform desktop companion for GraniteDB. It is built with Tauri, Vite, React, and TypeScript, and it talks directly to the engine by spawning `granitectl`. No HTTP server or external middleware is required.

## Installation

> **Prerequisite:** Install Node.js 20 LTS (any version `>=20 <23`). Use `nvm` or your package manager to pin the version before installing dependencies.

1. Ensure the Go engine is built so that `granitectl` is available on your `PATH`. Alternatively, set the `GRANITECTL_PATH` environment variable to point to the executable before launching the IDE.
2. Install the JavaScript dependencies:

   ```bash
   cd ide
   npm install
   ```

## Running the IDE

### Development mode

```bash
npm run tauri dev
```

This starts the Vite development server and launches the Tauri shell with live reload. Window size, position, theme, and the list of recently opened databases persist between runs via the Tauri store plugin.

### Release build

```bash
npm run tauri build
```

Tauri produces native bundles for macOS, Windows, and Linux.

## Features

* **File menu and recents** – open a `.gdb` database through the File → Open dialogue. The ten most recent databases are stored locally and shown in the drop-down on the toolbar.
* **Schema explorer** – browse tables, columns, indexes, and foreign keys in the sidebar. Clicking a table injects `SELECT * FROM <table> LIMIT 100;` into the editor for quick inspection.
* **SQL editor** – Monaco provides SQL syntax highlighting, snippets, and keyboard shortcuts. Use <kbd>Ctrl</kbd>+<kbd>Enter</kbd> (<kbd>⌘</kbd>+<kbd>Enter</kbd> on macOS) to execute and <kbd>Ctrl</kbd>+<kbd>L</kbd> (<kbd>⌘</kbd>+<kbd>L</kbd>) to run EXPLAIN. Errors appear inline beneath the editor and as toasts.
* **Results grid** – the bottom pane renders query output with paging controls, copy-to-clipboard support, and row counts. CSV export is available from the toolbar and writes through the Rust/Tauri bridge for consistent formatting.
* **Plan view** – EXPLAIN plans are visualised as expandable operator cards that surface key metadata such as join type, predicate, and index usage. The raw plan text appears above the interactive tree. See [docs/plan-json.md](./plan-json.md) for the JSON schema.
* **Status bar** – displays the last action, row count, duration, active database path, and the current colour scheme.
* **Theme toggle** – switch between light and dark styles from the toolbar. The preference is saved in the Tauri store.

## CSV export

Exports are streamed via `granitectl exec --format csv`. Choose “Export CSV” on the toolbar, pick a destination, and the IDE will materialise the file locally. The status bar confirms the target path once the operation completes.

## JSON execution and metadata

The IDE relies on the enhanced CLI support added in Stage 8:

* `granitectl exec --format json -q "…"` returns machine-readable result sets for the grid.
* `granitectl meta --json <dbfile>` produces the schema explorer payload.
* `granitectl explain --json -q "…"` emits the Stage 7 plan JSON for the visualiser.

These commands are surfaced through the Tauri command handlers; the IDE validates inputs and enforces a 60-second timeout so runaway queries can be interrupted cleanly.

## Keyboard shortcuts

* <kbd>Ctrl</kbd>+<kbd>Enter</kbd> / <kbd>⌘</kbd>+<kbd>Enter</kbd> – execute the current selection or the entire editor contents.
* <kbd>Ctrl</kbd>+<kbd>L</kbd> / <kbd>⌘</kbd>+<kbd>L</kbd> – run EXPLAIN and show the plan view.
* <kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>P</kbd> – toggle the command palette provided by Monaco (useful for additional editor commands).

## Changelog

* 2025-10-23 – Migrated the desktop shell to Tauri v2, introduced explicit capability files, and standardised on Node.js 20 LTS for local development.

## Troubleshooting

* If the IDE cannot find `granitectl`, set `GRANITECTL_PATH` or ensure the binary is on the system `PATH` before launching.
* Queries that run for longer than 60 seconds are terminated and reported as timeouts; adjust the SQL and re-run.
* Window layout, theme, and recents are stored in `granite-ide.settings.dat` within the Tauri store. Delete the file to reset the session state if necessary.

## Post-change validation

```bash
# From repo root
cd ide
npm install
npm run dev      # Vite prints:  http://localhost:5173/
npm run tauri dev
# Expect:
# - Tauri validates config (no schema errors)
# - It launches the window and loads the Vite app
# - File Open/Save dialogs work (no permission errors)
# - Settings persist (store/window-state work)
# - Clipboard copy works if used
```
