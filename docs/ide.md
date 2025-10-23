# Granite IDE

The Granite IDE is a desktop companion for GraniteDB. It is built with Tauri, Vite, React, and TypeScript, and it talks directly to the engine by spawning `granitectl`. This document summarises the setup, available features, and tips for day-to-day development.

## Prerequisites

1. Install Node.js 20 LTS (`>=20 <23`). Tools such as `nvm` make switching versions simple.
2. Build the engine so that `granitectl` is available. Either place the binary on your `PATH` or set the `GRANITECTL_PATH` environment variable before launching the IDE.

## Installing dependencies

```bash
cd ide
npm install
```

## Running the IDE

### Development mode

```bash
npm run tauri dev
```

This command starts the Vite development server and launches the Tauri shell with live reload. Window state, theme, and the list of recently opened databases persist across sessions via the Tauri store plugin.

### Release build

```bash
npm run tauri build
```

Tauri produces native bundles for macOS, Windows, and Linux.

## Core features

* **File menu and recents** – open a `.gdb` database through the File → Open dialogue. The ten most recent databases are stored locally and exposed in the toolbar for quick access.
* **Schema explorer** – powered by `granitectl meta --json`, the sidebar lists tables, columns, indexes, and foreign keys. Selecting a table injects `SELECT * FROM <table> LIMIT 100;` into the editor and the tree updates automatically after DDL statements.
* **SQL editor** – Monaco provides SQL syntax highlighting, snippets, and keyboard shortcuts. Use <kbd>Ctrl</kbd>+<kbd>Enter</kbd> (<kbd>⌘</kbd>+<kbd>Enter</kbd> on macOS) to run the current statement and <kbd>Ctrl</kbd>+<kbd>L</kbd> (<kbd>⌘</kbd>+<kbd>L</kbd>) to open an EXPLAIN plan. Errors surface as in-editor decorations and toast notifications so the session remains usable even after failures.
* **Results grid** – query output is rendered with pagination, copy-to-clipboard support, and row counts. CSV export runs through the Tauri bridge for consistent formatting.
* **Plan view** – EXPLAIN plans from `granitectl explain --json` appear as expandable operator cards that highlight join types, predicates, and index usage. See [docs/plan-json.md](./plan-json.md) for the payload schema.
* **Status bar** – displays the last action, row count, duration, active database path, and the current theme. Long-running queries show a non-blocking spinner so the UI stays responsive.
* **Theme toggle** – switch between light and dark modes from the toolbar. Preferences are saved via the store plugin and Monaco adopts the chosen palette immediately.
* **Resilient engine bridge** – all Tauri invocations route through a gateway that converts failures into friendly errors. The schema explorer guards against malformed responses so unexpected CLI output no longer blanks the UI.

## CSV export

Exports stream through `granitectl exec --format csv`. Choose “Export CSV” on the toolbar, provide a destination, and the IDE writes the file locally. The status bar confirms completion once the operation finishes.

## JSON execution and metadata

The IDE relies on three JSON endpoints provided by `granitectl`:

* `granitectl exec --format json -q "…"` – returns machine-readable result sets for the grid.
* `granitectl meta --json <dbfile>` – produces the schema explorer payload shown above.
* `granitectl explain --json -q "…"` – emits plan JSON for the visualiser.

## Keyboard shortcuts

* <kbd>Ctrl</kbd>+<kbd>Enter</kbd> / <kbd>⌘</kbd>+<kbd>Enter</kbd> – execute the current selection or the entire editor contents.
* <kbd>Ctrl</kbd>+<kbd>L</kbd> / <kbd>⌘</kbd>+<kbd>L</kbd> – run EXPLAIN and display the plan view.
* <kbd>Ctrl</kbd>+<kbd>Shift</kbd>+<kbd>P</kbd> – open Monaco’s command palette for additional editor commands.

## Troubleshooting

* If the IDE cannot find `granitectl`, set `GRANITECTL_PATH` or ensure the binary is on the system `PATH` before launching. The toolbar shows a banner when resolution fails.
* Queries running longer than 60 seconds are terminated and reported as timeouts. Optimise the SQL and retry.
* Window layout, theme, and recents live in `granite-ide.settings.dat` within the Tauri store. Delete the file to reset the session state if necessary.

## Testing the IDE

```bash
cd ide
npm run test    # Vitest unit tests
npm run e2e     # Playwright suite using the mock engine (set VITE_ENABLE_E2E_MOCKS=true)
```

The Playwright suite opens a mock database, exercises schema refresh behaviour, checks dark mode integration with Monaco, and asserts that failed engine calls produce toasts rather than blanking the window.

## Manual validation checklist

```bash
# From the repository root
cd ide
npm install
npm run dev      # Vite serves the frontend on http://localhost:5173/
npm run tauri dev
# Confirm:
# - Tauri validates its configuration without warnings.
# - The window launches and renders the Vite app.
# - File Open/Save dialogues operate correctly.
# - Settings persist between sessions.
# - Clipboard copy works where expected.
```
