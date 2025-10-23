# GraniteDB

GraniteDB is a compact relational database engine written in Go with a cross-platform desktop IDE built on Tauri and React. The project is designed to be easy to build locally, explore, and extend. This guide introduces the main components, shows how to get them running, and highlights the most important commands for day-to-day work.

## Repository layout

* `engine/` – the Go-based storage engine, planner, and the `granitectl` command-line client.
* `ide/` – the Tauri desktop IDE that wraps `granitectl` and presents an editor, schema explorer, and result visualisations.
* `docs/` – reference material for the IDE and JSON payloads.

## Building the engine

GraniteDB requires Go 1.21 or newer.

```bash
cd engine
go build ./...
```

This produces the `granitectl` binary in the `engine/` directory. Point the IDE at this binary via the `GRANITECTL_PATH` environment variable if it is not on your `PATH`.

### Creating and inspecting a database

```bash
cd engine
./granitectl new demo.gdb
./granitectl exec -q "CREATE TABLE people(id INT PRIMARY KEY, name VARCHAR(50));" demo.gdb
./granitectl exec -q "INSERT INTO people VALUES (1, 'Ada'), (2, 'Grace');" demo.gdb
./granitectl exec -q "SELECT * FROM people;" demo.gdb
```

The CLI supports several verbs:

* `granitectl exec` – run ad-hoc SQL or scripts in table, CSV, or JSON format.
* `granitectl dump` – print a human-readable schema report.
* `granitectl explain` – emit textual and JSON execution plans.
* `granitectl meta [--json] <dbfile>` – output the schema catalogue. Use `--json` for a stable machine-readable payload documented below.

The `meta` JSON structure returned by the new command looks like:

```json
{
  "database": "demo.gdb",
  "tables": [
    {
      "name": "people",
      "rowCount": 2,
      "columns": [
        { "name": "id", "type": "INT", "notNull": true, "default": null, "isPrimaryKey": true },
        { "name": "name", "type": "VARCHAR(50)", "notNull": false, "default": null, "isPrimaryKey": false }
      ],
      "indexes": [
        { "name": "pk_people", "unique": true, "columns": ["id"], "type": "BTREE" }
      ],
      "foreignKeys": []
    }
  ]
}
```

Use this payload when integrating with external tools or the IDE.

## Running the IDE

The desktop IDE requires Node.js 20 LTS (`>=20 <23`).

```bash
cd ide
npm install
npm run tauri dev
```

The development build launches Vite and the Tauri shell with live reload. The IDE discovers `granitectl` automatically when the engine has been built in `engine/`, or you can set `GRANITECTL_PATH` to an explicit executable.

To produce native bundles for macOS, Windows, or Linux:

```bash
cd ide
npm run tauri build
```

### Key IDE features

* **Schema explorer** – powered by `granitectl meta --json`, showing tables, columns, indexes, and foreign keys.
* **SQL editor** – Monaco-based editing with shortcuts for running queries and EXPLAIN plans.
* **Results grid** – tabular display with CSV export and JSON-backed result handling.
* **Plan view** – renders the JSON plan output from `granitectl explain --json`.
* **Resilient engine bridge** – all calls to `granitectl` go through a guarded gateway that surfaces errors without freezing the UI.

Further IDE guidance lives in [docs/ide.md](docs/ide.md).

## Testing

Engine tests:

```bash
cd engine
go test ./...
```

IDE tests (Vitest and Playwright):

```bash
cd ide
npm run test      # unit tests
npm run e2e       # Playwright against the mock engine
```

## Contributing

* Keep changes small and cohesive.
* Run the Go and Node test suites relevant to your changes.
* Use clear commit messages and UK English for documentation and comments.

## Licence

GraniteDB is available under the Apache 2.0 licence. See [LICENCE](LICENSE) for details.
