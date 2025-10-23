#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;
use wait_timeout::ChildExt;

const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct QueryResultPayload {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
    duration_ms: u64,
    #[serde(default)]
    rows_affected: Option<u64>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecResponse {
    format: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<QueryResultPayload>,
}

struct CommandOutput {
    stdout: String,
    stderr: String,
}

#[tauri::command]
fn open_db(path: String) -> Result<(), String> {
    let path = PathBuf::from(&path);
    if !path.exists() {
        return Err("Database file not found".into());
    }
    let metadata = fs::metadata(&path).map_err(|err| format!("Unable to read metadata: {err}"))?;
    if !metadata.is_file() {
        return Err("Path must point to a file".into());
    }
    fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .map_err(|err| format!("Unable to open database: {err}"))?;
    Ok(())
}

#[tauri::command]
fn exec_sql(path: String, sql: String, format: String) -> Result<ExecResponse, String> {
    if sql.trim().is_empty() {
        return Err("SQL must not be empty".into());
    }
    let db_path = Path::new(&path);
    if !db_path.exists() {
        return Err("Database file not found".into());
    }
    let db = db_path
        .to_str()
        .ok_or_else(|| "Database path contains unsupported characters".to_string())?;
    match format.as_str() {
        "jsonRows" => {
            let output = run_granitectl(&["exec", "--format", "json", "-q", &sql, db])?;
            let payload: QueryResultPayload = serde_json::from_str(&output.stdout)
                .map_err(|err| format!("Failed to parse JSON output: {err}"))?;
            Ok(ExecResponse {
                format,
                output: None,
                result: Some(payload),
            })
        }
        "table" | "csv" => {
            let output = run_granitectl(&["exec", "--format", &format, "-q", &sql, db])?;
            Ok(ExecResponse {
                format,
                output: Some(output.stdout),
                result: None,
            })
        }
        other => Err(format!("Unsupported format {other}")),
    }
}

#[tauri::command]
fn explain_sql(path: String, sql: String) -> Result<String, String> {
    if sql.trim().is_empty() {
        return Err("SQL must not be empty".into());
    }
    let db_path = Path::new(&path);
    if !db_path.exists() {
        return Err("Database file not found".into());
    }
    let db = db_path
        .to_str()
        .ok_or_else(|| "Database path contains unsupported characters".to_string())?;
    let output = run_granitectl(&["explain", "--json", "-q", &sql, db])?;
    Ok(output.stdout)
}

#[tauri::command]
fn metadata(path: String) -> Result<String, String> {
    let db_path = Path::new(&path);
    if !db_path.exists() {
        return Err("Database file not found".into());
    }
    let db = db_path
        .to_str()
        .ok_or_else(|| "Database path contains unsupported characters".to_string())?;
    match run_granitectl(&["meta", "--json", db]) {
        Ok(output) => Ok(output.stdout),
        Err(err) => {
            if err.contains("unknown command") {
                let legacy = legacy_metadata(db)?;
                Ok(legacy)
            } else {
                Err(err)
            }
        }
    }
}

#[tauri::command]
fn export_csv(path: String, sql: String, out_path: String) -> Result<(), String> {
    if sql.trim().is_empty() {
        return Err("SQL must not be empty".into());
    }
    let db = Path::new(&path)
        .to_str()
        .ok_or_else(|| "Database path contains unsupported characters".to_string())?;
    let output = run_granitectl(&["exec", "--format", "csv", "-q", &sql, db])?;
    fs::write(&out_path, output.stdout).map_err(|err| format!("Failed to write CSV: {err}"))?;
    Ok(())
}

fn run_granitectl(args: &[&str]) -> Result<CommandOutput, String> {
    let mut command = Command::new(granitectl_path());
    command.args(args);
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .map_err(|err| format!("Failed to run granitectl: {err}"))?;
    match child.wait_timeout(QUERY_TIMEOUT) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let _ = child.kill();
            return Err("granitectl timed out".into());
        }
        Err(err) => {
            let _ = child.kill();
            return Err(format!("Failed to await granitectl: {err}"));
        }
    }
    let output = child
        .wait_with_output()
        .map_err(|err| format!("Failed to read granitectl output: {err}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    if !output.status.success() {
        let err_msg = if stderr.trim().is_empty() {
            "granitectl returned an error".to_string()
        } else {
            stderr.trim().to_string()
        };
        return Err(err_msg);
    }
    Ok(CommandOutput { stdout, stderr })
}

fn granitectl_path() -> OsString {
     if let Some(path) = std::env::var_os("GRANITECTL_PATH") {
        if !path.is_empty() {
            return path;
        }
    }

    let exe_name = format!("granitectl{}", std::env::consts::EXE_SUFFIX);
    if let Ok(current_exe) = std::env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            for ancestor in parent.ancestors() {
                let candidate = ancestor.join("engine").join(&exe_name);
                if candidate.exists() {
                    return candidate.into_os_string();
                }
            }
        }
    }

    OsString::from("granitectl")
}

fn legacy_metadata(db: &str) -> Result<String, String> {
    let output = run_granitectl(&["dump", db])?;
    parse_legacy_metadata(&output.stdout)
}

fn parse_legacy_metadata(output: &str) -> Result<String, String> {
    let mut tables: Vec<LegacyTable> = Vec::new();
    let mut current: Option<LegacyTableBuilder> = None;
    let mut section = LegacySection::Columns;

    for raw_line in output.lines() {
        let line = raw_line.trim_end();
        if line.is_empty() {
            continue;
        }

        if line == "No tables defined" {
            return Ok(
                serde_json::to_string(&LegacyMetadata { tables: Vec::new() })
                    .map_err(|err| format!("Failed to encode metadata: {err}"))?,
            );
        }

        if let Some(rest) = line.strip_prefix("Table ") {
            if let Some(table) = current.take() {
                tables.push(table.into());
            }
            let (name_part, rows_part) = rest
                .split_once(" (")
                .ok_or_else(|| format!("unexpected table header: {line}"))?;
            let rows_part = rows_part
                .trim_end_matches(')')
                .strip_suffix(" row(s)")
                .ok_or_else(|| format!("unexpected row count: {line}"))?;
            let row_count = rows_part
                .parse::<u64>()
                .map_err(|_| format!("invalid row count in line: {line}"))?;
            current = Some(LegacyTableBuilder {
                name: name_part.to_string(),
                row_count,
                columns: Vec::new(),
                indexes: Vec::new(),
                fks: Vec::new(),
            });
            section = LegacySection::Columns;
            continue;
        }

        let Some(table) = current.as_mut() else {
            continue;
        };

        if line == "  Indexes:" {
            section = LegacySection::Indexes;
            continue;
        }
        if line == "  Foreign Keys:" {
            section = LegacySection::ForeignKeys;
            continue;
        }

        match section {
            LegacySection::Columns => {
                if let Some(rest) = line.strip_prefix("  - ") {
                    table.columns.push(parse_legacy_column(rest)?);
                }
            }
            LegacySection::Indexes => {
                if let Some(rest) = line.strip_prefix("    - ") {
                    table.indexes.push(parse_legacy_index(rest)?);
                }
            }
            LegacySection::ForeignKeys => {
                if let Some(rest) = line.strip_prefix("    - ") {
                    table.fks.push(parse_legacy_fk(rest)?);
                }
            }
        }
    }

    if let Some(table) = current.take() {
        tables.push(table.into());
    }

    serde_json::to_string(&LegacyMetadata { tables })
        .map_err(|err| format!("Failed to encode metadata: {err}"))
}

#[derive(Debug)]
enum LegacySection {
    Columns,
    Indexes,
    ForeignKeys,
}

struct LegacyTableBuilder {
    name: String,
    row_count: u64,
    columns: Vec<LegacyColumn>,
    indexes: Vec<LegacyIndex>,
    fks: Vec<LegacyForeignKey>,
}

impl From<LegacyTableBuilder> for LegacyTable {
    fn from(builder: LegacyTableBuilder) -> Self {
        LegacyTable {
            name: builder.name,
            row_count: builder.row_count,
            columns: builder.columns,
            indexes: if builder.indexes.is_empty() {
                None
            } else {
                Some(builder.indexes)
            },
            fks: if builder.fks.is_empty() {
                None
            } else {
                Some(builder.fks)
            },
        }
    }
}

fn parse_legacy_column(line: &str) -> Result<LegacyColumn, String> {
    let mut rest = line.trim();
    let not_null = rest.contains(" NOT NULL");
    let primary = rest.contains(" PRIMARY KEY");

    if let Some(idx) = rest.find(" NOT NULL") {
        rest = &rest[..idx];
    }
    if let Some(idx) = rest.find(" PRIMARY KEY") {
        rest = &rest[..idx];
    }

    let (name, column_type) = rest
        .split_once(' ')
        .ok_or_else(|| format!("unexpected column line: {line}"))?;

    Ok(LegacyColumn {
        name: name.to_string(),
        column_type: column_type.trim().to_string(),
        not_null,
        primary,
    })
}

fn parse_legacy_index(line: &str) -> Result<LegacyIndex, String> {
    let trimmed = line.trim();
    let (base, unique) = if let Some(base) = trimmed.strip_suffix(" UNIQUE") {
        (base.trim_end(), true)
    } else {
        (trimmed, false)
    };

    let (name, cols_part) = base
        .split_once(" (")
        .ok_or_else(|| format!("unexpected index line: {line}"))?;
    let columns = cols_part
        .trim_end_matches(')')
        .split(',')
        .filter_map(|col| {
            let trimmed = col.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect();

    Ok(LegacyIndex {
        name: name.trim().to_string(),
        columns,
        unique,
    })
}

fn parse_legacy_fk(line: &str) -> Result<LegacyForeignKey, String> {
    let (name, rest) = line
        .split_once(" (")
        .ok_or_else(|| format!("unexpected foreign key line: {line}"))?;
    let (child_cols, remainder) = rest
        .split_once(") REFERENCES ")
        .ok_or_else(|| format!("unexpected foreign key line: {line}"))?;
    let (ref_table, ref_cols) = remainder
        .split_once('(')
        .ok_or_else(|| format!("unexpected foreign key line: {line}"))?;
    let ref_columns = ref_cols
        .trim_end_matches(')')
        .split(',')
        .map(|col| col.trim().to_string())
        .filter(|col| !col.is_empty())
        .collect();

    Ok(LegacyForeignKey {
        name: name.trim().to_string(),
        columns: child_cols
            .split(',')
            .map(|col| col.trim().to_string())
            .filter(|col| !col.is_empty())
            .collect(),
        ref_table: ref_table.trim().to_string(),
        ref_columns: ref_columns,
    })
}

#[derive(Serialize)]
struct LegacyMetadata {
    tables: Vec<LegacyTable>,
}

#[derive(Serialize)]
struct LegacyTable {
    name: String,
    #[serde(rename = "rowCount")]
    row_count: u64,
    columns: Vec<LegacyColumn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexes: Option<Vec<LegacyIndex>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fks: Option<Vec<LegacyForeignKey>>,
}

#[derive(Serialize)]
struct LegacyColumn {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
    #[serde(rename = "notNull")]
    not_null: bool,
    #[serde(rename = "pk")]
    primary: bool,
}

#[derive(Serialize)]
struct LegacyIndex {
    name: String,
    columns: Vec<String>,
    unique: bool,
}

#[derive(Serialize)]
struct LegacyForeignKey {
    name: String,
    columns: Vec<String>,
    #[serde(rename = "refTable")]
    ref_table: String,
    #[serde(rename = "refColumns")]
    ref_columns: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_empty_dump() {
        let json = parse_legacy_metadata("No tables defined\n").unwrap();
        assert_eq!(json, "{\"tables\":[]}");
    }

    #[test]
    fn parses_basic_table() {
        let dump =
            "Table users (3 row(s))\n  - id INT NOT NULL PRIMARY KEY\n  - name VARCHAR(50)\n";
        let json = parse_legacy_metadata(dump).unwrap();
        let expected = serde_json::json!({
            "tables": [
                {
                    "name": "users",
                    "rowCount": 3,
                    "columns": [
                        {
                            "name": "id",
                            "type": "INT",
                            "notNull": true,
                            "pk": true
                        },
                        {
                            "name": "name",
                            "type": "VARCHAR(50)",
                            "notNull": false,
                            "pk": false
                        }
                    ]
                }
            ]
        });
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn parses_indexes_and_foreign_keys() {
        let dump = "Table orders (1 row(s))\n  - id INT PRIMARY KEY\n  - customer_id INT NOT NULL\n  Indexes:\n    - idx_orders_customer (customer_id) UNIQUE\n  Foreign Keys:\n    - fk_orders_customer (customer_id) REFERENCES customers(id)\n";
        let json = parse_legacy_metadata(dump).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        let expected = serde_json::json!({
            "tables": [
                {
                    "name": "orders",
                    "rowCount": 1,
                    "columns": [
                        {
                            "name": "id",
                            "type": "INT",
                            "notNull": false,
                            "pk": true
                        },
                        {
                            "name": "customer_id",
                            "type": "INT",
                            "notNull": true,
                            "pk": false
                        }
                    ],
                    "indexes": [
                        {
                            "name": "idx_orders_customer",
                            "columns": ["customer_id"],
                            "unique": true
                        }
                    ],
                    "fks": [
                        {
                            "name": "fk_orders_customer",
                            "columns": ["customer_id"],
                            "refTable": "customers",
                            "refColumns": ["id"]
                        }
                    ]
                }
            ]
        });
        assert_eq!(parsed, expected);
    }
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_fs::init())
        .invoke_handler(tauri::generate_handler![
            open_db,
            exec_sql,
            explain_sql,
            metadata,
            export_csv
        ])
        .run(tauri::generate_context!())
        .expect("error while running Granite IDE application");
}
