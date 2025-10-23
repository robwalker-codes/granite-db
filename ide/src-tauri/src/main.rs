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
    let output = run_granitectl(&["meta", "--json", db])?;
    Ok(output.stdout)
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
    std::env::var_os("GRANITECTL_PATH").unwrap_or_else(|| OsString::from("granitectl"))
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_window_state::Builder::default().build())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_dialog::init())
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
