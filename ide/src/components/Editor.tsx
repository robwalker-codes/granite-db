import { useCallback, useEffect, useRef } from "react";
import Editor, { OnChange, useMonaco } from "@monaco-editor/react";
import type { editor as MonacoEditor } from "monaco-editor";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun: (sql: string) => void;
  onExplain: (sql: string) => void;
  theme: "light" | "dark";
  errorMessage: string | null;
}

export default function SqlEditor({ value, onChange, onRun, onExplain, theme, errorMessage }: SqlEditorProps) {
  const editorRef = useRef<MonacoEditor.IStandaloneCodeEditor | null>(null);
  const monaco = useMonaco();

  const handleMount = useCallback((editor: MonacoEditor.IStandaloneCodeEditor) => {
    editorRef.current = editor;
  }, []);

  useEffect(() => {
    const editor = editorRef.current;
    if (!editor || !monaco) {
      return;
    }
    const runAction = editor.addAction({
      id: "granite-run-query",
      label: "Run Query",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        onRun(getSelectedText(editor));
      }
    });
    const explainAction = editor.addAction({
      id: "granite-explain-query",
      label: "Explain Query",
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyL],
      run: () => {
        onExplain(getSelectedText(editor));
      }
    });
    return () => {
      runAction.dispose();
      explainAction.dispose();
    };
  }, [monaco, onExplain, onRun]);

  useEffect(() => {
    if (!monaco) {
      return;
    }
    const background = getCssColor("--color-editor-bg", theme === "dark" ? "#0b0f14" : "#ffffff");
    const foreground = getCssColor("--color-editor-foreground", theme === "dark" ? "#e6edf3" : "#0f172a");
    const lineNumber = getCssColor("--color-editor-line-number", theme === "dark" ? "#8b949e" : "#475569");
    const selection = getCssColor("--color-editor-selection", theme === "dark" ? "#264f78" : "#bfdbfe");
    const cursor = getCssColor("--color-editor-cursor", theme === "dark" ? "#e6edf3" : "#1f2937");

    monaco.editor.defineTheme("granite-dark", {
      base: "vs-dark",
      inherit: true,
      rules: [],
      colors: {
        "editor.background": background,
        "editor.foreground": foreground,
        "editorLineNumber.foreground": lineNumber,
        "editorCursor.foreground": cursor,
        "editor.selectionBackground": selection,
        "editor.lineHighlightBackground": getCssColor("--color-editor-highlight", "#1f293780"),
        "editor.inactiveSelectionBackground": getCssColor("--color-editor-inactive-selection", "#1f293733")
      }
    });

    monaco.editor.defineTheme("granite-light", {
      base: "vs",
      inherit: true,
      rules: [],
      colors: {
        "editor.background": background,
        "editor.foreground": foreground,
        "editorLineNumber.foreground": lineNumber,
        "editorCursor.foreground": cursor,
        "editor.selectionBackground": selection,
        "editor.lineHighlightBackground": getCssColor("--color-editor-highlight", "#e2e8f0"),
        "editor.inactiveSelectionBackground": getCssColor("--color-editor-inactive-selection", "#cbd5f54d")
      }
    });

    monaco.editor.setTheme(theme === "dark" ? "granite-dark" : "granite-light");
  }, [monaco, theme]);

  const handleChange: OnChange = useCallback((nextValue) => {
    onChange(nextValue ?? "");
  }, [onChange]);

  return (
    <div className="flex h-full flex-col">
      <div
        className="flex-1 overflow-hidden border-b border-slate-200 dark:border-slate-700"
        style={{ background: "var(--color-editor-bg)" }}
      >
        <Editor
          height="100%"
          defaultLanguage="sql"
          value={value}
          onMount={handleMount}
          onChange={handleChange}
          theme={theme === "dark" ? "granite-dark" : "granite-light"}
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            wordWrap: "on",
            automaticLayout: true
          }}
        />
      </div>
      {errorMessage && (
        <div className="border-t border-red-300 bg-red-50 px-4 py-2 text-sm text-red-700 dark:border-red-800 dark:bg-red-950 dark:text-red-200">
          {errorMessage}
        </div>
      )}
    </div>
  );
}

function getSelectedText(editor: MonacoEditor.IStandaloneCodeEditor): string {
  const selection = editor.getSelection();
  if (selection && !selection.isEmpty()) {
    return editor.getModel()?.getValueInRange(selection) ?? "";
  }
  return editor.getValue();
}

function getCssColor(variable: string, fallback: string): string {
  if (typeof window === "undefined") {
    return fallback;
  }
  const value = getComputedStyle(document.documentElement).getPropertyValue(variable).trim();
  return value || fallback;
}
