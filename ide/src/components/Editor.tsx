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
    const runCommand = editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      onRun(getSelectedText(editor));
    });
    const explainCommand = editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.KeyL, () => {
      onExplain(getSelectedText(editor));
    });
    return () => {
      if (runCommand) {
        editor.removeCommand(runCommand);
      }
      if (explainCommand) {
        editor.removeCommand(explainCommand);
      }
    };
  }, [monaco, onExplain, onRun]);

  useEffect(() => {
    if (!monaco) {
      return;
    }
    monaco.editor.setTheme(theme === "dark" ? "vs-dark" : "vs");
  }, [monaco, theme]);

  const handleChange: OnChange = useCallback((nextValue) => {
    onChange(nextValue ?? "");
  }, [onChange]);

  return (
    <div className="flex h-full flex-col">
      <div className="flex-1 overflow-hidden border-b border-slate-200 dark:border-slate-700">
        <Editor
          height="100%"
          defaultLanguage="sql"
          value={value}
          onMount={handleMount}
          onChange={handleChange}
          theme={theme === "dark" ? "vs-dark" : "vs"}
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
