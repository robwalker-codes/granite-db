import { fireEvent, render, screen } from "@testing-library/react";
import SqlEditor from "../Editor";

const monacoMock = {
  editor: {
    defineTheme: vi.fn(),
    setTheme: vi.fn()
  }
};

vi.mock("@monaco-editor/react", () => {
  return {
    __esModule: true,
    default: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
      <textarea value={value} onChange={(event) => onChange(event.target.value)} />
    ),
    useMonaco: () => monacoMock
  };
});

describe("SqlEditor", () => {
  it("renders error message", () => {
    render(
      <SqlEditor
        value="SELECT 1;"
        onChange={() => {}}
        onRun={() => {}}
        onExplain={() => {}}
        theme="light"
        errorMessage="Syntax error"
      />
    );
    expect(screen.getByText("Syntax error")).toBeInTheDocument();
  });

  it("propagates changes", () => {
    const handleChange = vi.fn();
    render(
      <SqlEditor
        value="SELECT 1;"
        onChange={handleChange}
        onRun={() => {}}
        onExplain={() => {}}
        theme="light"
        errorMessage={null}
      />
    );
    const textarea = screen.getByRole("textbox");
    fireEvent.change(textarea, { target: { value: "SELECT 2;" } });
    expect(handleChange).toHaveBeenCalledWith("SELECT 2;");
  });

  it("applies Monaco themes when the UI theme changes", () => {
    monacoMock.editor.defineTheme.mockClear();
    monacoMock.editor.setTheme.mockClear();

    const { rerender } = render(
      <SqlEditor
        value="SELECT 1;"
        onChange={() => {}}
        onRun={() => {}}
        onExplain={() => {}}
        theme="light"
        errorMessage={null}
      />
    );

    expect(monacoMock.editor.defineTheme).toHaveBeenCalledWith(
      "granite-light",
      expect.objectContaining({ colors: expect.any(Object) })
    );
    expect(monacoMock.editor.defineTheme).toHaveBeenCalledWith(
      "granite-dark",
      expect.objectContaining({ colors: expect.any(Object) })
    );
    expect(monacoMock.editor.setTheme).toHaveBeenLastCalledWith("granite-light");

    rerender(
      <SqlEditor
        value="SELECT 1;"
        onChange={() => {}}
        onRun={() => {}}
        onExplain={() => {}}
        theme="dark"
        errorMessage={null}
      />
    );

    expect(monacoMock.editor.setTheme).toHaveBeenLastCalledWith("granite-dark");
  });
});
