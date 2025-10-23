import { fireEvent, render, screen } from "@testing-library/react";
import SqlEditor from "../Editor";

vi.mock("@monaco-editor/react", () => {
  return {
    __esModule: true,
    default: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
      <textarea value={value} onChange={(event) => onChange(event.target.value)} />
    ),
    useMonaco: () => null
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
});
