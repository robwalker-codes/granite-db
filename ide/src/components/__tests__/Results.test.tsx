import { render, screen } from "@testing-library/react";
import Results from "../Results";
import type { QueryResult } from "../../state/db";

describe("Results", () => {
  const sample: QueryResult = {
    columns: ["id", "name"],
    rows: [
      ["1", "Ada"],
      ["2", "Grace"]
    ],
    durationMs: 10
  };

  it("renders a table when result present", () => {
    render(<Results result={sample} active />);
    expect(screen.getByText("id")).toBeInTheDocument();
    expect(screen.getByText("Ada")).toBeInTheDocument();
  });

  it("shows placeholder when no result", () => {
    render(<Results result={null} active />);
    expect(screen.getByText(/Run a query/)).toBeInTheDocument();
  });
});
