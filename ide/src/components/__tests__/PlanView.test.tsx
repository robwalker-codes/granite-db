import { render, screen } from "@testing-library/react";
import PlanView from "../PlanView";
import type { ExplainPayload } from "../../lib/planTypes";

describe("PlanView", () => {
  const plan: ExplainPayload = {
    version: 1,
    text: "Project\n  SeqScan",
    physical: {
      node: "Project",
      children: [
        {
          node: "SeqScan",
          props: {
            table: "orders",
            predicate: "total > 10"
          }
        }
      ]
    }
  };

  it("renders plan text", () => {
    render(<PlanView plan={plan} active />);
    expect(screen.getAllByText(/Project/).length).toBeGreaterThan(0);
    expect(screen.getByText(/total > 10/)).toBeInTheDocument();
  });

  it("shows hint when no plan", () => {
    render(<PlanView plan={null} active />);
    expect(screen.getByText(/Use EXPLAIN/)).toBeInTheDocument();
  });
});
