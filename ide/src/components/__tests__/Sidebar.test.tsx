import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import Sidebar from "../Sidebar";
import type { DatabaseMetadata } from "../../state/db";

vi.mock("../../state/db", async (importOriginal) => ({
  ...(await importOriginal())
}));

describe("Sidebar", () => {
  const noop = () => {};

  it("renders gracefully when metadata is null", () => {
    expect(() =>
      render(
        <Sidebar
          metadata={null}
          search=""
          onSearchChange={noop}
          onSelectTable={noop as never}
          onRefresh={noop}
          refreshing={false}
        />
      )
    ).not.toThrow();
    expect(screen.getByText("No tables found.")).toBeInTheDocument();
  });

  it("renders without crashing when tables are missing", () => {
    const malformed = { database: "demo.gdb" } as unknown as DatabaseMetadata;
    render(
      <Sidebar
        metadata={malformed}
        search=""
        onSearchChange={noop}
        onSelectTable={noop as never}
        onRefresh={noop}
        refreshing={false}
      />
    );
    expect(screen.getByText("No tables found.")).toBeInTheDocument();
  });
});
