import { describe, expect, it, vi } from "vitest";

vi.mock("../lib/tauri/gateway", () => ({
  call: vi.fn()
}));

const dbModulePromise = import("../db");

describe("SQL classification", () => {
  it("identifies DDL statements", async () => {
    const { isDdlStatement } = await dbModulePromise;
    expect(isDdlStatement("CREATE TABLE t (id INT);"))
      .toBe(true);
    expect(isDdlStatement("drop index i"))
      .toBe(true);
    expect(isDdlStatement(" ALTER TABLE t ADD COLUMN name TEXT"))
      .toBe(true);
    expect(isDdlStatement("select 1"))
      .toBe(false);
  });

  it("identifies commit statements", async () => {
    const { isCommitStatement } = await dbModulePromise;
    expect(isCommitStatement("COMMIT"))
      .toBe(true);
    expect(isCommitStatement("commit;"))
      .toBe(true);
    expect(isCommitStatement("commit transaction"))
      .toBe(true);
    expect(isCommitStatement("rollback"))
      .toBe(false);
  });
});
