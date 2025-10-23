import { describe, expect, it, vi } from "vitest";

const invokeMock = vi.fn();

vi.mock("@tauri-apps/api/tauri", () => ({
  invoke: invokeMock
}));

describe("Tauri gateway", () => {
  it("wraps successful calls", async () => {
    invokeMock.mockResolvedValueOnce({ ok: true });
    const { call } = await import("../gateway");
    const result = await call<{ ok: boolean }>("ping");
    expect(result).toEqual({ ok: true, value: { ok: true } });
  });

  it("captures errors and returns failure", async () => {
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    invokeMock.mockRejectedValueOnce(new Error("boom"));
    const { call } = await import("../gateway");
    const result = await call<unknown>("ping");
    expect(result).toEqual({ ok: false, error: "boom" });
    errorSpy.mockRestore();
  });
});
