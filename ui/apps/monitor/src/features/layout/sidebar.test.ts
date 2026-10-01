import { describe, expect, it } from "vitest";
import { sidebarOpenFrom } from "./sidebar";

describe("sidebarOpenFrom", () => {
  it("defaults to open when nothing is stored", () => {
    expect(sidebarOpenFrom(null)).toBe(true);
  });

  it("is closed only for the one value that means it", () => {
    expect(sidebarOpenFrom("closed")).toBe(false);
  });

  it("treats anything else as open", () => {
    // A stale or hand-edited value must not leave the operator
    // staring at a hidden tree with no obvious cause.
    expect(sidebarOpenFrom("")).toBe(true);
    expect(sidebarOpenFrom("open")).toBe(true);
    expect(sidebarOpenFrom("true")).toBe(true);
  });
});
