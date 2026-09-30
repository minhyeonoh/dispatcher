import { describe, expect, it } from "vitest";
import {
  ancestorsOf,
  isOpen,
  reveal,
  toggle,
  type Expansion,
} from "./expansion";

describe("ancestorsOf", () => {
  it("lists strict ancestors, outermost first", () => {
    expect(ancestorsOf("bench/v7/front5")).toEqual([
      "bench",
      "bench/v7",
    ]);
  });

  it("has none for a root", () => {
    expect(ancestorsOf("bench")).toEqual([]);
    expect(ancestorsOf("")).toEqual([]);
  });
});

describe("isOpen / toggle", () => {
  it("defaults to open — a collapsed tree hides the point of it", () => {
    expect(isOpen({}, "bench")).toBe(true);
  });

  it("toggles from the default without needing a prior entry", () => {
    const once = toggle({}, "bench");
    expect(isOpen(once, "bench")).toBe(false);
    expect(isOpen(toggle(once, "bench"), "bench")).toBe(true);
  });

  it("touches only the node named", () => {
    const next = toggle({ other: false }, "bench");
    expect(next["other"]).toBe(false);
  });
});

describe("reveal", () => {
  it("opens the ancestors of the active path", () => {
    const closed: Expansion = { bench: false, "bench/v7": false };
    const next = reveal(closed, "bench/v7/front5");
    expect(isOpen(next, "bench")).toBe(true);
    expect(isOpen(next, "bench/v7")).toBe(true);
  });

  it("does NOT open the node itself — arriving somewhere should not expand it", () => {
    const next = reveal({ "bench/v7": false }, "bench/v7");
    expect(isOpen(next, "bench/v7")).toBe(false);
  });

  it("leaves unrelated collapses alone", () => {
    const next = reveal({ other: false }, "bench/v7");
    expect(isOpen(next, "other")).toBe(false);
  });

  it("returns the same object when nothing changes", () => {
    // Identity matters: this feeds a setState on every navigation,
    // and a fresh object each time would re-render forever.
    const open: Expansion = {};
    expect(reveal(open, "bench/v7")).toBe(open);
  });
});
