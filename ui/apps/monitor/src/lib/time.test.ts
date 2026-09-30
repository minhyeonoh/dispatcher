import { describe, expect, it } from "vitest";
import {
  durationSeconds,
  formatAge,
  formatDuration,
  formatExact,
} from "./time";

const start = "2026-09-30T13:00:00+09:00";
const at = (offsetSeconds: number) =>
  Date.parse(start) + offsetSeconds * 1000;

describe("durationSeconds", () => {
  it("measures a finished instance from its own timestamps", () => {
    expect(durationSeconds(start, "2026-09-30T13:02:30+09:00", 0)).toBe(
      150,
    );
  });

  it("measures a running instance against now", () => {
    expect(durationSeconds(start, null, at(90))).toBe(90);
  });

  it("never goes negative on clock skew", () => {
    expect(durationSeconds(start, null, at(-5))).toBe(0);
  });
});

describe("formatDuration", () => {
  it("formats across the units", () => {
    expect(formatDuration(45)).toBe("45s");
    expect(formatDuration(150)).toBe("2m 30s");
    expect(formatDuration(7260)).toBe("2h 1m");
  });
});

describe("formatAge", () => {
  it("is coarse on purpose", () => {
    expect(formatAge(start, at(10))).toBe("just now");
    expect(formatAge(start, at(300))).toBe("5m ago");
    expect(formatAge(start, at(3 * 3600))).toBe("3h ago");
    expect(formatAge(start, at(50 * 3600))).toBe("2d ago");
  });

  it("reads a future stamp as just now rather than negative", () => {
    // Hosts disagree about the clock; "-3m ago" would look broken.
    expect(formatAge(start, at(-200))).toBe("just now");
  });

  it("does not crash on an unparseable stamp", () => {
    expect(formatAge("not a date", at(0))).toBe("\u2014");
  });
});

describe("formatExact", () => {
  it("returns the input unchanged when it is not a date", () => {
    expect(formatExact("nonsense")).toBe("nonsense");
  });

  it("renders a real stamp as something longer than the date", () => {
    expect(formatExact(start).length).toBeGreaterThan(8);
  });
});
