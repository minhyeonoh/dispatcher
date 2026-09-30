import { describe, expect, it } from "vitest";
import {
  mergeVisibility,
  parseSorting,
  pruneSorting,
} from "./tablePrefs";

const defaults = { job: true, err: true, pool: false };

describe("mergeVisibility", () => {
  it("keeps what the operator chose", () => {
    expect(
      mergeVisibility({ job: true, err: false, pool: true }, defaults),
    ).toEqual({ job: true, err: false, pool: true });
  });

  it("gives a column added since the prefs were saved ITS default", () => {
    // Not `false` — a new column must be able to arrive switched on,
    // or every release would silently hide its own additions.
    expect(mergeVisibility({ job: false }, defaults)).toEqual({
      job: false,
      err: true,
      pool: false,
    });
  });

  it("drops a column this build no longer has", () => {
    const merged = mergeVisibility(
      { job: true, ancient_column: true },
      defaults,
    );
    expect(Object.keys(merged).sort()).toEqual(["err", "job", "pool"]);
  });

  it("falls back whole for junk", () => {
    for (const junk of [null, undefined, 42, "nope", []]) {
      expect(mergeVisibility(junk, defaults)).toEqual(defaults);
    }
  });

  it("ignores a non-boolean value for a known column", () => {
    expect(mergeVisibility({ err: "yes" }, defaults).err).toBe(true);
  });
});

describe("parseSorting", () => {
  it("reads id and direction", () => {
    expect(parseSorting([{ id: "err", desc: true }])).toEqual([
      { id: "err", desc: true },
    ]);
  });

  it("defaults desc to false and skips malformed entries", () => {
    expect(
      parseSorting([{ id: "ok" }, { desc: true }, null, "x"]),
    ).toEqual([{ id: "ok", desc: false }]);
  });

  it("returns empty for non-arrays", () => {
    expect(parseSorting({ id: "ok" })).toEqual([]);
  });
});

describe("pruneSorting", () => {
  it("drops a sort on a column that no longer exists", () => {
    // Otherwise the table sorts by nothing and looks broken.
    expect(
      pruneSorting(
        [
          { id: "gone", desc: false },
          { id: "err", desc: true },
        ],
        Object.keys(defaults),
      ),
    ).toEqual([{ id: "err", desc: true }]);
  });
});
