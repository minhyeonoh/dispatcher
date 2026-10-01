import { describe, expect, it } from "vitest";
import type { JobRow } from "../../live/fold";
import {
  collectOperatorColumns,
  formatColumnValue,
  operatorColumnId,
  summariseColumns,
} from "./operatorColumns";

function row(
  source: string,
  columns: Record<string, unknown>,
  id = source + JSON.stringify(columns),
): JobRow {
  return {
    job_id: id,
    columns,
    columns_source_arena: source,
  } as unknown as JobRow;
}

describe("collectOperatorColumns", () => {
  it("keeps the function's own key order", () => {
    // The operator decides what sits leftmost by the order they wrote
    // the dict literal; sorting here would throw that away.
    const spec = collectOperatorColumns([
      row("appworld", { tgc: 0.5, solved: true, zzz: 1 }),
    ]);
    expect(spec.columns.map((c) => c.key)).toEqual([
      "tgc",
      "solved",
      "zzz",
    ]);
    expect(spec.multiSource).toBe(false);
  });

  it("labels a key plainly while only one source has it", () => {
    const spec = collectOperatorColumns([
      row("appworld", { tgc: 0.5 }),
      row("appworld", { tgc: 0.6 }),
    ]);
    expect(spec.columns.map((c) => c.label)).toEqual(["tgc"]);
  });

  it("prefixes a key two sources both define", () => {
    // Two meanings of `solved` must not share a header — that is the
    // "one name, two computations" collision, silent on a table.
    const spec = collectOperatorColumns([
      row("appworld", { tgc: 0.5, solved: true }),
      row("bench/v7", { solved: 0.83 }),
    ]);
    expect(spec.multiSource).toBe(true);
    expect(spec.columns.map((c) => c.label)).toEqual([
      "tgc",
      "solved:appworld",
      "solved:bench/v7",
    ]);
    // …and they are two distinct columns.
    expect(new Set(spec.columns.map((c) => operatorColumnId(c.source, c.key))).size).toBe(3);
  });

  it("calls a column numeric until a non-number appears", () => {
    expect(
      collectOperatorColumns([row("a", { x: 1, s: "ml7", b: true })])
        .columns.map((c) => [c.key, c.numeric]),
    ).toEqual([
      ["x", true],
      ["s", false],
      ["b", true],
    ]);
  });

  it("treats an all-null column as numeric", () => {
    // Usually a number not computed yet; a right-aligned empty column
    // beats one that jumps sides when the first value lands.
    const spec = collectOperatorColumns([row("a", { x: null })]);
    expect(spec.columns[0]?.numeric).toBe(true);
  });

  it("ignores rows with no columns function", () => {
    const spec = collectOperatorColumns([
      row("", {}),
      row("appworld", { tgc: 1 }),
    ]);
    expect(spec.multiSource).toBe(false);
    expect(spec.columns).toHaveLength(1);
  });
});

describe("formatColumnValue", () => {
  it("renders a missing value as a dash, never a blank", () => {
    // Blank under a numeric header reads as zero.
    expect(formatColumnValue(null)).toBe("—");
    expect(formatColumnValue(undefined)).toBe("—");
    expect(formatColumnValue(NaN)).toBe("—");
  });

  it("words booleans and trims floats", () => {
    expect(formatColumnValue(true)).toBe("yes");
    expect(formatColumnValue(false)).toBe("no");
    expect(formatColumnValue(7)).toBe("7");
    expect(formatColumnValue(0.6510000001)).toBe("0.651");
    expect(formatColumnValue("ml7")).toBe("ml7");
  });
});

describe("summariseColumns", () => {
  it("lists every column this row has, in order", () => {
    expect(summariseColumns(row("a", { tgc: 0.651, solved: true }))).toBe(
      "tgc 0.651 · solved yes",
    );
  });

  it("is empty when the row has none", () => {
    expect(summariseColumns(row("", {}))).toBe("");
  });
});
