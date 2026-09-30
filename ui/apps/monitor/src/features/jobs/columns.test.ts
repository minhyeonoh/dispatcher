import { describe, expect, it } from "vitest";
import type { JobCounts } from "../../api/types";
import type { JobRow } from "../../live/fold";
import {
  columnTitle,
  DEFAULT_VISIBLE,
  defaultVisibility,
  jobColumns,
} from "./columns";

const columns = jobColumns(null);

function row(counts: Partial<JobCounts>): JobRow {
  return {
    job_id: "j",
    label: "arm",
    alias: "brave-otter",
    counts: {
      pending: 0,
      running: 0,
      done_ok: 0,
      done_err: 0,
      ghosted: 0,
      unknown: 0,
      total: 0,
      ...counts,
    },
  } as JobRow;
}

/** The sort/filter value a column produces for a row. */
function value(id: string, job: JobRow): unknown {
  const column = columns.find((c) => c.id === id);
  if (!column || !("accessorFn" in column) || !column.accessorFn)
    throw new Error(`no accessor for ${id}`);
  return column.accessorFn(job, 0);
}

describe("column registry", () => {
  it("has a unique id per column", () => {
    const ids = columns.map((c) => String(c.id));
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("every default-visible id exists", () => {
    const ids = new Set(columns.map((c) => String(c.id)));
    for (const id of DEFAULT_VISIBLE) expect(ids).toContain(id);
  });

  it("defaultVisibility covers every column", () => {
    const vis = defaultVisibility(columns);
    expect(Object.keys(vis).sort()).toEqual(
      columns.map((c) => String(c.id)).sort(),
    );
    expect(vis["job"]).toBe(true);
    expect(vis["job_id"]).toBe(false);
  });

  it("keeps the job column unhideable — the row needs a link", () => {
    const job = columns.find((c) => c.id === "job");
    expect(job?.enableHiding).toBe(false);
  });

  it("names columns for the picker, never leaving a bare id", () => {
    for (const column of columns) {
      expect(columnTitle(column).length).toBeGreaterThan(0);
    }
    expect(columnTitle(columns.find((c) => c.id === "unresolved")!)).toBe(
      "unresolved (unknown + ghosted)",
    );
  });
});

describe("derived columns", () => {
  it("unresolved sums unknown and ghosted", () => {
    expect(value("unresolved", row({ unknown: 2, ghosted: 1 }))).toBe(3);
  });

  it("done % counts errors as done", () => {
    expect(
      value("done_pct", row({ done_ok: 3, done_err: 1, total: 8 })),
    ).toBe(50);
  });

  it("done % is 0 rather than NaN for an empty job", () => {
    expect(value("done_pct", row({ total: 0 }))).toBe(0);
  });

  it("err % is over SCORED instances, not all tasks", () => {
    // 1 of 4 scored failed while 6 are still pending: 25%, not 10%.
    expect(
      value(
        "err_rate",
        row({ done_ok: 3, done_err: 1, pending: 6, total: 10 }),
      ),
    ).toBe(25);
  });

  it("err % is 0 rather than NaN before anything is scored", () => {
    expect(value("err_rate", row({ pending: 5, total: 5 }))).toBe(0);
  });
});
