import { describe, expect, it } from "vitest";
import type { JobCounts } from "../../api/types";
import type { JobRow } from "../../live/fold";
import {
  columnName,
  columnDescription,
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

  it("names a column exactly as its table header reads", () => {
    // The picker exists to find the column you saw in the table, so an
    // expanded label beside a `mcw` header would leave you matching
    // them up by guesswork.
    for (const column of columns) {
      expect(columnName(column).length).toBeGreaterThan(0);
    }
    // The id is `unresolved` and the header reads `unres`; the picker
    // must say `unres`, because that is what is on screen.
    expect(columnName(columns.find((c) => c.id === "unresolved")!)).toBe(
      "unres",
    );
    expect(columnName(columns.find((c) => c.id === "max_concurrent")!)).toBe(
      "mcw",
    );
  });

  it("describes every built-in column", () => {
    // Operator columns are made to describe themselves at
    // registration; ours have no such gate, so the test is the gate.
    for (const column of columns) {
      if ((column.meta as { group?: string } | undefined)?.group) continue;
      expect(
        columnDescription(column),
        `no description for column ${String(column.id)}`,
      ).not.toBe("");
    }
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

describe("outstanding work", () => {
  // Null is "nothing is watching", 0 is "caught up". Sorting them
  // together would put an untracked job among the healthy ones, which
  // is the reading that matters least to be wrong about.
  it("sorts an untracked job below every real count", () => {
    expect(value("pack_lag", row({}))).toBe(-1);
    expect(value("readout_lag", row({}))).toBe(-1);
  });

  it("keeps zero distinct from untracked", () => {
    const caught = { ...row({}), pack_lag: 0, readout_lag: 0 } as JobRow;
    expect(value("pack_lag", caught)).toBe(0);
    expect(value("readout_lag", caught)).toBe(0);
  });

  it("passes a real count through", () => {
    const behind = { ...row({}), pack_lag: 7, readout_lag: 3 } as JobRow;
    expect(value("pack_lag", behind)).toBe(7);
    expect(value("readout_lag", behind)).toBe(3);
  });

  it("leaves both off by default — usually 0, and noise when it is", () => {
    expect(DEFAULT_VISIBLE).not.toContain("pack_lag");
    expect(DEFAULT_VISIBLE).not.toContain("readout_lag");
    const vis = defaultVisibility(columns);
    expect(vis.pack_lag).toBe(false);
    expect(vis.readout_lag).toBe(false);
  });
});
