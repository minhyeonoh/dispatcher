import { describe, expect, it } from "vitest";
import type { JobCounts } from "../../api/types";
import type { JobRow } from "../../live/fold";
import {
  filterJobs,
  filterToSearch,
  isFilterActive,
  parseJobFilter,
  toFilter,
  type JobFilter,
} from "./filter";

function job(over: Partial<JobRow> = {}): JobRow {
  const counts: JobCounts = {
    pending: 0,
    running: 0,
    done_ok: 0,
    done_err: 0,
    ghosted: 0,
    unknown: 0,
    total: 0,
    ...(over.counts ?? {}),
  };
  return {
    job_id: "j",
    label: "arm",
    alias: "brave-otter",
    arena: "",
    paused: false,
    blocked: null,
    archived_at: null,
    ...over,
    counts,
  } as JobRow;
}

const filter = (over: Partial<JobFilter> = {}): JobFilter => ({
  q: "",
  state: "any",
  ...over,
});

describe("url round trip", () => {
  it("drops unknown keys and bad values", () => {
    expect(
      parseJobFilter({ q: "v7", state: "nonsense", nope: 1 }),
    ).toEqual({ q: "v7" });
  });

  it("omits defaults so an unfiltered view has a clean url", () => {
    expect(parseJobFilter({ q: "", state: "any" })).toEqual({});
    expect(filterToSearch(filter())).toEqual({});
  });

  it("survives the round trip", () => {
    const f = filter({ q: "baseline", state: "with-errors" });
    expect(toFilter(parseJobFilter({ ...filterToSearch(f) }))).toEqual(
      f,
    );
  });

  it("fills defaults for an absent search", () => {
    expect(toFilter({})).toEqual({ q: "", state: "any" });
  });

  it("knows when a filter is doing nothing", () => {
    expect(isFilterActive(filter())).toBe(false);
    expect(isFilterActive(filter({ q: "x" }))).toBe(true);
    expect(isFilterActive(filter({ state: "paused" }))).toBe(true);
  });
});

describe("filterJobs", () => {
  const jobs = [
    job({ job_id: "a", label: "sleepbench-baseline", arena: "demo/x" }),
    job({
      job_id: "b",
      label: "sleepbench-variant",
      alias: "cyan-alpaca",
      counts: { done_err: 2 } as JobCounts,
    }),
    job({ job_id: "c", label: "other", paused: true }),
    job({ job_id: "d", label: "held", blocked: "pool_cap" }),
    job({
      job_id: "e",
      label: "murky",
      counts: { unknown: 1 } as JobCounts,
    }),
    job({ job_id: "f", label: "old", archived_at: "2026-09-01T00:00:00+09:00" }),
  ];
  const ids = (f: Partial<JobFilter>) =>
    filterJobs(jobs, filter(f)).map((j) => j.job_id);

  it("matches label, alias and arena, case-insensitively", () => {
    expect(ids({ q: "BASELINE" })).toEqual(["a"]);
    expect(ids({ q: "cyan" })).toEqual(["b"]);
    expect(ids({ q: "demo/" })).toEqual(["a"]);
  });

  it("ignores surrounding whitespace in the query", () => {
    expect(ids({ q: "  variant  " })).toEqual(["b"]);
  });

  it("selects by state", () => {
    expect(ids({ state: "paused" })).toEqual(["c"]);
    expect(ids({ state: "with-errors" })).toEqual(["b"]);
    expect(ids({ state: "unresolved" })).toEqual(["e"]);
    expect(ids({ state: "archived" })).toEqual(["f"]);
  });

  it("'blocked' means the scheduler is holding it, not 'has pending'", () => {
    // job "a" has pending work but blocked=null — it is simply next
    // in the rotation, and calling that blocked would cry wolf.
    expect(ids({ state: "blocked" })).toEqual(["d"]);
  });

  it("'active' excludes paused and archived", () => {
    expect(ids({ state: "active" })).toEqual(["a", "b", "d", "e"]);
  });

  it("ands the query with the state", () => {
    expect(ids({ q: "sleepbench", state: "with-errors" })).toEqual(["b"]);
    expect(ids({ q: "other", state: "with-errors" })).toEqual([]);
  });

  it("returns everything when nothing is set", () => {
    expect(ids({}).length).toBe(jobs.length);
  });
});
