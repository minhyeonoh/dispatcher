import { describe, expect, it } from "vitest";
import type { JobCounts } from "../../api/types";
import type { JobRow } from "../../live/fold";
import { attentionRows } from "./attention";

function job(id: string, c: Partial<JobCounts>, extra = {}): JobRow {
  const counts: JobCounts = {
    pending: 0,
    running: 0,
    done_ok: 0,
    done_err: 0,
    ghosted: 0,
    unknown: 0,
    total: 0,
    ...c,
  };
  return { job_id: id, alias: id, counts, paused: false, ...extra } as JobRow;
}

describe("attentionRows", () => {
  it("surfaces unresolved instances first — they block drain", () => {
    const rows = attentionRows([
      job("drained-with-errors", { done_err: 2, done_ok: 1 }),
      job("has-unknown", { unknown: 1, running: 1 }),
    ]);
    expect(rows.map((r) => r.kind)).toEqual(["unresolved", "errors"]);
    expect(rows[0]?.job.job_id).toBe("has-unknown");
  });

  it("flags a job that stopped itself on an error", () => {
    const rows = attentionRows([
      job("stopped", { done_err: 1, pending: 5 }, { paused: true }),
    ]);
    expect(rows[0]?.kind).toBe("paused_with_errors");
  });

  it("flags stalled work WITH the scheduler's reason", () => {
    const rows = attentionRows([
      job("stalled", { pending: 3 }, { blocked: "no_host" }),
    ]);
    expect(rows[0]?.kind).toBe("stalled");
    expect(rows[0]?.detail).toContain("no_host");
  });

  it("stays quiet when pending work is merely awaiting its turn", () => {
    // blocked=null means the scheduler WOULD dispatch it — that
    // resolves within a tick and is not attention-worthy.
    expect(attentionRows([job("turn", { pending: 3 })])).toEqual([]);
  });

  it("stays quiet for a healthy job", () => {
    expect(
      attentionRows([job("fine", { running: 2, pending: 4, done_ok: 1 })]),
    ).toEqual([]);
  });

  it("ignores cancelled and archived jobs", () => {
    const rows = attentionRows([
      job("gone", { unknown: 3 }, { cancelled: true }),
      job("frozen", { unknown: 3 }, { archived_at: "2026-09-30T00:00:00+09:00" }),
    ]);
    expect(rows).toEqual([]);
  });
});
