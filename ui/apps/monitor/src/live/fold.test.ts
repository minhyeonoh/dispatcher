import { describe, expect, it } from "vitest";
import type { JobSummary } from "../api/types";
import { fold, initialLiveState, type LiveState } from "./fold";

function job(id: string, extra: Partial<JobSummary> = {}): JobSummary {
  return {
    job_id: id,
    label: id,
    weight: 1,
    max_concurrent: null,
    pause_on_error: null,
    paused: false,
    counts: {
      pending: 1,
      running: 0,
      done_ok: 0,
      done_err: 0,
      ghosted: 0,
      unknown: 0,
      total: 1,
    },
    submitted_at: "2026-09-30T13:00:00+09:00",
    home_root: `/data/${id}`,
    alias: `${id}-alias`,
    arena: "",
    pool: "default",
    image_id: "",
    source_sha256: "",
    archived_at: null,
    archive_kind: "",
    ...extra,
  };
}

function run(events: [string, unknown][]): LiveState {
  return events.reduce(
    (s, [ev, payload]) => fold(s, ev, payload),
    initialLiveState,
  );
}

describe("fold", () => {
  it("keeps submission order stable across updates", () => {
    const s = run([
      ["job_updated", job("A")],
      ["job_updated", job("B")],
      ["job_updated", job("A", { paused: true })],
    ]);
    expect(s.order).toEqual(["A", "B"]);
    expect(s.jobs["A"]?.paused).toBe(true);
  });

  it("snapshot resets folded jobs (reconnect replays the world)", () => {
    const s = run([
      ["job_updated", job("stale")],
      ["snapshot", { running_total: 0 }],
      ["job_updated", job("fresh")],
    ]);
    expect(s.order).toEqual(["fresh"]);
  });

  it("cancel badges the last snapshot instead of dropping it", () => {
    const s = run([
      ["job_updated", job("A")],
      ["job_cancelled", { job_id: "A" }],
    ]);
    expect(s.jobs["A"]?.cancelled).toBe(true);
    expect(s.order).toEqual(["A"]);
  });

  it("ignores unknown events and torn payloads", () => {
    const s = run([
      ["job_updated", job("A")],
      ["instance_completed", { job_id: "A" }],
      ["job_updated", {}],
    ]);
    expect(s.order).toEqual(["A"]);
  });

  it("heartbeat is recorded", () => {
    const s = run([["heartbeat", { at: "t0" }]]);
    expect(s.lastHeartbeatAt).toBe("t0");
  });
});
