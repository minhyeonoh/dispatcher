import { describe, expect, it } from "vitest";
import type { QueryClient } from "@tanstack/react-query";
import { invalidateOnJobEvent, jobIdFromEvent } from "./invalidate";

function fakeClient() {
  const calls: unknown[] = [];
  const client = {
    invalidateQueries: (arg: unknown) => {
      calls.push(arg);
      return Promise.resolve();
    },
  } as unknown as QueryClient;
  return { client, calls };
}

describe("jobIdFromEvent", () => {
  it("picks the job out of a change event", () => {
    expect(jobIdFromEvent("job_updated", { job_id: "j1" })).toBe("j1");
    expect(jobIdFromEvent("job_drained", { job_id: "j2" })).toBe("j2");
  });

  it("ignores events that say nothing about a job's detail", () => {
    expect(jobIdFromEvent("heartbeat", { at: "t" })).toBeNull();
    expect(jobIdFromEvent("snapshot", { running_total: 1 })).toBeNull();
    // Instance-level frames arrive alongside a job_updated for the
    // same job, so acting on them too would double every refetch.
    expect(
      jobIdFromEvent("instance_completed", { job_id: "j1" }),
    ).toBeNull();
  });

  it("tolerates a malformed payload", () => {
    for (const bad of [null, "x", 1, {}, { job_id: 5 }, { job_id: "" }]) {
      expect(jobIdFromEvent("job_updated", bad)).toBeNull();
    }
  });
});

describe("invalidateOnJobEvent", () => {
  it("invalidates exactly that job's detail query", () => {
    const { client, calls } = fakeClient();
    expect(invalidateOnJobEvent(client, "job_patched", { job_id: "j1" })).toBe(
      "j1",
    );
    expect(calls).toEqual([{ queryKey: ["job", "j1"] }]);
  });

  it("does nothing for unrelated events", () => {
    const { client, calls } = fakeClient();
    expect(
      invalidateOnJobEvent(client, "heartbeat", { at: "t" }),
    ).toBeNull();
    expect(calls).toEqual([]);
  });

  it("never throws on a garbage frame", () => {
    const { client } = fakeClient();
    expect(() =>
      invalidateOnJobEvent(client, "job_updated", undefined),
    ).not.toThrow();
  });
});

describe("stable query key", () => {
  it("is the same for the same job across count changes", () => {
    // The point of the change: one cache entry per job, refreshed by
    // invalidation — not a new entry per count (which is what
    // putting counts in the key did).
    const a = jobIdFromEvent("job_updated", { job_id: "j", counts: { ok: 1 } });
    const b = jobIdFromEvent("job_updated", { job_id: "j", counts: { ok: 2 } });
    expect(a).toBe(b);
  });
});
