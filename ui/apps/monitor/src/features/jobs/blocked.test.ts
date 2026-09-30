import { describe, expect, it } from "vitest";
import type { ClusterSnapshot, JobSummary } from "../../api/types";
import {
  describeBlocked,
  durationSeconds,
  formatDuration,
} from "./blocked";

const job = {
  pool: "gpu",
  max_concurrent: 2,
  counts: {
    pending: 4,
    running: 2,
    done_ok: 1,
    done_err: 0,
    ghosted: 1,
    unknown: 2,
    total: 10,
  },
} as JobSummary;

const cluster = {
  running_total: 500,
  running_per_host: { ml10: 8, ml9: 4 },
  running_per_pool: { gpu: 6 },
  settings: {
    max_concurrent: 500,
    pool_caps: { gpu: 6 },
    hosts: {
      ml10: { max_concurrent: 8, active: true, alive: true },
      ml9: { max_concurrent: 4, active: true, alive: true },
    },
  },
} as unknown as ClusterSnapshot;

describe("describeBlocked", () => {
  it("names the number that is full for a job cap", () => {
    expect(describeBlocked("job_cap", job, cluster).detail).toBe(
      "2/2 of its own limit",
    );
  });

  it("names the pool and its cap", () => {
    const out = describeBlocked("pool_cap", job, cluster);
    expect(out.label).toBe("pool gpu full");
    expect(out.detail).toBe("6/6 in this pool");
  });

  it("counts unresolved instances for awaiting_resolution", () => {
    const out = describeBlocked("awaiting_resolution", job, cluster);
    expect(out.detail).toContain("3 unresolved");
    expect(out.tone).toBe("danger");
  });

  it("lists per-host utilisation when no host has room", () => {
    expect(describeBlocked("no_host", job, cluster).detail).toBe(
      "all capacity in use — ml10 8/8, ml9 4/4",
    );
  });

  it("says so when every host is down rather than blaming capacity", () => {
    const dead = {
      ...cluster,
      settings: {
        ...cluster.settings,
        hosts: {
          ml10: { max_concurrent: 8, active: false, alive: true },
          ml9: { max_concurrent: 4, active: true, alive: false },
        },
      },
    } as unknown as ClusterSnapshot;
    expect(describeBlocked("no_host", job, dead).detail).toContain(
      "inactive or dead",
    );
  });

  it("degrades without a cluster snapshot instead of throwing", () => {
    for (const reason of [
      "paused",
      "job_cap",
      "pool_cap",
      "awaiting_resolution",
      "global_cap",
      "no_host",
      "no_pending",
    ] as const) {
      expect(describeBlocked(reason, job, null).label).toBeTruthy();
    }
  });
});

describe("durations", () => {
  const start = "2026-09-30T13:00:00+09:00";

  it("measures a finished instance from its own timestamps", () => {
    expect(
      durationSeconds(start, "2026-09-30T13:02:30+09:00", 0),
    ).toBe(150);
  });

  it("measures a running instance against now", () => {
    const now = Date.parse(start) + 90_000;
    expect(durationSeconds(start, null, now)).toBe(90);
  });

  it("never goes negative on clock skew", () => {
    expect(durationSeconds(start, null, Date.parse(start) - 5000)).toBe(
      0,
    );
  });

  it("formats across the units", () => {
    expect(formatDuration(45)).toBe("45s");
    expect(formatDuration(150)).toBe("2m 30s");
    expect(formatDuration(7260)).toBe("2h 1m");
  });
});
