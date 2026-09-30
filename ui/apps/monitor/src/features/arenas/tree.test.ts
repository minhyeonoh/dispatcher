import { describe, expect, it } from "vitest";
import type { JobCounts } from "../../api/types";
import type { JobRow } from "../../live/fold";
import { buildArenaTree, inSubtree } from "./tree";

function row(arena: string, pending = 1): JobRow {
  const counts: JobCounts = {
    pending,
    running: 0,
    done_ok: 0,
    done_err: 0,
    ghosted: 0,
    unknown: 0,
    total: pending,
  };
  return { arena, counts, job_id: arena + pending } as JobRow;
}

describe("buildArenaTree", () => {
  it("aggregates counts up the path", () => {
    const { roots, ungrouped } = buildArenaTree([
      row("bench/v7/front5", 2),
      row("bench/v7/tail15", 3),
      row("bench/v6", 1),
      row("", 1),
    ]);
    expect(ungrouped).toBe(1);
    const bench = roots.find((n) => n.path === "bench")!;
    expect(bench.jobs).toBe(3);
    expect(bench.counts.pending).toBe(6);
    const v7 = bench.children.find((n) => n.path === "bench/v7")!;
    expect(v7.jobs).toBe(2);
    expect(v7.counts.pending).toBe(5);
    expect(v7.children.map((n) => n.name).sort()).toEqual([
      "front5",
      "tail15",
    ]);
  });

  it("skips cancelled jobs", () => {
    const cancelled = { ...row("bench/v7"), cancelled: true };
    const { roots } = buildArenaTree([cancelled]);
    expect(roots).toEqual([]);
  });
});

describe("inSubtree", () => {
  it("is segment-aware, matching the server rule", () => {
    expect(inSubtree("bench/v7", "bench/v7")).toBe(true);
    expect(inSubtree("bench/v7/front5", "bench/v7")).toBe(true);
    expect(inSubtree("bench/v70", "bench/v7")).toBe(false);
  });
});
