// Arena tree — pure derivation from the live job list, mirroring
// the server's subtree semantics (a path owns `== path` and
// `path + "/"` descendants; segment-aware, so v7 ≠ v70).

import type { JobCounts } from "../../api/types";
import type { JobRow } from "../../live/fold";

export interface ArenaNode {
  /** last path segment, for display */
  name: string;
  /** full path — the API/route key */
  path: string;
  /** jobs in the whole subtree */
  jobs: number;
  counts: JobCounts;
  children: ArenaNode[];
}

const ZERO: JobCounts = {
  pending: 0,
  running: 0,
  done_ok: 0,
  done_err: 0,
  ghosted: 0,
  unknown: 0,
  total: 0,
};

function addCounts(a: JobCounts, b: JobCounts): JobCounts {
  return {
    pending: a.pending + b.pending,
    running: a.running + b.running,
    done_ok: a.done_ok + b.done_ok,
    done_err: a.done_err + b.done_err,
    ghosted: a.ghosted + b.ghosted,
    unknown: a.unknown + b.unknown,
    total: a.total + b.total,
  };
}

export function buildArenaTree(jobs: JobRow[]): {
  roots: ArenaNode[];
  ungrouped: number;
} {
  const roots: ArenaNode[] = [];
  let ungrouped = 0;
  for (const job of jobs) {
    if (job.cancelled) continue;
    const arena = job.arena ?? "";
    if (!arena) {
      ungrouped += 1;
      continue;
    }
    let level = roots;
    let path = "";
    for (const segment of arena.split("/")) {
      path = path ? `${path}/${segment}` : segment;
      let node = level.find((n) => n.path === path);
      if (!node) {
        node = {
          name: segment,
          path,
          jobs: 0,
          counts: { ...ZERO },
          children: [],
        };
        level.push(node);
      }
      node.jobs += 1;
      node.counts = addCounts(node.counts, job.counts);
      level = node.children;
    }
  }
  return { roots, ungrouped };
}

/** The server's membership rule, client-side. */
export function inSubtree(jobArena: string, path: string): boolean {
  return jobArena === path || jobArena.startsWith(path + "/");
}
