// What the overview page ranks. The first operational question is
// not "what is running" but "what is going wrong", so a job earns
// a row here only for a reason someone must act on.

import type { JobRow } from "../../live/fold";

export type ReasonKind =
  /** instances ended with no readable outcome and the resolver has
   * not settled them — these block drain and archive */
  | "unresolved"
  /** the job stopped itself on an error (pause_on_error) */
  | "paused_with_errors"
  /** work left, nothing running: capacity, caps, or a pause */
  | "stalled"
  /** finished, but some tasks ended in error */
  | "errors";

export interface Attention {
  job: JobRow;
  kind: ReasonKind;
  detail: string;
}

const RANK: Record<ReasonKind, number> = {
  unresolved: 0,
  paused_with_errors: 1,
  stalled: 2,
  errors: 3,
};

export function attentionRows(jobs: JobRow[]): Attention[] {
  const rows: Attention[] = [];
  for (const job of jobs) {
    if (job.cancelled || job.archived_at) continue;
    const c = job.counts;
    const unresolved = c.unknown + c.ghosted;
    if (unresolved > 0) {
      rows.push({
        job,
        kind: "unresolved",
        detail: `${unresolved} instance(s) in unknown/ghosted`,
      });
      continue;
    }
    if (job.paused && c.done_err > 0 && c.pending + c.running > 0) {
      rows.push({
        job,
        kind: "paused_with_errors",
        detail: `paused with ${c.done_err} error(s), ${c.pending} task(s) left`,
      });
      continue;
    }
    if (c.pending > 0 && c.running === 0) {
      rows.push({
        job,
        kind: "stalled",
        detail: job.paused
          ? `paused, ${c.pending} task(s) waiting`
          : `${c.pending} task(s) waiting, none running`,
      });
      continue;
    }
    if (c.done_err > 0 && c.pending === 0 && c.running === 0) {
      rows.push({
        job,
        kind: "errors",
        detail: `drained with ${c.done_err} error(s)`,
      });
    }
  }
  return rows.sort((a, b) => RANK[a.kind] - RANK[b.kind]);
}
