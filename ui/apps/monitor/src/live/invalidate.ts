// Which SSE events mean a job's DETAIL (its buckets, instances,
// timings — everything the summary in the stream does not carry) is
// now stale.
//
// The alternative, stuffing the live counts into the query key, is
// what this replaces: that made every count change mint a NEW cache
// entry, so a busy sweep left a trail of dead ones behind and the
// "refetch" was really a cache miss in disguise.

import type { QueryClient } from "@tanstack/react-query";

/** Events after which the server re-emits the job's summary — i.e.
 * something about that job changed. */
const JOB_CHANGED = new Set([
  "job_updated",
  "job_submitted",
  "job_patched",
  "job_cancelled",
  "job_reclaimed",
  "job_retried",
  "job_archived",
  "job_unarchived",
  "job_drained",
]);

export function jobIdFromEvent(
  event: string,
  payload: unknown,
): string | null {
  if (!JOB_CHANGED.has(event)) return null;
  if (typeof payload !== "object" || payload === null) return null;
  const id = (payload as Record<string, unknown>).job_id;
  return typeof id === "string" && id ? id : null;
}

/** Mark one job's detail stale. Active queries refetch; inactive
 * ones simply refetch next time they mount, so this is cheap even
 * when the event is for a job nobody is looking at. */
export function invalidateOnJobEvent(
  queryClient: QueryClient,
  event: string,
  payload: unknown,
): string | null {
  const jobId = jobIdFromEvent(event, payload);
  if (jobId === null) return null;
  void queryClient.invalidateQueries({ queryKey: ["job", jobId] });
  return jobId;
}
