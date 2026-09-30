// Job URLs are keyed by job_id, which never changes. An alias is
// a rename-able handle, so a link built from one rots — but
// operators read and type aliases, so /jobs/rigorous-unicorn has
// to work. It resolves to the canonical url instead of rendering
// under the alias.

import type { JobRow } from "../../live/fold";

export type Resolution =
  | { kind: "id" }
  | { kind: "alias"; jobId: string }
  | { kind: "unknown" };

export function resolveJobKey(
  key: string,
  jobs: Record<string, JobRow>,
): Resolution {
  if (key in jobs) return { kind: "id" };
  for (const [jobId, row] of Object.entries(jobs)) {
    if (row.alias === key) return { kind: "alias", jobId };
  }
  // Not in the live set: could be an id we simply have not folded
  // yet (fresh connection), or an alias of a job the server knows.
  // The detail fetch decides — 404 there is the real answer.
  return { kind: "unknown" };
}
