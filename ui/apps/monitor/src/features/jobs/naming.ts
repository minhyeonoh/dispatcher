// How a job is named on screen, in one place — four screens
// disagreeing about which name is primary is how a table becomes
// unreadable.
//
// `label` leads: scanning a sweep, the question is which ARM this
// is, and the label is what the submitter called it. `alias` is a
// handle for naming a job out loud or on a command line (short and
// pronounceable where job_id is neither), so it rides along as
// secondary. Neither is unique or stable — links use job_id.

import type { JobSummary } from "../../api/types";

type Named = Pick<JobSummary, "label" | "alias">;

export function jobTitle(job: Named): string {
  return job.label || job.alias || "(unnamed)";
}

/** The other name, or null when there is only one worth showing. */
export function jobSubtitle(job: Named): string | null {
  return job.label && job.alias ? job.alias : null;
}
