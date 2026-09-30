// Rendering for the server's block reason. The server answers WHY
// in one word (it is the scheduler's own decision); turning that
// into something an operator can act on — including the cap that
// is actually full — happens here, where the cluster snapshot is
// already at hand.

import type { ClusterSnapshot, JobSummary } from "../../api/types";

export type BlockReason = NonNullable<JobSummary["blocked"]>;

export interface BlockedText {
  label: string;
  /** what to do about it, or the number that is full */
  detail: string;
  tone: "warn" | "danger" | "neutral";
}

export function describeBlocked(
  reason: BlockReason,
  job: JobSummary,
  cluster: ClusterSnapshot | null,
): BlockedText {
  const settings = cluster?.settings;
  switch (reason) {
    case "paused":
      return {
        label: "paused",
        detail: "resume to let it dispatch",
        tone: "warn",
      };
    case "job_cap":
      return {
        label: "job cap",
        detail: `${job.counts.running}/${job.max_concurrent} of its own limit`,
        tone: "neutral",
      };
    case "pool_cap": {
      const cap = settings?.pool_caps?.[job.pool];
      const running = cluster?.running_per_pool?.[job.pool];
      return {
        label: `pool ${job.pool} full`,
        detail:
          cap === undefined
            ? "the pool cap is reached"
            : `${running ?? "?"}/${cap} in this pool`,
        tone: "neutral",
      };
    }
    case "awaiting_resolution":
      return {
        label: "awaiting resolution",
        detail:
          `${job.counts.unknown + job.counts.ghosted} unresolved ` +
          "instance(s); pause_on_error holds dispatch until the " +
          "resolver settles them",
        tone: "danger",
      };
    case "global_cap":
      return {
        label: "cluster cap full",
        detail: `${cluster?.running_total ?? "?"}/${
          settings?.max_concurrent ?? "?"
        } across all jobs`,
        tone: "neutral",
      };
    case "no_host":
      return {
        label: "no host with room",
        detail: hostDetail(cluster),
        tone: "warn",
      };
    case "no_pending":
      // The server reports null for this; kept for exhaustiveness.
      return { label: "nothing pending", detail: "", tone: "neutral" };
  }
}

function hostDetail(cluster: ClusterSnapshot | null): string {
  const hosts = Object.entries(cluster?.settings.hosts ?? {});
  if (hosts.length === 0) return "no hosts configured";
  const down = hosts.filter(([, h]) => !h.active || !h.alive);
  if (down.length === hosts.length) {
    return `every host is inactive or dead (${down
      .map(([n]) => n)
      .join(", ")})`;
  }
  const full = hosts
    .filter(([, h]) => h.active && h.alive)
    .map(([name, h]) => {
      const running = cluster?.running_per_host?.[name] ?? 0;
      return `${name} ${running}/${h.max_concurrent}`;
    });
  return `all capacity in use — ${full.join(", ")}`;
}
