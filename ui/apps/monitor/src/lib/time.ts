// Time formatting. App-level rather than feature-level: jobs,
// instances and (later) settings all age things the same way.
//
// Every timestamp the dispatcher mints is KST-aware ISO-8601, so
// Date.parse is safe — no "assume local" guessing.

/** Seconds an instance ran, or has been running. */
export function durationSeconds(
  dispatchedAt: string,
  finishedAt: string | null | undefined,
  now: number,
): number {
  const start = Date.parse(dispatchedAt);
  const end = finishedAt ? Date.parse(finishedAt) : now;
  return Math.max(0, (end - start) / 1000);
}

export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60);
  if (m < 60) return `${m}m ${Math.round(seconds % 60)}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

/** "3m ago", "2h ago", "4d ago" — coarse on purpose: the exact
 * instant belongs in a tooltip, this is for scanning a column. */
export function formatAge(iso: string, now: number): string {
  const seconds = (now - Date.parse(iso)) / 1000;
  if (!Number.isFinite(seconds)) return "—";
  if (seconds < 0) return "just now"; // clock skew between hosts
  if (seconds < 45) return "just now";
  const m = Math.round(seconds / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(seconds / 3600);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

/** Full local rendering, for titles/tooltips. */
export function formatExact(iso: string): string {
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}
