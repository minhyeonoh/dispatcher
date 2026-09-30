// Aliases over the generated OpenAPI schemas (types.gen.ts is
// produced by `pnpm gen:api` from the server's wire.py — that
// file is the source of truth; never edit it).
import type { components } from "./types.gen";

export type JobSummary = components["schemas"]["JobSummaryOut"];
export type JobCounts = components["schemas"]["JobCountsOut"];
export type ArenaSummary = components["schemas"]["ArenaSummaryOut"];
export type ArenaDetail = components["schemas"]["ArenaDetailOut"];
export type StateOut = components["schemas"]["StateOut"];
// ClusterSnapshotOut itself is not referenced by any typed
// endpoint (the SSE stream is untyped), but StateOut extends it —
// the SSE `snapshot`/`cluster_updated` payload is exactly this.
export type ClusterSnapshot = Omit<StateOut, "jobs">;

// GET /jobs/{id} is `response_model=None` on the server (archived
// jobs splice cached bytes), so FullJobOut never reaches the
// OpenAPI document. Mirrors wire.py FullJobOut — keep in sync.
export interface InstanceView {
  instance_id: string;
  host: string;
  dispatched_at: string;
}

export interface FullJob extends JobSummary {
  pending: string[];
  running: Record<string, InstanceView>;
  done_ok: Record<string, InstanceView>;
  done_err: Record<string, InstanceView>;
  ghosted: Record<string, InstanceView>;
  unknown: Record<string, InstanceView>;
}
