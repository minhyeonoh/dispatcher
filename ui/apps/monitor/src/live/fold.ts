// SSE folding — the TS port of the server-side monitor client's
// MonitorState.apply(). Pure function: (state, event) → state.
// The stream contract (api/app.py monitor_stream): on connect a
// `snapshot` (cluster) followed by one `job_updated` per job;
// afterwards every state change re-emits `job_updated` +
// `cluster_updated` for the touched job, so folding these three
// (plus `job_cancelled`, which has no job_updated after it —
// the job is gone) reconstructs the whole picture.

import type { ClusterSnapshot, JobSummary } from "../api/types";

export interface JobRow extends JobSummary {
  cancelled?: boolean;
}

export interface LiveState {
  connected: boolean;
  cluster: ClusterSnapshot | null;
  jobs: Record<string, JobRow>;
  /** submission order, stable across updates */
  order: string[];
  lastHeartbeatAt: string | null;
}

export const initialLiveState: LiveState = {
  connected: false,
  cluster: null,
  jobs: {},
  order: [],
  lastHeartbeatAt: null,
};

export function fold(
  state: LiveState,
  event: string,
  payload: unknown,
): LiveState {
  switch (event) {
    case "snapshot":
      // New connection = fresh authoritative feed; drop folded
      // jobs so rows cancelled while we were away don't linger.
      return {
        ...state,
        cluster: payload as ClusterSnapshot,
        jobs: {},
        order: [],
      };
    case "cluster_updated":
      return { ...state, cluster: payload as ClusterSnapshot };
    case "job_updated":
    case "job_submitted": {
      const row = payload as JobRow;
      if (!row.job_id) return state;
      const known = row.job_id in state.jobs;
      return {
        ...state,
        jobs: { ...state.jobs, [row.job_id]: row },
        order: known ? state.order : [...state.order, row.job_id],
      };
    }
    case "job_cancelled": {
      const p = payload as { job_id?: string };
      if (!p.job_id || !(p.job_id in state.jobs)) return state;
      const jobs = { ...state.jobs };
      const prev = jobs[p.job_id];
      if (prev !== undefined)
        jobs[p.job_id] = { ...prev, cancelled: true };
      return { ...state, jobs };
    }
    case "heartbeat": {
      const p = payload as { at?: string };
      return { ...state, lastHeartbeatAt: p.at ?? null };
    }
    default:
      return state;
  }
}
