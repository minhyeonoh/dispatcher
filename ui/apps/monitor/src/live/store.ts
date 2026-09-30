import { create } from "zustand";
import { API } from "../api/client";
import { fold, initialLiveState, type LiveState } from "./fold";

interface LiveStore extends LiveState {
  apply: (event: string, payload: unknown) => void;
  setConnected: (connected: boolean) => void;
}

export const useLive = create<LiveStore>((set) => ({
  ...initialLiveState,
  apply: (event, payload) => set((s) => fold(s, event, payload)),
  setConnected: (connected) => set({ connected }),
}));

const FOLDED_EVENTS = [
  "snapshot",
  "cluster_updated",
  "job_updated",
  "job_submitted",
  "job_cancelled",
  "heartbeat",
] as const;

// Events the fold ignores but other layers care about — the query
// cache learns from these that a job's detail went stale.
const OBSERVED_EVENTS = [
  "job_patched",
  "job_reclaimed",
  "job_retried",
  "job_archived",
  "job_unarchived",
  "job_drained",
] as const;

/** Attach the SSE stream to the store. EventSource reconnects on
 * its own; each reconnect replays snapshot + full job list, so
 * the fold self-heals after a gap.
 *
 * `onEvent` sees every frame the stream carries, folded or not —
 * that is how the query cache hears that a job changed. */
export function startLiveStream(
  onEvent?: (event: string, payload: unknown) => void,
): () => void {
  const source = new EventSource(`${API}/monitor/stream`);
  source.onopen = () => useLive.getState().setConnected(true);
  source.onerror = () => useLive.getState().setConnected(false);
  const folded = new Set<string>(FOLDED_EVENTS);
  for (const name of [...FOLDED_EVENTS, ...OBSERVED_EVENTS]) {
    source.addEventListener(name, (ev: MessageEvent<string>) => {
      let payload: unknown;
      try {
        payload = JSON.parse(ev.data);
      } catch {
        return; // torn frame — the next full snapshot heals it
      }
      if (folded.has(name)) useLive.getState().apply(name, payload);
      onEvent?.(name, payload);
    });
  }
  return () => source.close();
}
