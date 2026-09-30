import { create } from "zustand";
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

/** Attach the SSE stream to the store. EventSource reconnects on
 * its own; each reconnect replays snapshot + full job list, so
 * the fold self-heals after a gap. */
export function startLiveStream(): () => void {
  const source = new EventSource("/monitor/stream");
  source.onopen = () => useLive.getState().setConnected(true);
  source.onerror = () => useLive.getState().setConnected(false);
  for (const name of FOLDED_EVENTS) {
    source.addEventListener(name, (ev: MessageEvent<string>) => {
      let payload: unknown;
      try {
        payload = JSON.parse(ev.data);
      } catch {
        return; // torn frame — the next full snapshot heals it
      }
      useLive.getState().apply(name, payload);
    });
  }
  return () => source.close();
}
