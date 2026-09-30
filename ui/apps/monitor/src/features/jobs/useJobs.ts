import { useMemo } from "react";
import type { JobRow } from "../../live/fold";
import { useLive } from "../../live/store";

/** Live jobs in submission order. */
export function useOrderedJobs(): JobRow[] {
  const jobs = useLive((s) => s.jobs);
  const order = useLive((s) => s.order);
  return useMemo(
    () =>
      order.flatMap((id) => {
        const row = jobs[id];
        return row ? [row] : [];
      }),
    [jobs, order],
  );
}
