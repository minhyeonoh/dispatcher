import type {
  ArenaDetail,
  ArenaSummary,
  FullJob,
  InstanceOutcome,
  StateOut,
} from "./types";

/** The JSON surface. The root url space belongs to the pages. */
export const API = "/api";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: string,
  ) {
    super(`${status}: ${detail}`);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(API + path, {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
  if (!resp.ok) {
    let detail = resp.statusText;
    try {
      const body: unknown = await resp.json();
      if (
        typeof body === "object" &&
        body !== null &&
        "detail" in body &&
        typeof body.detail === "string"
      )
        detail = body.detail;
    } catch {
      // non-JSON error body — keep statusText
    }
    throw new ApiError(resp.status, detail);
  }
  return resp.json() as Promise<T>;
}

export const api = {
  state: () => request<StateOut>("/state"),
  job: (jobId: string) =>
    request<FullJob>(`/jobs/${encodeURIComponent(jobId)}`),
  instanceOutcome: (jobId: string, instanceId: string) =>
    request<InstanceOutcome>(
      `/jobs/${encodeURIComponent(jobId)}/instances/` +
        `${encodeURIComponent(instanceId)}/outcome`,
    ),
  /** What is registered where, and what each operator column means.
   * Changes only when someone re-registers, so callers cache it. */
  readouts: () =>
    request<{
      by_arena: Record<string, unknown>;
      column_descriptions: Record<string, Record<string, string>>;
    }>("/readouts"),
  arenas: () => request<ArenaSummary[]>("/arenas"),
  arena: (path: string) => request<ArenaDetail>(`/arenas/${path}`),
};
