import type { ArenaDetail, ArenaSummary, FullJob } from "./types";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly detail: string,
  ) {
    super(`${status}: ${detail}`);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(path, {
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
  job: (jobId: string) =>
    request<FullJob>(`/jobs/${encodeURIComponent(jobId)}`),
  arenas: () => request<ArenaSummary[]>("/arenas"),
  arena: (path: string) => request<ArenaDetail>(`/arenas/${path}`),
};
