import { describe, expect, it } from "vitest";
import type { JobRow } from "../../live/fold";
import { resolveJobKey } from "./resolve";

const jobs: Record<string, JobRow> = {
  "job-1": { job_id: "job-1", alias: "rigorous-unicorn" } as JobRow,
  "job-2": { job_id: "job-2", alias: "brave-otter" } as JobRow,
};

describe("resolveJobKey", () => {
  it("passes a job_id through", () => {
    expect(resolveJobKey("job-1", jobs)).toEqual({ kind: "id" });
  });

  it("maps an alias to its canonical job_id", () => {
    expect(resolveJobKey("rigorous-unicorn", jobs)).toEqual({
      kind: "alias",
      jobId: "job-1",
    });
  });

  it("leaves anything else to the detail fetch", () => {
    expect(resolveJobKey("who-dis", jobs)).toEqual({ kind: "unknown" });
  });

  it("prefers id over a colliding alias", () => {
    const shadowed: Record<string, JobRow> = {
      ...jobs,
      "brave-otter": { job_id: "brave-otter", alias: "x" } as JobRow,
    };
    expect(resolveJobKey("brave-otter", shadowed)).toEqual({ kind: "id" });
  });
});
