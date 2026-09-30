import { Badge, Card, CardBody, CardHeader, CardTitle } from "@lab/kit";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "@tanstack/react-router";
import { api } from "../../api/client";
import type { FullJob, InstanceView } from "../../api/types";
import {
  durationSeconds,
  formatDuration,
  formatExact,
} from "../../lib/time";
import { jobTitle } from "../jobs/naming";

type Bucket = "running" | "unknown" | "ghosted" | "done_err" | "done_ok";

const BUCKETS: Bucket[] = [
  "running",
  "unknown",
  "ghosted",
  "done_err",
  "done_ok",
];

const TONE = {
  running: "accent",
  unknown: "warn",
  ghosted: "warn",
  done_err: "danger",
  done_ok: "ok",
} as const;

/** Where this instance sits, and which task it belongs to. */
function locate(
  job: FullJob,
  instanceId: string,
): { bucket: Bucket; task: string; view: InstanceView } | null {
  for (const bucket of BUCKETS) {
    for (const [task, view] of Object.entries(job[bucket])) {
      if (view.instance_id === instanceId)
        return { bucket, task, view };
    }
  }
  return null;
}

/** Instances of the same task, in sequence order — the requeue
 * chain (`t1__0000001` → `t1__0000002`), which is the history a
 * zombie hunt actually needs. */
function chain(job: FullJob, task: string): InstanceView[] {
  const seen: InstanceView[] = [];
  for (const bucket of BUCKETS) {
    const view = job[bucket][task];
    if (view) seen.push(view);
  }
  return seen.sort((a, b) =>
    a.instance_id.localeCompare(b.instance_id),
  );
}

function Row({ label, children }: { label: string; children: unknown }) {
  return (
    <div className="flex gap-3 py-1">
      <div className="w-28 shrink-0 text-xs text-fg-faint">{label}</div>
      <div className="min-w-0 text-sm">{children as never}</div>
    </div>
  );
}

export function InstancePage() {
  const { jobKey, instanceId } = useParams({
    from: "/jobs/$jobKey/instances/$instanceId",
  });
  const query = useQuery({
    queryKey: ["job", jobKey],
    queryFn: () => api.job(jobKey),
  });
  const job = query.data;
  if (!job)
    return (
      <div className="p-4 text-sm text-fg-faint">
        {query.isError ? String(query.error) : "loading…"}
      </div>
    );
  const found = locate(job, instanceId);
  return (
    <div className="flex flex-col gap-4">
      <div>
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="font-mono text-lg font-semibold">
            {instanceId}
          </h1>
          {found ? (
            <Badge tone={TONE[found.bucket]}>{found.bucket}</Badge>
          ) : (
            <Badge tone="neutral">superseded</Badge>
          )}
        </div>
        <div className="mt-1 text-xs text-fg-faint">
          <Link
            to="/jobs/$jobKey"
            params={{ jobKey: job.job_id }}
            className="text-accent hover:underline"
          >
            {jobTitle(job)}
          </Link>
          {found && <> · task {found.task}</>}
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>instance</CardTitle>
        </CardHeader>
        <CardBody>
          {found ? (
            <>
              <Row label="task">
                <span className="font-mono">{found.task}</span>
              </Row>
              <Row label="host">{found.view.host}</Row>
              <Row label="dispatched">
                {formatExact(found.view.dispatched_at)}
              </Row>
              <Row
                label={found.view.finished_at ? "finished" : "still running"}
              >
                {found.view.finished_at
                  ? formatExact(found.view.finished_at)
                  : "—"}
              </Row>
              <Row label="took">
                {formatDuration(
                  durationSeconds(
                    found.view.dispatched_at,
                    found.view.finished_at,
                    Date.now(),
                  ),
                )}
              </Row>
              <Row label="home">
                <span className="font-mono text-xs break-all">
                  {job.home_root}/{instanceId}
                </span>
              </Row>
            </>
          ) : (
            <p className="text-sm text-fg-muted">
              This instance no longer occupies a bucket — its task was
              requeued or retried, and a later instance holds the
              record. The home dir stays on disk:{" "}
              <span className="font-mono text-xs break-all">
                {job.home_root}/{instanceId}
              </span>
            </p>
          )}
        </CardBody>
      </Card>

      {found && (
        <Card>
          <CardHeader>
            <CardTitle>task instances</CardTitle>
          </CardHeader>
          <CardBody className="flex flex-col gap-1">
            {chain(job, found.task).map((view) => (
              <Link
                key={view.instance_id}
                to="/jobs/$jobKey/instances/$instanceId"
                params={{
                  jobKey: job.job_id,
                  instanceId: view.instance_id,
                }}
                className={`font-mono text-xs hover:underline ${
                  view.instance_id === instanceId
                    ? "text-fg"
                    : "text-accent"
                }`}
              >
                {view.instance_id}
                {view.instance_id === instanceId && " ←"}
              </Link>
            ))}
          </CardBody>
        </Card>
      )}
    </div>
  );
}
