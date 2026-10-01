import {
  Badge,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  Table,
  TD,
  TH,
  THead,
  TR,
} from "@lab/kit";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import { api } from "../../api/client";
import type { FullJob, InstanceView } from "../../api/types";
import { useLive } from "../../live/store";
import { durationSeconds, formatDuration } from "../../lib/time";
import { describeBlocked } from "./blocked";
import { jobSubtitle, jobTitle } from "./naming";
import { resolveJobKey } from "./resolve";

const BUCKETS = [
  "running",
  "unknown",
  "ghosted",
  "done_err",
  "done_ok",
] as const;

const BUCKET_TONE = {
  running: "accent",
  unknown: "warn",
  ghosted: "warn",
  done_err: "danger",
  done_ok: "ok",
} as const;

/** Ticks for a running instance (its elapsed time is live), static
 * once the instance has a finished_at. */
function Elapsed({ view }: { view: InstanceView }) {
  const running = !view.finished_at;
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [running]);
  const seconds = durationSeconds(
    view.dispatched_at,
    view.finished_at,
    now,
  );
  return <>{formatDuration(seconds)}</>;
}

function BucketCard({
  name,
  rows,
  jobId,
}: {
  name: (typeof BUCKETS)[number];
  rows: Record<string, InstanceView>;
  jobId: string;
}) {
  const entries = Object.entries(rows);
  if (entries.length === 0) return null;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          {name}
          <Badge tone={BUCKET_TONE[name]}>{entries.length}</Badge>
        </CardTitle>
      </CardHeader>
      <div className="overflow-x-auto">
        <Table className="min-w-[34rem]">
          <THead>
            <TR>
              <TH>task</TH>
              <TH>instance</TH>
              <TH>host</TH>
              <TH>dispatched</TH>
              <TH className="text-right">
                {name === "running" ? "running for" : "took"}
              </TH>
            </TR>
          </THead>
          <tbody>
            {entries.map(([task, tv]) => (
              <TR key={task}>
                <TD className="font-mono text-xs">{task}</TD>
                <TD className="font-mono text-xs">
                  <Link
                    to="/jobs/$jobKey/instances/$instanceId"
                    params={{ jobKey: jobId, instanceId: tv.instance_id }}
                    className="text-accent hover:underline"
                  >
                    {tv.instance_id}
                  </Link>
                </TD>
                <TD>{tv.host}</TD>
                <TD className="text-fg-muted">
                  {new Date(tv.dispatched_at).toLocaleString()}
                </TD>
                <TD className="text-right tabular-nums text-fg-muted">
                  <Elapsed view={tv} />
                </TD>
              </TR>
            ))}
          </tbody>
        </Table>
      </div>
    </Card>
  );
}

/** Why these pending tasks are not dispatching — the answer comes
 * from the scheduler, so it is the reason, not a guess. */
function BlockedNote({ job }: { job: FullJob }) {
  const cluster = useLive((s) => s.cluster);
  if (!job.blocked)
    return (
      <span className="text-xs text-fg-faint">
        waiting its turn in the rotation
      </span>
    );
  const { label, detail, tone } = describeBlocked(
    job.blocked,
    job,
    cluster,
  );
  return (
    <span className="flex items-center gap-2 text-xs">
      <Badge tone={tone}>{label}</Badge>
      <span className="text-fg-muted">{detail}</span>
    </span>
  );
}

export function JobPage() {
  const { jobKey } = useParams({ from: "/jobs/$jobKey" });
  const navigate = useNavigate();
  const jobs = useLive((s) => s.jobs);
  const resolution = resolveJobKey(jobKey, jobs);

  // An alias in the url resolves to the canonical job_id url:
  // aliases are renameable, so the address bar (and anything
  // copied out of it) must settle on the stable key.
  useEffect(() => {
    if (resolution.kind === "alias") {
      void navigate({
        to: "/jobs/$jobKey",
        params: { jobKey: resolution.jobId },
        replace: true,
      });
    }
  }, [resolution, navigate]);

  const query = useQuery({
    // Stable key: the SSE stream invalidates it when this job
    // changes (see live/invalidate.ts), so the cache entry is
    // reused instead of a new one per count change.
    queryKey: ["job", jobKey],
    queryFn: () => api.job(jobKey),
    enabled: resolution.kind !== "alias",
    placeholderData: (prev) => prev,
  });
  const job = query.data;
  if (!job)
    return (
      <div className="p-4 text-sm text-fg-faint">
        {query.isError ? String(query.error) : "loading…"}
      </div>
    );
  return (
    <div className="flex flex-col gap-4">
      <div>
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-2xl font-semibold tracking-tight">
            {jobTitle(job)}
          </h1>
          {jobSubtitle(job) && (
            <span className="font-mono text-xs text-fg-faint">
              {jobSubtitle(job)}
            </span>
          )}
          {job.archived_at ? (
            <Badge tone="neutral">archived</Badge>
          ) : job.paused ? (
            <Badge tone="warn">paused</Badge>
          ) : (
            <Badge tone="ok">active</Badge>
          )}
          {job.arena && (
            <Link to="/arenas/$" params={{ _splat: job.arena }}>
              <Badge tone="accent">{job.arena}</Badge>
            </Link>
          )}
          <Badge>pool {job.pool}</Badge>
          <Badge>w {job.weight}</Badge>
          {job.max_concurrent !== null && (
            <Badge>mcw {job.max_concurrent}</Badge>
          )}
        </div>
        {/* An absolute NFS path has no spaces to wrap at, so
            without break-all it sets the whole page's width. */}
        <div className="mt-1 font-mono text-xs break-all text-fg-faint">
          {job.job_id} · {job.home_root}
        </div>
      </div>
      {BUCKETS.map((b) => (
        <BucketCard key={b} name={b} rows={job[b]} jobId={job.job_id} />
      ))}
      {job.pending.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              pending
              <Badge>{job.pending.length}</Badge>
            </CardTitle>
            <BlockedNote job={job} />
          </CardHeader>
          <CardBody className="flex flex-wrap gap-1.5">
            {job.pending.map((task) => (
              <span
                key={task}
                className="rounded-control bg-sunken px-1.5 py-0.5 font-mono text-xs"
              >
                {task}
              </span>
            ))}
          </CardBody>
        </Card>
      )}
    </div>
  );
}
