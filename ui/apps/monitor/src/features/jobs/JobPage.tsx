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
import { useParams } from "@tanstack/react-router";
import { api } from "../../api/client";
import type { InstanceView } from "../../api/types";
import { useLive } from "../../live/store";

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

function BucketCard({
  name,
  rows,
}: {
  name: (typeof BUCKETS)[number];
  rows: Record<string, InstanceView>;
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
      <Table>
        <THead>
          <TR>
            <TH>task</TH>
            <TH>instance</TH>
            <TH>host</TH>
            <TH>dispatched</TH>
          </TR>
        </THead>
        <tbody>
          {entries.map(([task, tv]) => (
            <TR key={task}>
              <TD className="font-mono text-xs">{task}</TD>
              <TD className="font-mono text-xs">{tv.instance_id}</TD>
              <TD>{tv.host}</TD>
              <TD className="text-fg-muted">
                {new Date(tv.dispatched_at).toLocaleString()}
              </TD>
            </TR>
          ))}
        </tbody>
      </Table>
    </Card>
  );
}

export function JobPage() {
  const { jobId } = useParams({ from: "/jobs/$jobId" });
  const live = useLive((s) => s.jobs[jobId]);
  const query = useQuery({
    queryKey: ["job", jobId, live?.counts],
    queryFn: () => api.job(jobId),
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
        <div className="flex items-center gap-2">
          <h1 className="text-lg font-semibold">
            {job.alias || job.label}
          </h1>
          {job.archived_at ? (
            <Badge tone="neutral">archived</Badge>
          ) : job.paused ? (
            <Badge tone="warn">paused</Badge>
          ) : (
            <Badge tone="ok">active</Badge>
          )}
          {job.arena && <Badge tone="accent">{job.arena}</Badge>}
          <Badge>pool {job.pool}</Badge>
          <Badge>w {job.weight}</Badge>
        </div>
        <div className="mt-1 font-mono text-xs text-fg-faint">
          {job.job_id} · {job.home_root}
        </div>
      </div>
      {BUCKETS.map((b) => (
        <BucketCard key={b} name={b} rows={job[b]} />
      ))}
      {job.pending.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              pending
              <Badge>{job.pending.length}</Badge>
            </CardTitle>
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
