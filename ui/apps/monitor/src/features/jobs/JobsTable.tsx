import {
  Badge,
  SegmentBar,
  Table,
  TD,
  TH,
  THead,
  TR,
} from "@lab/kit";
import { Link } from "@tanstack/react-router";
import type { JobRow } from "../../live/fold";

function StateBadge({ job }: { job: JobRow }) {
  if (job.cancelled) return <Badge tone="danger">cancelled</Badge>;
  if (job.archived_at) return <Badge tone="neutral">archived</Badge>;
  if (job.paused) return <Badge tone="warn">paused</Badge>;
  return <Badge tone="ok">active</Badge>;
}

export function JobsTable({ jobs }: { jobs: JobRow[] }) {
  if (jobs.length === 0)
    return <div className="p-4 text-sm text-fg-faint">no jobs</div>;
  return (
    <Table>
      <THead>
        <TR>
          <TH>job</TH>
          <TH>state</TH>
          <TH className="text-right">ok</TH>
          <TH className="text-right">err</TH>
          <TH className="text-right">run</TH>
          <TH className="text-right">pnd</TH>
          <TH className="text-right">tot</TH>
          <TH className="w-40">progress</TH>
        </TR>
      </THead>
      <tbody>
        {jobs.map((job) => {
          const c = job.counts;
          return (
            <TR key={job.job_id} className="hover:bg-sunken/50">
              <TD>
                <Link
                  to="/jobs/$jobKey"
                  params={{ jobKey: job.job_id }}
                  className="font-medium text-accent hover:underline"
                >
                  {job.alias || job.label}
                </Link>
                {job.alias && (
                  <span className="ml-2 text-xs text-fg-faint">
                    {job.label}
                  </span>
                )}
              </TD>
              <TD>
                <StateBadge job={job} />
              </TD>
              <TD className="text-right tabular-nums">{c.done_ok}</TD>
              <TD
                className={`text-right tabular-nums ${
                  c.done_err > 0 ? "font-medium text-danger" : ""
                }`}
              >
                {c.done_err}
              </TD>
              <TD className="text-right tabular-nums">{c.running}</TD>
              <TD className="text-right tabular-nums">{c.pending}</TD>
              <TD className="text-right tabular-nums">{c.total}</TD>
              <TD>
                <SegmentBar
                  segments={[
                    { value: c.done_ok, tone: "ok" },
                    { value: c.done_err, tone: "danger" },
                    { value: c.unknown + c.ghosted, tone: "warn" },
                    { value: c.running, tone: "accent" },
                    { value: c.pending, tone: "muted" },
                  ]}
                />
              </TD>
            </TR>
          );
        })}
      </tbody>
    </Table>
  );
}
