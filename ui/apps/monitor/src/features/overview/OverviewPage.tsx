import {
  Badge,
  Card,
  CardBody,
  CardHeader,
  CardTitle,
  SegmentBar,
} from "@lab/kit";
import { Link } from "@tanstack/react-router";
import { useMemo } from "react";
import { useLive } from "../../live/store";
import { useOrderedJobs } from "../jobs/useJobs";
import { attentionRows, type ReasonKind } from "./attention";

const TONE: Record<ReasonKind, "danger" | "warn" | "neutral"> = {
  unresolved: "danger",
  paused_with_errors: "danger",
  stalled: "warn",
  errors: "neutral",
};

function Attention() {
  const jobs = useOrderedJobs();
  const rows = useMemo(() => attentionRows(jobs), [jobs]);
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          needs attention
          {rows.length > 0 && <Badge tone="danger">{rows.length}</Badge>}
        </CardTitle>
      </CardHeader>
      <CardBody>
        {rows.length === 0 ? (
          <p className="text-sm text-fg-muted">
            Nothing waiting on you.
          </p>
        ) : (
          <ul className="flex flex-col gap-1.5">
            {rows.map(({ job, kind, detail }) => (
              <li
                key={job.job_id}
                className="flex flex-wrap items-center gap-2 text-sm"
              >
                <Badge tone={TONE[kind]}>{kind.replace(/_/g, " ")}</Badge>
                <Link
                  to="/jobs/$jobKey"
                  params={{ jobKey: job.job_id }}
                  className="font-medium text-accent hover:underline"
                >
                  {job.alias || job.label}
                </Link>
                <span className="text-fg-muted">{detail}</span>
              </li>
            ))}
          </ul>
        )}
      </CardBody>
    </Card>
  );
}

function Fleet() {
  const cluster = useLive((s) => s.cluster);
  if (!cluster) return null;
  const hosts = Object.entries(cluster.settings.hosts);
  const pools = Object.entries(cluster.running_per_pool ?? {});
  return (
    <Card>
      <CardHeader>
        <CardTitle>fleet</CardTitle>
        <Link
          to="/hosts"
          className="text-xs text-accent hover:underline"
        >
          hosts →
        </Link>
      </CardHeader>
      <CardBody className="flex flex-col gap-3">
        <div>
          <div className="mb-1 flex items-baseline justify-between text-sm">
            <span className="text-fg-muted">global</span>
            <span className="tabular-nums">
              {cluster.running_total}/{cluster.settings.max_concurrent}
            </span>
          </div>
          <SegmentBar
            segments={[
              { value: cluster.running_total, tone: "accent" },
              {
                value: Math.max(
                  0,
                  cluster.settings.max_concurrent - cluster.running_total,
                ),
                tone: "muted",
              },
            ]}
          />
        </div>
        {hosts.map(([host, hs]) => {
          const running = cluster.running_per_host[host] ?? 0;
          return (
            <div key={host}>
              <div className="mb-1 flex items-baseline justify-between text-sm">
                <span className={hs.active ? "" : "text-fg-faint"}>
                  {host}
                  {!hs.active && " (inactive)"}
                  {!hs.alive && " (dead)"}
                </span>
                <span className="tabular-nums">
                  {running}/{hs.max_concurrent}
                </span>
              </div>
              <SegmentBar
                segments={[
                  { value: running, tone: hs.alive ? "ok" : "danger" },
                  {
                    value: Math.max(0, hs.max_concurrent - running),
                    tone: "muted",
                  },
                ]}
              />
            </div>
          );
        })}
        {pools.length > 0 && (
          <div className="flex flex-wrap gap-1.5 pt-1">
            {pools.map(([pool, n]) => {
              const cap = cluster.settings.pool_caps?.[pool];
              return (
                <Badge key={pool}>
                  {pool} {n}
                  {cap === undefined ? "" : `/${cap}`}
                </Badge>
              );
            })}
          </div>
        )}
      </CardBody>
    </Card>
  );
}

export function OverviewPage() {
  const jobs = useOrderedJobs();
  const live = jobs.filter((j) => !j.cancelled && !j.archived_at);
  const totals = live.reduce(
    (acc, j) => ({
      running: acc.running + j.counts.running,
      pending: acc.pending + j.counts.pending,
      done_ok: acc.done_ok + j.counts.done_ok,
      done_err: acc.done_err + j.counts.done_err,
    }),
    { running: 0, pending: 0, done_ok: 0, done_err: 0 },
  );
  return (
    <div className="flex flex-col gap-4">
      <div className="grid gap-4 lg:grid-cols-2">
        <Attention />
        <Fleet />
      </div>
      <Card>
        <CardHeader>
          <CardTitle>live jobs</CardTitle>
          <Link
            to="/jobs"
            className="text-xs text-accent hover:underline"
          >
            all jobs →
          </Link>
        </CardHeader>
        <CardBody className="flex flex-wrap gap-4 text-sm">
          <span>
            <span className="text-fg-faint">jobs </span>
            <span className="tabular-nums">{live.length}</span>
          </span>
          <span>
            <span className="text-fg-faint">running </span>
            <span className="tabular-nums">{totals.running}</span>
          </span>
          <span>
            <span className="text-fg-faint">pending </span>
            <span className="tabular-nums">{totals.pending}</span>
          </span>
          <span>
            <span className="text-fg-faint">ok </span>
            <span className="tabular-nums">{totals.done_ok}</span>
          </span>
          <span>
            <span className="text-fg-faint">err </span>
            <span className="tabular-nums text-danger">
              {totals.done_err}
            </span>
          </span>
        </CardBody>
      </Card>
    </div>
  );
}
