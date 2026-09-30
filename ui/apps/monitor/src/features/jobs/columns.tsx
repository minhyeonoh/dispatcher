// The job table's column registry.
//
// One definition per column, all of them selectable; `DEFAULT_VISIBLE`
// decides which start on. Adding a column means adding one entry —
// including, later, operator-defined extractor columns, which is why
// the accessor is a function of the row rather than a field name.

import { Badge, SegmentBar } from "@lab/kit";
import { Link } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import type { JobRow } from "../../live/fold";
import { describeBlocked } from "./blocked";
import { jobSubtitle, jobTitle } from "./naming";

/** Extra per-column knowledge the table header/cells need. */
export interface ColumnMeta {
  /** Shown in the column picker; falls back to the header text. */
  title?: string;
  align?: "right";
  /** Narrow numeric column — tabular figures, tighter padding. */
  numeric?: boolean;
}

export type JobColumn = ColumnDef<JobRow> & { meta?: ColumnMeta };

function StateBadge({ job }: { job: JobRow }) {
  if (job.cancelled) return <Badge tone="danger">cancelled</Badge>;
  if (job.archived_at) return <Badge tone="neutral">archived</Badge>;
  if (job.paused) return <Badge tone="warn">paused</Badge>;
  return <Badge tone="ok">active</Badge>;
}

/** Numeric count column; `tone` colours a non-zero value. */
function countColumn(
  id: string,
  header: string,
  pick: (job: JobRow) => number,
  opts: { title?: string; danger?: boolean; warn?: boolean } = {},
): JobColumn {
  return {
    id,
    header,
    accessorFn: pick,
    meta: { align: "right", numeric: true, ...(opts.title ? { title: opts.title } : {}) },
    cell: ({ getValue }) => {
      const n = getValue<number>();
      const hot = n > 0 && (opts.danger || opts.warn);
      return (
        <span
          className={
            hot
              ? opts.danger
                ? "font-medium text-danger"
                : "font-medium text-warn"
              : undefined
          }
        >
          {n}
        </span>
      );
    },
  };
}

export function jobColumns(
  cluster: Parameters<typeof describeBlocked>[2],
): JobColumn[] {
  return [
    {
      id: "job",
      header: "job",
      accessorFn: (job) => jobTitle(job),
      enableHiding: false, // the row needs something to click
      cell: ({ row }) => {
        const job = row.original;
        return (
          <>
            <Link
              to="/jobs/$jobKey"
              params={{ jobKey: job.job_id }}
              className="font-medium text-accent hover:underline"
            >
              {jobTitle(job)}
            </Link>
            {jobSubtitle(job) && (
              <span className="ml-2 font-mono text-xs text-fg-faint">
                {jobSubtitle(job)}
              </span>
            )}
          </>
        );
      },
    },
    {
      id: "arena",
      header: "arena",
      accessorFn: (job) => job.arena ?? "",
      cell: ({ row }) => {
        const arena = row.original.arena;
        if (!arena)
          return <span className="text-fg-faint">ungrouped</span>;
        return (
          <Link
            to="/arenas/$"
            params={{ _splat: arena }}
            className="font-mono text-xs text-accent hover:underline"
          >
            {arena}
          </Link>
        );
      },
    },
    {
      id: "state",
      header: "state",
      accessorFn: (job) =>
        job.cancelled
          ? "cancelled"
          : job.archived_at
            ? "archived"
            : job.paused
              ? "paused"
              : "active",
      cell: ({ row }) => <StateBadge job={row.original} />,
    },
    countColumn("ok", "ok", (j) => j.counts.done_ok, { title: "done ok" }),
    countColumn("err", "err", (j) => j.counts.done_err, {
      title: "done err",
      danger: true,
    }),
    countColumn(
      "unresolved",
      "unres",
      (j) => j.counts.unknown + j.counts.ghosted,
      { title: "unresolved (unknown + ghosted)", warn: true },
    ),
    countColumn("run", "run", (j) => j.counts.running, {
      title: "running",
    }),
    countColumn("pnd", "pnd", (j) => j.counts.pending, {
      title: "pending",
    }),
    countColumn("tot", "tot", (j) => j.counts.total, { title: "tasks" }),
    {
      id: "done_pct",
      header: "done",
      accessorFn: (job) => {
        const { done_ok, done_err, total } = job.counts;
        return total === 0 ? 0 : (100 * (done_ok + done_err)) / total;
      },
      meta: { align: "right", numeric: true, title: "done %" },
      cell: ({ getValue }) => `${Math.floor(getValue<number>())}%`,
    },
    {
      id: "err_rate",
      header: "err %",
      accessorFn: (job) => {
        const scored = job.counts.done_ok + job.counts.done_err;
        return scored === 0 ? 0 : (100 * job.counts.done_err) / scored;
      },
      meta: {
        align: "right",
        numeric: true,
        title: "err % (of scored)",
      },
      cell: ({ getValue, row }) => {
        const scored =
          row.original.counts.done_ok + row.original.counts.done_err;
        if (scored === 0) return <span className="text-fg-faint">–</span>;
        const pct = getValue<number>();
        return (
          <span className={pct > 0 ? "text-danger" : undefined}>
            {pct.toFixed(0)}%
          </span>
        );
      },
    },
    {
      id: "progress",
      header: "progress",
      enableSorting: false,
      cell: ({ row }) => {
        const c = row.original.counts;
        return (
          <SegmentBar
            className="w-36"
            segments={[
              { value: c.done_ok, tone: "ok" },
              { value: c.done_err, tone: "danger" },
              { value: c.unknown + c.ghosted, tone: "warn" },
              { value: c.running, tone: "accent" },
              { value: c.pending, tone: "muted" },
            ]}
          />
        );
      },
    },
    {
      id: "blocked",
      header: "blocked",
      accessorFn: (job) => job.blocked ?? "",
      cell: ({ row }) => {
        const job = row.original;
        if (!job.blocked)
          return <span className="text-fg-faint">–</span>;
        const { label, detail, tone } = describeBlocked(
          job.blocked,
          job,
          cluster,
        );
        return (
          <Badge tone={tone} title={detail}>
            {label}
          </Badge>
        );
      },
    },
    {
      id: "pool",
      header: "pool",
      accessorFn: (job) => job.pool,
    },
    {
      id: "weight",
      header: "w",
      accessorFn: (job) => job.weight,
      meta: { align: "right", numeric: true, title: "weight" },
    },
    {
      id: "max_concurrent",
      header: "mcw",
      accessorFn: (job) => job.max_concurrent ?? 0,
      meta: {
        align: "right",
        numeric: true,
        title: "max concurrent (per job)",
      },
      cell: ({ row }) =>
        row.original.max_concurrent ?? (
          <span className="text-fg-faint">–</span>
        ),
    },
    {
      id: "pause_on_error",
      header: "on error",
      accessorFn: (job) =>
        job.pause_on_error === null ? "auto" : String(job.pause_on_error),
      meta: { title: "pause on error" },
      cell: ({ getValue }) => (
        <span className="text-xs text-fg-muted">
          {getValue<string>() === "true"
            ? "pause"
            : getValue<string>() === "false"
              ? "continue"
              : "auto"}
        </span>
      ),
    },
    {
      id: "image_id",
      header: "image",
      accessorFn: (job) => job.image_id,
      meta: { title: "image id (pinned)" },
      cell: ({ getValue }) => {
        const id = getValue<string>().replace(/^sha256:/, "");
        return id ? (
          <span className="font-mono text-xs" title={getValue<string>()}>
            {id.slice(0, 12)}
          </span>
        ) : (
          <span className="text-fg-faint">–</span>
        );
      },
    },
    {
      id: "source_sha256",
      header: "source",
      accessorFn: (job) => job.source_sha256,
      meta: { title: "source archive sha256" },
      cell: ({ getValue }) => {
        const sha = getValue<string>();
        return sha ? (
          <span className="font-mono text-xs" title={sha}>
            {sha.slice(0, 12)}
          </span>
        ) : (
          <span className="text-fg-faint">–</span>
        );
      },
    },
    {
      id: "home_root",
      header: "home",
      accessorFn: (job) => job.home_root,
      meta: { title: "home root" },
      cell: ({ getValue }) => (
        <span className="font-mono text-xs break-all text-fg-muted">
          {getValue<string>()}
        </span>
      ),
    },
    {
      id: "job_id",
      header: "job_id",
      accessorFn: (job) => job.job_id,
      cell: ({ getValue }) => (
        <span className="font-mono text-xs break-all text-fg-muted">
          {getValue<string>()}
        </span>
      ),
    },
  ];
}

/** On by default: enough to answer "how is the sweep going" without
 * scrolling sideways. Everything else is one click away. */
export const DEFAULT_VISIBLE = [
  "job",
  "state",
  "ok",
  "err",
  "unresolved",
  "run",
  "pnd",
  "tot",
  "progress",
  "blocked",
] as const;

export function defaultVisibility(
  columns: JobColumn[],
): Record<string, boolean> {
  const on = new Set<string>(DEFAULT_VISIBLE);
  return Object.fromEntries(
    columns.map((c) => [String(c.id), on.has(String(c.id))]),
  );
}

export function columnTitle(column: JobColumn): string {
  return column.meta?.title ?? String(column.header ?? column.id);
}
