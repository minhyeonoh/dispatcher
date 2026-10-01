// The job table's column registry.
//
// One definition per column, all of them selectable; `DEFAULT_VISIBLE`
// decides which start on. Adding a column means adding one entry —
// including, later, operator-defined extractor columns, which is why
// the accessor is a function of the row rather than a field name.

import { Badge, cn, SegmentBar } from "@lab/kit";
import { Link } from "@tanstack/react-router";
import type { ColumnDef } from "@tanstack/react-table";
import type { JobRow } from "../../live/fold";
import { formatAge, formatExact } from "../../lib/time";
import { describeBlocked } from "./blocked";
import { jobSubtitle, jobTitle } from "./naming";
import {
  formatColumnValue,
  operatorColumnId,
  summariseColumns,
  type OperatorSpec,
} from "./operatorColumns";

/** Extra per-column knowledge the table header/cells need. */
export interface ColumnMeta {
  align?: "right";
  /** Narrow numeric column — tabular figures, tighter padding. */
  numeric?: boolean;
  /** Which section of the column picker this belongs under. Unset =
   * built in. Operator columns carry the arena their `columns`
   * function is registered on, so the picker can say where each one
   * came from instead of leaving it to the header suffix. */
  group?: string;
  /** What the column means, shown in the picker. Built-ins say it
   * here; an operator column's comes from its own
   * `column_descriptions()`, which is why that function is required
   * at registration. */
  description?: string;
}

/** One line per built-in column. Separate from the definitions so the
 * set reads as prose — writing these next to their accessors buried
 * them, and a column whose meaning is only obvious to whoever added it
 * is the kind that gets misread in a screenshot. */
const BUILT_IN_DESCRIPTION: Record<string, string> = {
  job: "the job's label, with its alias underneath",
  arena:
    "the comparison group it belongs to; a path, and a path owns its subtree",
  state: "active, paused, archived or cancelled",
  ok: "done ok — instances that finished with a clean envelope",
  err: "done err — instances whose envelope reported a failure of the work",
  unresolved:
    "unknown plus ghosted: instances that ended with no readable envelope. These block drain and archive, and the resolver is what clears them",
  run: "running right now",
  pnd: "pending — tasks not yet dispatched",
  tot: "total tasks in the job, which is what the other counts are out of",
  done_pct: "done % — finished share of the task list, ok and err together",
  err_rate:
    "err % of SCORED instances — not of the whole task list, so it does not drift down as pending work dispatches",
  progress: "the same counts as a bar, in task order",
  blocked:
    "why pending work is not dispatching right now, straight from the scheduler's own decision rather than a guess",
  submitted: "when the job was accepted",
  pool: "capacity axis — a pool cap limits every job in it at once",
  weight:
    "round-robin share against other jobs; higher dispatches more often",
  max_concurrent:
    "max concurrent for THIS job — its own ceiling on simultaneous instances, under the pool and global caps",
  pause_on_error:
    "pause on error: stop the job at its first failed instance. Unset means auto — on when max concurrent is 1",
  image_id:
    "image id, the immutable one resolved at submit; every instance ran exactly this",
  source_sha256:
    "sha256 of the frozen source archive — the record of what code ran",
  home_root: "home root: the shared directory holding one subdir per instance",
  job_id: "the stable key; aliases are renameable, this is not",
  readout_lag:
    "unscored: finished instances still missing a readout value. 0 means the live path kept up; a dash means no readout service is attached. `dispatcher readout <job>` fills them in",
  pack_lag:
    "unpacked: finished instances whose files are in no squashfs archive, so they read over NFS. 0 means every one is archived; a dash means nothing is packing. `dispatcher pack <job>` adds them",
  columns:
    "every operator-defined column for this row in one cell, compact. Not sortable, and the one that works when the rows do not share a columns function",
};

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
  opts: { danger?: boolean; warn?: boolean } = {},
): JobColumn {
  return {
    id,
    header,
    accessorFn: pick,
    meta: { align: "right", numeric: true },
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

/** Work the dispatcher knows is outstanding for a job: finished
 * instances with no readout value, or with no archive holding them.
 *
 * Null is not zero and must not render as one. Zero means "caught up";
 * null means nothing is watching — no readout service, no packer — and
 * showing that as 0 would be a reassurance nobody earned. A dash says
 * "unknown", which is the honest answer. */
function lagColumn(
  id: string,
  header: string,
  pick: (job: JobRow) => number | null,
): JobColumn {
  return {
    id,
    header,
    // Sort nulls below every real count rather than alongside 0 — a
    // job nobody is watching is not a job that is caught up.
    accessorFn: (job) => pick(job) ?? -1,
    meta: { align: "right", numeric: true },
    cell: ({ row }) => {
      const n = pick(row.original);
      if (n === null)
        return <span className="text-fg-faint" title="not tracked">–</span>;
      return (
        <span className={n > 0 ? "font-medium text-warn" : undefined}>
          {n}
        </span>
      );
    },
  };
}

/** A value the operator's `columns` function produced — dimmed while
 * stale, because a number known to be behind its input must not look
 * like the current one. */
function ColumnValue({
  job,
  value,
}: {
  job: JobRow;
  value: unknown;
}) {
  return (
    <span
      className={job.columns_stale ? "text-fg-faint" : undefined}
      title={
        job.columns_stale
          ? job.columns_error || "recomputing — values changed"
          : undefined
      }
    >
      {formatColumnValue(value)}
    </span>
  );
}

/** One column per (source arena, key) pair the rows carry.
 *
 * A row from a DIFFERENT source shows an em dash, not a blank: blank
 * under a numeric header reads as zero or missing, and the truth here
 * is "another function decides this row's columns". */
function operatorColumns(spec: OperatorSpec): JobColumn[] {
  return spec.columns.map(({ source, key, label, numeric }) => ({
    id: operatorColumnId(source, key),
    header: label,
    meta: {
      group: source,
      ...(numeric ? { align: "right" as const, numeric: true } : {}),
    },
    accessorFn: (job: JobRow) =>
      job.columns_source_arena === source
        ? ((job.columns?.[key] ?? null) as never)
        : (null as never),
    cell: ({ row }: { row: { original: JobRow } }) => {
      const job = row.original;
      if (job.columns_source_arena !== source) {
        return <span className="text-fg-faint">—</span>;
      }
      return <ColumnValue job={job} value={job.columns?.[key] ?? null} />;
    },
  }));
}

export function jobColumns(
  cluster: Parameters<typeof describeBlocked>[2],
  operator: OperatorSpec = { columns: [], multiSource: false },
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
    countColumn("ok", "ok", (j) => j.counts.done_ok),
    countColumn("err", "err", (j) => j.counts.done_err, {
      danger: true,
    }),
    countColumn(
      "unresolved",
      "unres",
      (j) => j.counts.unknown + j.counts.ghosted,
      { warn: true },
    ),
    countColumn("run", "run", (j) => j.counts.running, {
    }),
    countColumn("pnd", "pnd", (j) => j.counts.pending, {
    }),
    countColumn("tot", "tot", (j) => j.counts.total),
    {
      id: "done_pct",
      header: "done",
      accessorFn: (job) => {
        const { done_ok, done_err, total } = job.counts;
        return total === 0 ? 0 : (100 * (done_ok + done_err)) / total;
      },
      meta: { align: "right", numeric: true },
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
    lagColumn(
      "readout_lag",
      "unscored",
      (job) => job.readout_lag ?? null,
    ),
    lagColumn("pack_lag", "unpacked", (job) => job.pack_lag ?? null),
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
      id: "submitted",
      header: "submitted",
      // Sort on the instant, render the age: "2h ago" scans, the
      // exact stamp is one hover away.
      accessorFn: (job) => Date.parse(job.submitted_at),
      meta: { align: "right", numeric: true },
      cell: ({ row }) => {
        const iso = row.original.submitted_at;
        return (
          <span title={formatExact(iso)}>{formatAge(iso, Date.now())}</span>
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
      meta: { align: "right", numeric: true },
    },
    {
      id: "max_concurrent",
      header: "mcw",
      accessorFn: (job) => job.max_concurrent ?? 0,
      meta: {
        align: "right",
        numeric: true,
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
    // Last, and in the operator's own order: these are the columns
    // they defined, and the built-ins are the frame around them.
    ...operatorColumns(operator),
    compactColumn(),
  ];
}

/** The compact form: one cell holding whatever this row's own
 * function returned.
 *
 * Not sortable, and that is the trade — it is the column that works
 * when the rows do not share a function, where per-key columns would
 * be a mostly-empty grid. The per-key columns stay available in the
 * picker for when you want to sort one. */
function compactColumn(): JobColumn {
  return {
    id: "columns",
    header: "columns",
    accessorFn: (job) => summariseColumns(job),
    enableSorting: false,
    cell: ({ row }) => {
      const job = row.original;
      const text = summariseColumns(job);
      if (!text) return <span className="text-fg-faint">—</span>;
      return (
        <span
          className={cn(
            "font-mono text-xs",
            job.columns_stale && "text-fg-faint",
          )}
          title={job.columns_error || undefined}
        >
          {text}
        </span>
      );
    },
  };
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
  operator: OperatorSpec = { columns: [], multiSource: false },
): Record<string, boolean> {
  const on = new Set<string>(DEFAULT_VISIBLE);
  // One function feeding every row means its keys ARE comparable, so
  // they earn headers. Several means they are not, and the compact
  // cell shows each row its own without a grid of em dashes. Same
  // rendering either way — only which starts visible differs.
  if (operator.multiSource) {
    on.add("columns");
  } else {
    for (const c of operator.columns) {
      on.add(operatorColumnId(c.source, c.key));
    }
  }
  return Object.fromEntries(
    columns.map((c) => [String(c.id), on.has(String(c.id))]),
  );
}

/** A column's NAME: exactly the text in its table header.
 *
 * The picker exists to find the column you saw in the table (or to go
 * looking for one), so it has to name it identically — an expanded
 * label like "max concurrent (per job)" beside a header reading `mcw`
 * leaves you matching them up by guesswork, and the expansion is
 * description material anyway. */
export function columnName(column: JobColumn): string {
  return String(column.header ?? column.id);
}

export function columnDescription(column: JobColumn): string {
  return (
    column.meta?.description ?? BUILT_IN_DESCRIPTION[String(column.id)] ?? ""
  );
}
