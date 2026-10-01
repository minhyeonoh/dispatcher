import { cn, Table, TD, TH, THead, TR } from "@lab/kit";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
  type Column,
  type SortingState,
  type VisibilityState,
} from "@tanstack/react-table";
import { useEffect, useMemo, useState } from "react";
import type { JobRow } from "../../live/fold";
import { useLive } from "../../live/store";
import { ColumnPicker } from "./ColumnPicker";
import {
  columnTitle,
  defaultVisibility,
  jobColumns,
  type ColumnMeta,
} from "./columns";
import { filterJobs, type JobFilter } from "./filter";
import { JobsToolbar } from "./JobsToolbar";
import { collectOperatorColumns } from "./operatorColumns";
import { loadPrefs, mergeVisibility, savePrefs } from "./tablePrefs";

/** Nineteen columns means horizontal scrolling, and scrolling away
 * from the job name leaves rows unidentifiable — so the name column
 * is pinned. It carries its own opaque background (rows are
 * transparent over the panel) or the scrolled cells would show
 * through it. */
function pinnedClass(column: Column<JobRow, unknown>): string | false {
  return (
    column.getIsPinned() === "left" &&
    "sticky left-0 z-10 bg-raised group-hover:bg-sunken border-r border-line"
  );
}

export function JobsTable({
  jobs,
  filter,
  onFilterChange,
  tableId = "jobs",
}: {
  jobs: JobRow[];
  filter: JobFilter;
  onFilterChange: (next: JobFilter) => void;
  /** Prefs are stored per table id, so the arena view and the
   * all-jobs view can carry different column sets. */
  tableId?: string;
}) {
  const cluster = useLive((s) => s.cluster);
  const rows = useMemo(() => filterJobs(jobs, filter), [jobs, filter]);

  // Operator columns come from the rows, so they appear and vanish as
  // the filter moves. Memoised on a digest of the (source, key) pairs
  // rather than on `rows`: the data changes on every SSE frame and
  // rebuilding the column defs each time would churn the table for
  // nothing.
  const operator = useMemo(() => collectOperatorColumns(rows), [rows]);
  const operatorKey = operator.columns
    .map((c) => `${c.source}/${c.key}/${c.label}/${c.numeric}`)
    .join("|");
  const stableOperator = useMemo(
    () => operator,
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [operatorKey, operator.multiSource],
  );

  const columns = useMemo(
    () => jobColumns(cluster, stableOperator),
    [cluster, stableOperator],
  );
  const defaults = useMemo(
    () => defaultVisibility(columns, stableOperator),
    [columns, stableOperator],
  );

  // Read once — later renders must not clobber the operator's edits.
  const [initial] = useState(() => loadPrefs(tableId, defaults));
  const [visibility, setVisibility] = useState<VisibilityState>(
    initial.visibility,
  );
  const [sorting, setSorting] = useState<SortingState>(initial.sorting);

  // Operator columns can appear after the first render (a readout
  // lands, or the filter widens). TanStack treats an id missing from
  // the visibility record as VISIBLE, so a new column would show up
  // regardless of its default — the same merge that runs once at load
  // has to run again whenever the column set changes.
  useEffect(() => {
    setVisibility((prev) => {
      const next = mergeVisibility(prev, defaults);
      const same =
        Object.keys(next).length === Object.keys(prev).length &&
        Object.entries(next).every(([id, on]) => prev[id] === on);
      return same ? prev : next;
    });
  }, [defaults]);

  useEffect(() => {
    savePrefs(tableId, { visibility, sorting });
  }, [tableId, visibility, sorting]);

  const table = useReactTable({
    data: rows,
    columns,
    state: {
      columnVisibility: visibility,
      sorting,
      columnPinning: { left: ["job"], right: [] },
    },
    onColumnVisibilityChange: setVisibility,
    onSortingChange: setSorting,
    getRowId: (job) => job.job_id,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  return (
    <div className="flex flex-col gap-2">
      <JobsToolbar
        filter={filter}
        onChange={onFilterChange}
        total={jobs.length}
        shown={rows.length}
      >
        <ColumnPicker table={table} columns={columns} />
      </JobsToolbar>
      {rows.length === 0 ? (
        <div className="rounded-panel border border-line p-4 text-sm text-fg-faint">
          {jobs.length === 0 ? "no jobs" : "no jobs match this filter"}
        </div>
      ) : (
        <div className="overflow-x-auto rounded-panel border border-line bg-raised">
          <Table>
            <THead>
              {table.getHeaderGroups().map((group) => (
                <TR key={group.id}>
                  {group.headers.map((header) => {
                    const meta = header.column.columnDef.meta as
                      | ColumnMeta
                      | undefined;
                    const sortable = header.column.getCanSort();
                    const dir = header.column.getIsSorted();
                    return (
                      <TH
                        key={header.id}
                        className={cn(
                          meta?.align === "right" && "text-right",
                          sortable && "cursor-pointer select-none",
                          header.column.getIsPinned() === "left" &&
                            "sticky left-0 z-10 bg-raised border-r border-line",
                        )}
                        onClick={
                          sortable
                            ? header.column.getToggleSortingHandler()
                            : undefined
                        }
                        title={
                          sortable
                            ? `sort by ${columnTitle(
                                header.column.columnDef,
                              )}`
                            : undefined
                        }
                      >
                        {flexRender(
                          header.column.columnDef.header,
                          header.getContext(),
                        )}
                        {dir && (
                          <span className="ml-1 text-accent">
                            {dir === "desc" ? "▾" : "▴"}
                          </span>
                        )}
                      </TH>
                    );
                  })}
                </TR>
              ))}
            </THead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <TR key={row.id} className="group hover:bg-sunken">
                  {row.getVisibleCells().map((cell) => {
                    const meta = cell.column.columnDef.meta as
                      | ColumnMeta
                      | undefined;
                    return (
                      <TD
                        key={cell.id}
                        className={cn(
                          meta?.align === "right" && "text-right",
                          meta?.numeric && "tabular-nums",
                          pinnedClass(cell.column),
                        )}
                      >
                        {flexRender(
                          cell.column.columnDef.cell,
                          cell.getContext(),
                        )}
                      </TD>
                    );
                  })}
                </TR>
              ))}
            </tbody>
          </Table>
        </div>
      )}
    </div>
  );
}
