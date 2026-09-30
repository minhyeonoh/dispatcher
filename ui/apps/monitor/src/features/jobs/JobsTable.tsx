import { cn, Table, TD, TH, THead, TR } from "@lab/kit";
import {
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
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
import { loadPrefs, savePrefs } from "./tablePrefs";

export function JobsTable({
  jobs,
  tableId = "jobs",
}: {
  jobs: JobRow[];
  /** Prefs are stored per table id, so the arena view and the
   * all-jobs view can carry different column sets. */
  tableId?: string;
}) {
  const cluster = useLive((s) => s.cluster);
  const columns = useMemo(() => jobColumns(cluster), [cluster]);
  const defaults = useMemo(() => defaultVisibility(columns), [columns]);

  // Read once — later renders must not clobber the operator's edits.
  const [initial] = useState(() => loadPrefs(tableId, defaults));
  const [visibility, setVisibility] = useState<VisibilityState>(
    initial.visibility,
  );
  const [sorting, setSorting] = useState<SortingState>(initial.sorting);

  useEffect(() => {
    savePrefs(tableId, { visibility, sorting });
  }, [tableId, visibility, sorting]);

  const table = useReactTable({
    data: jobs,
    columns,
    state: { columnVisibility: visibility, sorting },
    onColumnVisibilityChange: setVisibility,
    onSortingChange: setSorting,
    getRowId: (job) => job.job_id,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
  });

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="text-xs text-fg-faint">
          {jobs.length} job{jobs.length === 1 ? "" : "s"}
        </span>
        <ColumnPicker table={table} columns={columns} />
      </div>
      {jobs.length === 0 ? (
        <div className="rounded-panel border border-line p-4 text-sm text-fg-faint">
          no jobs
        </div>
      ) : (
        <div className="overflow-x-auto rounded-panel border border-line">
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
                <TR key={row.id} className="hover:bg-sunken/50">
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
