import { Button, Checkbox, cn, Dropdown } from "@lab/kit";
import type { Column, Table } from "@tanstack/react-table";
import type { JobRow } from "../../live/fold";
import {
  columnTitle,
  defaultVisibility,
  type ColumnMeta,
  type JobColumn,
} from "./columns";
import type { OperatorSpec } from "./operatorColumns";

const BUILT_IN = "built in";

/** Columns bucketed by section, in registry order, groups in
 * first-appearance order.
 *
 * Not by adjacency: the compact `columns` column is defined last but
 * belongs with the built-ins, and a naive scan would give it a second
 * "built in" heading at the bottom. */
function sections(
  all: Column<JobRow, unknown>[],
  byId: Map<string, JobColumn>,
): [string, Column<JobRow, unknown>[]][] {
  const buckets = new Map<string, Column<JobRow, unknown>[]>();
  for (const column of all) {
    const meta = byId.get(column.id)?.meta as ColumnMeta | undefined;
    const group = meta?.group || BUILT_IN;
    const bucket = buckets.get(group);
    if (bucket) bucket.push(column);
    else buckets.set(group, [column]);
  }
  return [...buckets.entries()];
}

export function ColumnPicker({
  table,
  columns,
  operator,
}: {
  table: Table<JobRow>;
  columns: JobColumn[];
  /** Needed so "reset to defaults" lands on the SAME defaults the
   * table started with — they depend on how many `columns` functions
   * feed the visible rows. */
  operator: OperatorSpec;
}) {
  const all = table.getAllLeafColumns();
  const shown = all.filter((c) => c.getIsVisible()).length;
  const byId = new Map(columns.map((c) => [String(c.id), c]));
  const groups = sections(all, byId);
  return (
    <Dropdown
      trigger={
        <>
          <span>columns</span>
          <span className="text-xs text-fg-faint">
            {shown}/{all.length}
          </span>
        </>
      }
      panelClassName="max-h-[70vh] overflow-y-auto"
    >
      <div className="flex flex-col">
        {groups.map(([group, members], i) => (
          <div key={group}>
            {/* A heading only earns its space once there is more than
                one section — with nothing operator-defined the list
                looks exactly as it did. */}
            {groups.length > 1 && (
              <div
                className={cn(
                  "px-2 pb-0.5 text-xs font-medium tracking-wide",
                  "text-fg-faint uppercase",
                  i > 0 && "mt-2 border-t border-line pt-2",
                )}
              >
                {group}
              </div>
            )}
            {members.map((column) => {
              const def = byId.get(column.id);
              return (
                <Checkbox
                  key={column.id}
                  checked={column.getIsVisible()}
                  disabled={!column.getCanHide()}
                  onChange={column.getToggleVisibilityHandler()}
                >
                  {def ? columnTitle(def) : column.id}
                </Checkbox>
              );
            })}
          </div>
        ))}
      </div>
      <div className="mt-1 border-t border-line pt-1">
        <Button
          variant="ghost"
          size="sm"
          className="w-full"
          onClick={() =>
            table.setColumnVisibility(
              defaultVisibility(columns, operator),
            )
          }
        >
          reset to defaults
        </Button>
      </div>
    </Dropdown>
  );
}
