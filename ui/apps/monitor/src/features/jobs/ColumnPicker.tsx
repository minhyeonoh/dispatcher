import { Button, Checkbox, cn, Sheet } from "@lab/kit";
import { useQuery } from "@tanstack/react-query";
import type { Column, Table } from "@tanstack/react-table";
import { useMemo, useState } from "react";
import { api } from "../../api/client";
import type { JobRow } from "../../live/fold";
import {
  columnDescription,
  columnTitle,
  defaultVisibility,
  type ColumnMeta,
  type JobColumn,
} from "./columns";
import { operatorColumnId, type OperatorSpec } from "./operatorColumns";

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
  const [query, setQuery] = useState("");
  const all = table.getAllLeafColumns();
  const shown = all.filter((c) => c.getIsVisible()).length;
  const byId = useMemo(
    () => new Map(columns.map((c) => [String(c.id), c])),
    [columns],
  );
  const groups = sections(all, byId);

  // Operator descriptions come from the registration, not from every
  // job row: the text is identical for every job resolving to one
  // function. Cached — it changes only when someone re-registers.
  const registrations = useQuery({
    queryKey: ["readouts"],
    queryFn: () => api.readouts(),
    staleTime: 60_000,
  });
  const described = registrations.data?.column_descriptions ?? {};

  function describe(column: Column<JobRow, unknown>): string {
    const def = byId.get(column.id);
    const meta = def?.meta as ColumnMeta | undefined;
    if (meta?.group) {
      const own = operator.columns.find(
        (c) => operatorColumnId(c.source, c.key) === column.id,
      );
      return own ? (described[own.source]?.[own.key] ?? "") : "";
    }
    return def ? columnDescription(def) : "";
  }

  const hit = (column: Column<JobRow, unknown>) => {
    const q = query.trim().toLowerCase();
    if (!q) return true;
    const def = byId.get(column.id);
    const title = def ? columnTitle(def) : column.id;
    return (
      title.toLowerCase().includes(q) ||
      describe(column).toLowerCase().includes(q)
    );
  };
  return (
    <Sheet
      title="columns"
      trigger={
        <>
          <span>columns</span>
          <span className="text-xs text-fg-faint">
            {shown}/{all.length}
          </span>
        </>
      }
      headerAside={
        <span className="text-xs text-fg-faint">
          {shown} of {all.length} shown
        </span>
      }
      footer={
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
      }
    >
      <div className="px-1 pb-2">
        <input
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="name or meaning…"
          className={cn(
            "h-8 w-full rounded-control border border-strong",
            "bg-surface px-2 text-sm text-fg outline-none",
            "placeholder:text-fg-faint",
            "focus-visible:ring-2 focus-visible:ring-focus/60",
          )}
        />
      </div>
      {groups.map(([group, members], i) => {
        const visible = members.filter(hit);
        if (visible.length === 0) return null;
        return (
          <div key={group}>
            {/* A heading only earns its space once there is more than
                one section — with nothing operator-defined the list
                reads as one flat set, which is what it is. */}
            {groups.length > 1 && (
              <div
                className={cn(
                  "px-2 pb-1 text-xs font-medium tracking-wide",
                  "text-fg-faint uppercase",
                  i > 0 && "mt-3 border-t border-line pt-3",
                )}
              >
                {group}
              </div>
            )}
            {visible.map((column) => {
              const def = byId.get(column.id);
              const meta = def?.meta as ColumnMeta | undefined;
              const description = describe(column);
              return (
                <div key={column.id} className="py-0.5">
                  <Checkbox
                    checked={column.getIsVisible()}
                    disabled={!column.getCanHide()}
                    onChange={column.getToggleVisibilityHandler()}
                  >
                    <span className="font-medium">
                      {def ? columnTitle(def) : column.id}
                    </span>
                  </Checkbox>
                  {/* Indented to the label rather than the box: the
                      description belongs to the name, and the room to
                      say it in full is why this is a sheet and not a
                      dropdown. */}
                  <div className="pl-7 text-xs leading-snug text-fg-muted">
                    {description || (
                      <span className="text-warn">
                        {meta?.group
                          ? "no description — column_descriptions() " +
                            "does not mention this key"
                          : "no description"}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        );
      })}
    </Sheet>
  );
}
