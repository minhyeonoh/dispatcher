import { Button, Checkbox, Dropdown } from "@lab/kit";
import type { Table } from "@tanstack/react-table";
import type { JobRow } from "../../live/fold";
import { columnTitle, defaultVisibility, type JobColumn } from "./columns";

export function ColumnPicker({
  table,
  columns,
}: {
  table: Table<JobRow>;
  columns: JobColumn[];
}) {
  const all = table.getAllLeafColumns();
  const shown = all.filter((c) => c.getIsVisible()).length;
  const byId = new Map(columns.map((c) => [String(c.id), c]));
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
        {all.map((column) => {
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
      <div className="mt-1 border-t border-line pt-1">
        <Button
          variant="ghost"
          size="sm"
          className="w-full"
          onClick={() =>
            table.setColumnVisibility(defaultVisibility(columns))
          }
        >
          reset to defaults
        </Button>
      </div>
    </Dropdown>
  );
}
