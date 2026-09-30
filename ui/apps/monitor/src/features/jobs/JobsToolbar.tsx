import { Badge, Button, cn } from "@lab/kit";
import {
  EMPTY_FILTER,
  isFilterActive,
  JOB_STATES,
  type JobFilter,
  type JobStateFilter,
} from "./filter";

const STATE_LABEL: Record<JobStateFilter, string> = {
  any: "any state",
  active: "active",
  paused: "paused",
  blocked: "blocked",
  "with-errors": "with errors",
  unresolved: "unresolved",
  archived: "archived",
};

export function JobsToolbar({
  filter,
  onChange,
  total,
  shown,
  children,
}: {
  filter: JobFilter;
  onChange: (next: JobFilter) => void;
  total: number;
  shown: number;
  /** Trailing controls (the column picker). */
  children?: React.ReactNode;
}) {
  const active = isFilterActive(filter);
  return (
    <div className="flex flex-wrap items-center gap-2">
      <input
        type="search"
        value={filter.q}
        onChange={(e) => onChange({ ...filter, q: e.target.value })}
        placeholder="label, alias, arena…"
        className={cn(
          "h-8 w-56 rounded-control border border-strong bg-raised",
          "px-2 text-sm text-fg placeholder:text-fg-faint outline-none",
          "focus-visible:ring-2 focus-visible:ring-focus/60",
        )}
      />
      <select
        value={filter.state}
        onChange={(e) =>
          onChange({
            ...filter,
            state: e.target.value as JobStateFilter,
          })
        }
        className={cn(
          "h-8 rounded-control border border-strong bg-raised px-2",
          "text-sm text-fg outline-none",
          "focus-visible:ring-2 focus-visible:ring-focus/60",
        )}
      >
        {JOB_STATES.map((state) => (
          <option key={state} value={state}>
            {STATE_LABEL[state]}
          </option>
        ))}
      </select>
      {active && (
        <>
          <Badge tone="accent">
            {shown} of {total}
          </Badge>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onChange(EMPTY_FILTER)}
          >
            clear
          </Button>
        </>
      )}
      {!active && (
        <span className="text-xs text-fg-faint">
          {total} job{total === 1 ? "" : "s"}
        </span>
      )}
      <div className="ml-auto">{children}</div>
    </div>
  );
}
