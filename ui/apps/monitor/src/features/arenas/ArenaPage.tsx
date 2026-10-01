import { Badge } from "@lab/kit";
import {
  useNavigate,
  useParams,
  useSearch,
} from "@tanstack/react-router";
import { inSubtree } from "./tree";
import {
  filterToSearch,
  toFilter,
  type JobFilter,
} from "../jobs/filter";
import { JobsTable } from "../jobs/JobsTable";
import { useOrderedJobs } from "../jobs/useJobs";

export function ArenaPage() {
  const { _splat = "" } = useParams({ from: "/arenas/$" });
  const filter = toFilter(useSearch({ from: "/arenas/$" }));
  const navigate = useNavigate({ from: "/arenas/$" });
  const setFilter = (next: JobFilter) =>
    void navigate({ search: filterToSearch(next), replace: true });
  const rows = useOrderedJobs().filter((j) =>
    inSubtree(j.arena ?? "", _splat),
  );
  const segments = _splat.split("/");
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <h1 className="min-w-0 font-mono text-sm break-all">
          {segments.map((s, i) => (
            <span key={i}>
              {i > 0 && <span className="text-fg-faint"> / </span>}
              <span
                className={
                  i === segments.length - 1 ? "text-fg" : "text-fg-muted"
                }
              >
                {s}
              </span>
            </span>
          ))}
        </h1>
        <Badge>{rows.length} jobs</Badge>
      </div>
      <p className="text-xs text-fg-faint">
        includes every job in this subtree
      </p>
      <JobsTable
        jobs={rows}
        filter={filter}
        onFilterChange={setFilter}
        tableId="arena-jobs"
      />
    </div>
  );
}
