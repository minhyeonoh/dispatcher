import { useNavigate, useSearch } from "@tanstack/react-router";
import { filterToSearch, toFilter, type JobFilter } from "./filter";
import { JobsTable } from "./JobsTable";
import { useOrderedJobs } from "./useJobs";

export function JobsPage() {
  const jobs = useOrderedJobs();
  const filter = toFilter(useSearch({ from: "/jobs" }));
  const navigate = useNavigate({ from: "/jobs" });
  const setFilter = (next: JobFilter) =>
    void navigate({ search: filterToSearch(next), replace: true });
  return (
    <div className="flex flex-col gap-3">
      <h1 className="text-lg font-semibold">jobs</h1>
      <JobsTable
        jobs={jobs}
        filter={filter}
        onFilterChange={setFilter}
      />
    </div>
  );
}
