import { JobsTable } from "./JobsTable";
import { useOrderedJobs } from "./useJobs";

export function JobsPage() {
  const jobs = useOrderedJobs();
  return (
    <div className="flex flex-col gap-3">
      <h1 className="text-lg font-semibold">jobs</h1>
      <JobsTable jobs={jobs} />
    </div>
  );
}
