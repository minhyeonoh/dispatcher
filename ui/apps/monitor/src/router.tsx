import {
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { useMemo } from "react";
import { AppShell } from "./features/layout/AppShell";
import { inSubtree } from "./features/arenas/tree";
import { JobPage } from "./features/jobs/JobPage";
import { JobsTable } from "./features/jobs/JobsTable";
import { useLive, } from "./live/store";

function useOrderedJobs() {
  const jobs = useLive((s) => s.jobs);
  const order = useLive((s) => s.order);
  return useMemo(
    () =>
      order.flatMap((id) => {
        const row = jobs[id];
        return row ? [row] : [];
      }),
    [jobs, order],
  );
}

function AllJobsPage() {
  return <JobsTable jobs={useOrderedJobs()} />;
}

function ArenaPage() {
  const { _splat = "" } = arenaRoute.useParams();
  const rows = useOrderedJobs().filter((j) =>
    inSubtree(j.arena ?? "", _splat),
  );
  return (
    <div className="flex flex-col gap-3">
      <h1 className="font-mono text-sm text-fg-muted">{_splat}</h1>
      <JobsTable jobs={rows} />
    </div>
  );
}

const rootRoute = createRootRoute({ component: AppShell });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: AllJobsPage,
});

const arenaRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/arenas/$",
  component: ArenaPage,
});

const jobRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs/$jobId",
  component: JobPage,
});

export const router = createRouter({
  routeTree: rootRoute.addChildren([indexRoute, arenaRoute, jobRoute]),
  basepath: "/ui",
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
