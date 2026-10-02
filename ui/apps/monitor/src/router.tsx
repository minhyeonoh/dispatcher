// URL space, mirroring the server's concepts. Pages live at the
// root; the JSON surface is /api/* (see api/client.ts), so a page
// path and its API path are siblings: /arenas/bench/v7 ↔
// /api/arenas/bench/v7.
//
//   /                                overview: what needs action
//   /jobs                            every job
//   /jobs/:jobKey                    job detail (id, or alias →
//                                    redirected to the id)
//   /jobs/:jobKey/instances/:id      one instance
//   /arenas/*path                    arena subtree
//   /hosts                           fleet
//   /settings                        the settings document
//   /design                          how this is put together

import {
  createRootRoute,
  createRoute,
  createRouter,
} from "@tanstack/react-router";
import { parseJobFilter } from "./features/jobs/filter";
import { ArenaPage } from "./features/arenas/ArenaPage";
import { DesignPage } from "./features/design/DesignPage";
import { HostsPage } from "./features/hosts/HostsPage";
import { InstancePage } from "./features/instances/InstancePage";
import { JobPage } from "./features/jobs/JobPage";
import { JobsPage } from "./features/jobs/JobsPage";
import { AppShell } from "./features/layout/AppShell";
import { OverviewPage } from "./features/overview/OverviewPage";
import { SettingsPage } from "./features/settings/SettingsPage";

const rootRoute = createRootRoute({ component: AppShell });

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: OverviewPage,
});

// Filters ride in the url so a filtered view is a link someone can
// send. `validateSearch` is also the sanitiser: a hand-edited or
// stale link falls back to defaults instead of erroring.
const jobsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs",
  validateSearch: parseJobFilter,
  component: JobsPage,
});

const jobRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs/$jobKey",
  component: JobPage,
});

const instanceRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs/$jobKey/instances/$instanceId",
  component: InstancePage,
});

const arenaRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/arenas/$",
  validateSearch: parseJobFilter,
  component: ArenaPage,
});

const hostsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/hosts",
  component: HostsPage,
});

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: SettingsPage,
});

const designRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/design",
  component: DesignPage,
});

export const router = createRouter({
  routeTree: rootRoute.addChildren([
    indexRoute,
    jobsRoute,
    jobRoute,
    instanceRoute,
    arenaRoute,
    hostsRoute,
    settingsRoute,
    designRoute,
  ]),
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
