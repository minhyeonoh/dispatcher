import { Button, cn } from "@lab/kit";
import { Link, Outlet, useLocation } from "@tanstack/react-router";
import { useEffect, useState, useSyncExternalStore } from "react";
import { ArenaTree } from "../arenas/ArenaTree";
import { ClusterBar } from "../cluster/ClusterBar";
import { DESKTOP_QUERY, loadSidebarOpen, saveSidebarOpen } from "./sidebar";

type Theme = "auto" | "light" | "dark";

let themeListeners: (() => void)[] = [];

function currentTheme(): Theme {
  const t = localStorage.getItem("theme");
  return t === "light" || t === "dark" ? t : "auto";
}

function cycleTheme() {
  const next: Record<Theme, Theme> = {
    auto: "light",
    light: "dark",
    dark: "auto",
  };
  const theme = next[currentTheme()];
  if (theme === "auto") {
    localStorage.removeItem("theme");
    delete document.documentElement.dataset.theme;
  } else {
    localStorage.setItem("theme", theme);
    document.documentElement.dataset.theme = theme;
  }
  themeListeners.forEach((fn) => fn());
}

function ThemeToggle() {
  const theme = useSyncExternalStore((fn) => {
    themeListeners.push(fn);
    return () => {
      themeListeners = themeListeners.filter((x) => x !== fn);
    };
  }, currentTheme);
  const icon = { auto: "◐", light: "☀", dark: "☾" }[theme];
  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={cycleTheme}
      title={`theme: ${theme}`}
      aria-label={`theme: ${theme}`}
    >
      {icon}
      {/* The word is the affordance on a desktop and noise on a
          phone, where the icon plus a title is enough. */}
      <span className="hidden sm:inline">{theme}</span>
    </Button>
  );
}

const NAV = [
  { to: "/", label: "overview", exact: true },
  { to: "/jobs", label: "jobs", exact: false },
  { to: "/hosts", label: "fleet", exact: false },
  { to: "/settings", label: "settings", exact: false },
] as const;

function Nav({ stacked = false }: { stacked?: boolean }) {
  return (
    <nav
      className={cn(
        stacked ? "flex flex-col gap-0.5" : "flex items-center gap-1",
      )}
    >
      {NAV.map(({ to, label, exact }) => (
        <Link
          key={to}
          to={to}
          className={cn(
            "rounded-control px-3 py-1.5 text-sm font-medium",
            "text-fg-muted transition-colors",
            "hover:bg-sunken hover:text-fg",
          )}
          activeProps={{ className: "bg-accent-surface text-accent" }}
          activeOptions={{ exact }}
        >
          {label}
        </Link>
      ))}
    </nav>
  );
}

/** Three stacked lines when open, the same lines when closed — the
 * icon does not try to say which way the panel will move, because
 * on a phone it slides and on a desktop it collapses. The label
 * carries the state for anyone who needs it stated. */
function SidebarToggle({
  open,
  onClick,
}: {
  open: boolean;
  onClick: () => void;
}) {
  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={onClick}
      aria-expanded={open}
      aria-controls="sidebar"
      aria-label={`${open ? "hide" : "show"} arenas`}
      title={`${open ? "hide" : "show"} arenas`}
      className="shrink-0 px-2"
    >
      <svg viewBox="0 0 16 16" aria-hidden="true" className="size-4">
        <path
          d="M2.5 4h11M2.5 8h11M2.5 12h11"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.5"
          strokeLinecap="round"
        />
      </svg>
    </Button>
  );
}

export function AppShell() {
  const [open, setOpen] = useState(loadSidebarOpen);
  const isDesktop = useSyncExternalStore(
    (fn) => {
      const mq = window.matchMedia(DESKTOP_QUERY);
      mq.addEventListener("change", fn);
      return () => mq.removeEventListener("change", fn);
    },
    () => window.matchMedia(DESKTOP_QUERY).matches,
    () => true,
  );
  const { pathname } = useLocation();

  // Only the desktop preference is remembered. A drawer left open
  // from a phone session must not come back as a collapsed column on
  // a laptop — they are the same flag but not the same intent.
  useEffect(() => {
    if (isDesktop) saveSidebarOpen(open);
  }, [open, isDesktop]);

  // Navigating is the point of the drawer, so following a link
  // closes it. On a desktop the column stays where the operator put
  // it.
  useEffect(() => {
    if (!isDesktop) setOpen(false);
  }, [pathname, isDesktop]);

  // Escape closes the overlay — it is modal over the content, and
  // anything modal needs a keyboard way out.
  useEffect(() => {
    if (isDesktop || !open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isDesktop, open]);

  return (
    <div className="flex h-dvh flex-col">
      <header
        className={cn(
          "flex h-14 shrink-0 items-center gap-2",
          "border-b border-line px-2 sm:px-5",
        )}
      >
        <SidebarToggle open={open} onClick={() => setOpen((v) => !v)} />
        <ClusterBar />
        <div className="ml-auto flex shrink-0 items-center gap-1">
          {/* On a phone the four links go into the drawer instead:
              four of them plus the cluster facts do not fit one row
              at 375px, and navigation is not the part to drop. */}
          <div className="hidden md:block">
            <Nav />
          </div>
          <ThemeToggle />
        </div>
      </header>
      <div className="relative flex min-h-0 flex-1">
        {/* Scrim, phone only: the drawer floats over the content, so
            there has to be somewhere to click that means "put it
            back". */}
        {open && !isDesktop && (
          <button
            type="button"
            aria-hidden="true"
            tabIndex={-1}
            onClick={() => setOpen(false)}
            className="fixed inset-0 top-14 z-30 bg-overlay md:hidden"
          />
        )}
        <aside
          id="sidebar"
          // Closed means unreachable, not merely invisible: a
          // collapsed column still renders its links, and tabbing
          // into something off-screen is worse than not having it.
          inert={!open}
          className={cn(
            // Phone: an overlay below the header that slides in.
            "fixed inset-y-0 top-14 left-0 z-40 w-64",
            "overflow-x-hidden overflow-y-auto",
            "border-r border-line bg-sidebar",
            "transition-transform duration-200 ease-out",
            open ? "translate-x-0" : "-translate-x-full",
            // Desktop: in the flow, where the width is what moves.
            "md:static md:z-auto md:translate-x-0",
            "md:transition-[width,padding] md:duration-200",
            open ? "w-64 px-3 py-4 md:w-60" : "md:w-0 md:border-r-0",
            !open && "px-3 py-4 md:px-0",
          )}
        >
          <div className="md:hidden">
            <Nav stacked />
            <div className="my-3 border-t border-line" />
          </div>
          <ArenaTree />
        </aside>
        <main className="min-w-0 flex-1 overflow-y-auto p-4 sm:p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
