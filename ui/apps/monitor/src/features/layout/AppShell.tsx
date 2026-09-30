import { Button } from "@lab/kit";
import { Outlet } from "@tanstack/react-router";
import { useSyncExternalStore } from "react";
import { ArenaTree } from "../arenas/ArenaTree";
import { ClusterBar } from "../cluster/ClusterBar";

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
  const theme = useSyncExternalStore(
    (fn) => {
      themeListeners.push(fn);
      return () => {
        themeListeners = themeListeners.filter((x) => x !== fn);
      };
    },
    currentTheme,
  );
  const icon = { auto: "◐", light: "☀", dark: "☾" }[theme];
  return (
    <Button variant="ghost" size="sm" onClick={cycleTheme}>
      {icon} {theme}
    </Button>
  );
}

export function AppShell() {
  return (
    <div className="flex h-screen flex-col">
      <header className="flex items-center justify-between border-b border-line bg-raised px-4 py-2">
        <ClusterBar />
        <ThemeToggle />
      </header>
      <div className="flex min-h-0 flex-1">
        <aside className="w-56 shrink-0 overflow-y-auto border-r border-line p-2">
          <ArenaTree />
        </aside>
        <main className="min-w-0 flex-1 overflow-y-auto p-4">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
