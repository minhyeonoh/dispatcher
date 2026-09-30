import { cn } from "@lab/kit";
import { Link, useParams } from "@tanstack/react-router";
import { useMemo } from "react";
import { useLive } from "../../live/store";
import { buildArenaTree, type ArenaNode } from "./tree";

function Node({ node, depth }: { node: ArenaNode; depth: number }) {
  const params = useParams({ strict: false });
  const active = params._splat === node.path;
  return (
    <>
      <Link
        to="/arenas/$"
        params={{ _splat: node.path }}
        className={cn(
          "flex items-center justify-between rounded-control px-2 py-1 text-sm",
          "hover:bg-sunken",
          active && "bg-accent-surface text-accent",
        )}
        style={{ paddingLeft: `${8 + depth * 14}px` }}
      >
        <span className="truncate">{node.name}</span>
        <span className="text-xs text-fg-faint">{node.jobs}</span>
      </Link>
      {node.children.map((child) => (
        <Node key={child.path} node={child} depth={depth + 1} />
      ))}
    </>
  );
}

export function ArenaTree() {
  const jobs = useLive((s) => s.jobs);
  const order = useLive((s) => s.order);
  const { roots, ungrouped } = useMemo(
    () =>
      buildArenaTree(
        order.flatMap((id) => {
          const row = jobs[id];
          return row ? [row] : [];
        }),
      ),
    [jobs, order],
  );
  return (
    <nav className="flex flex-col gap-0.5">
      <Link
        to="/"
        className="rounded-control px-2 py-1 text-sm font-medium hover:bg-sunken"
        activeProps={{ className: "bg-accent-surface text-accent" }}
        activeOptions={{ exact: true }}
      >
        all jobs
      </Link>
      {roots.map((node) => (
        <Node key={node.path} node={node} depth={0} />
      ))}
      {ungrouped > 0 && (
        <div className="px-2 py-1 text-xs text-fg-faint">
          ungrouped · {ungrouped}
        </div>
      )}
    </nav>
  );
}
