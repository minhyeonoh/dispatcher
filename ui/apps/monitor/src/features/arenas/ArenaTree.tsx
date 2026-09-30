import { cn } from "@lab/kit";
import { Link, useParams } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import { useOrderedJobs } from "../jobs/useJobs";
import {
  isOpen,
  loadExpansion,
  reveal,
  saveExpansion,
  toggle,
  type Expansion,
} from "./expansion";
import { buildArenaTree, type ArenaNode } from "./tree";

/** A leaf carries a dot in the chevron's slot rather than empty
 * space: the gap read as a missing control, and the dot says
 * "nothing below this" instead of saying nothing at all. Same
 * viewBox as the chevron so the two align exactly. */
function LeafDot() {
  return (
    <svg
      viewBox="0 0 12 12"
      aria-hidden="true"
      className="size-3 shrink-0 text-fg-faint"
    >
      <circle cx="6" cy="6" r="1.75" fill="currentColor" />
    </svg>
  );
}

function Chevron({ open }: { open: boolean }) {
  return (
    <svg
      viewBox="0 0 12 12"
      aria-hidden="true"
      className={cn(
        "size-3 shrink-0 transition-transform",
        open && "rotate-90",
      )}
    >
      <path
        d="M4.5 2.5 L8 6 L4.5 9.5"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function Node({
  node,
  activePath,
  expansion,
  onToggle,
}: {
  node: ArenaNode;
  activePath: string;
  expansion: Expansion;
  onToggle: (path: string) => void;
}) {
  const active = activePath === node.path;
  const hasChildren = node.children.length > 0;
  const open = hasChildren && isOpen(expansion, node.path);
  return (
    <div>
      {/* The chevron is a sibling of the link, not a child: a
          button inside an anchor is invalid, and collapsing a
          branch should not navigate into it. */}
      <div
        className={cn(
          "flex items-center gap-0.5 rounded-control pr-1",
          "transition-colors hover:bg-sunken",
          active && "bg-accent-surface hover:bg-accent-surface",
        )}
      >
        {hasChildren ? (
          <button
            type="button"
            onClick={() => onToggle(node.path)}
            aria-expanded={open}
            aria-label={`${open ? "collapse" : "expand"} ${node.path}`}
            className={cn(
              "flex size-5 shrink-0 items-center justify-center",
              "rounded text-fg-faint outline-none",
              "hover:text-fg focus-visible:ring-2 focus-visible:ring-focus/60",
            )}
          >
            <Chevron open={open} />
          </button>
        ) : (
          <span className="flex size-5 shrink-0 items-center justify-center">
            <LeafDot />
          </span>
        )}
        <Link
          to="/arenas/$"
          params={{ _splat: node.path }}
          className={cn(
            "flex min-w-0 flex-1 items-center justify-between gap-2",
            "py-1.5 text-sm",
            active ? "text-accent" : "text-fg",
          )}
        >
          <span className="truncate">{node.name}</span>
          <span
            className={cn(
              "shrink-0 text-xs tabular-nums",
              active ? "text-accent" : "text-fg-faint",
            )}
          >
            {node.jobs}
          </span>
        </Link>
      </div>
      {open && (
        // The guide line IS the containment: it spans exactly the
        // children it owns, which is what indentation alone stops
        // conveying once the tree is more than two deep.
        <div className="ml-2.5 border-l border-line pl-1.5">
          {node.children.map((child) => (
            <Node
              key={child.path}
              node={child}
              activePath={activePath}
              expansion={expansion}
              onToggle={onToggle}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export function ArenaTree() {
  const jobs = useOrderedJobs();
  const params = useParams({ strict: false });
  const activePath = params._splat ?? "";
  const { roots, ungrouped } = useMemo(
    () => buildArenaTree(jobs),
    [jobs],
  );

  const [expansion, setExpansion] = useState<Expansion>(loadExpansion);

  // Arriving at a deep arena must reveal it — but only by OPENING
  // ancestors, never by re-closing anything, so a branch collapsed
  // after arriving stays collapsed.
  useEffect(() => {
    if (!activePath) return;
    setExpansion((prev) => reveal(prev, activePath));
  }, [activePath]);

  useEffect(() => saveExpansion(expansion), [expansion]);

  const onToggle = (path: string) =>
    setExpansion((prev) => toggle(prev, path));

  return (
    <nav className="flex flex-col">
      <div className="px-2 pb-2 text-xs font-medium tracking-wide text-fg-faint uppercase">
        arenas
      </div>
      {roots.length === 0 && (
        <div className="px-2 text-xs text-fg-faint">none</div>
      )}
      {roots.map((node) => (
        <Node
          key={node.path}
          node={node}
          activePath={activePath}
          expansion={expansion}
          onToggle={onToggle}
        />
      ))}
      {ungrouped > 0 && (
        <div className="mt-2 px-2 text-xs text-fg-faint">
          ungrouped · {ungrouped}
        </div>
      )}
    </nav>
  );
}
