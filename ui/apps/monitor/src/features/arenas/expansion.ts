// Which tree branches are open.
//
// Only EXPLICIT choices are stored: a node the operator never
// touched has no entry, so changing the default later changes what
// they see instead of being overridden by a stale `true` written
// years ago. Same reasoning as the table's column prefs.

const KEY = "arena-tree:expanded";

export type Expansion = Record<string, boolean>;

/** Every strict ancestor path of a node, outermost first.
 * `a/b/c` → `["a", "a/b"]`. */
export function ancestorsOf(path: string): string[] {
  const parts = path.split("/").filter(Boolean);
  return parts.slice(0, -1).map((_, i) => parts.slice(0, i + 1).join("/"));
}

/** Branches start open: a collapsed-by-default tree hides the very
 * thing the sidebar is for. */
export function isOpen(expansion: Expansion, path: string): boolean {
  return expansion[path] ?? true;
}

export function toggle(expansion: Expansion, path: string): Expansion {
  return { ...expansion, [path]: !isOpen(expansion, path) };
}

/** Reveal a path by opening everything above it. Returns the SAME
 * object when nothing had to change, so it can be fed straight to a
 * setState without causing a render loop. */
export function reveal(
  expansion: Expansion,
  path: string,
): Expansion {
  const closed = ancestorsOf(path).filter(
    (p) => !isOpen(expansion, p),
  );
  if (closed.length === 0) return expansion;
  const next = { ...expansion };
  for (const p of closed) next[p] = true;
  return next;
}

export function loadExpansion(): Expansion {
  try {
    const raw = localStorage.getItem(KEY);
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    if (typeof parsed !== "object" || parsed === null) return {};
    const out: Expansion = {};
    for (const [k, v] of Object.entries(parsed)) {
      if (typeof v === "boolean") out[k] = v;
    }
    return out;
  } catch {
    return {};
  }
}

export function saveExpansion(expansion: Expansion): void {
  try {
    // Only the collapses are worth keeping — an entry saying "open"
    // matches the default and would just pin it against a future
    // change of mind about defaults.
    const closed = Object.fromEntries(
      Object.entries(expansion).filter(([, open]) => !open),
    );
    localStorage.setItem(KEY, JSON.stringify(closed));
  } catch {
    // Private mode / quota: the tree still works, it just forgets.
  }
}
