// Table preferences (which columns, what sort) persisted in
// localStorage. Pure functions around the storage so the merge rules
// are testable without a DOM.
//
// The merge matters: stored prefs are from an older build, so a
// column added since must appear (at ITS default, not hidden) and a
// column removed since must not resurrect as a phantom key.

import type { SortingState, VisibilityState } from "@tanstack/react-table";

export interface TablePrefs {
  visibility: VisibilityState;
  sorting: SortingState;
}

export function mergeVisibility(
  stored: unknown,
  defaults: VisibilityState,
): VisibilityState {
  if (typeof stored !== "object" || stored === null) return defaults;
  const out: VisibilityState = {};
  for (const [id, fallback] of Object.entries(defaults)) {
    const saved = (stored as Record<string, unknown>)[id];
    out[id] = typeof saved === "boolean" ? saved : fallback;
  }
  return out;
}

export function parseSorting(stored: unknown): SortingState {
  if (!Array.isArray(stored)) return [];
  return stored.flatMap((entry) => {
    if (typeof entry !== "object" || entry === null) return [];
    const { id, desc } = entry as Record<string, unknown>;
    if (typeof id !== "string") return [];
    return [{ id, desc: desc === true }];
  });
}

/** Drop a sort that names a column this build no longer has, or the
 * table would sort by nothing and look broken. */
export function pruneSorting(
  sorting: SortingState,
  knownIds: Iterable<string>,
): SortingState {
  const known = new Set(knownIds);
  return sorting.filter((s) => known.has(s.id));
}

function key(tableId: string): string {
  return `table-prefs:${tableId}`;
}

export function loadPrefs(
  tableId: string,
  defaults: VisibilityState,
): TablePrefs {
  let raw: unknown = null;
  try {
    const text = localStorage.getItem(key(tableId));
    raw = text ? JSON.parse(text) : null;
  } catch {
    // Unreadable or corrupt storage is not worth failing a page over.
  }
  const source =
    typeof raw === "object" && raw !== null
      ? (raw as Record<string, unknown>)
      : {};
  return {
    visibility: mergeVisibility(source.visibility, defaults),
    sorting: pruneSorting(
      parseSorting(source.sorting),
      Object.keys(defaults),
    ),
  };
}

export function savePrefs(tableId: string, prefs: TablePrefs): void {
  try {
    localStorage.setItem(key(tableId), JSON.stringify(prefs));
  } catch {
    // Private mode / quota — the table still works, it just forgets.
  }
}
