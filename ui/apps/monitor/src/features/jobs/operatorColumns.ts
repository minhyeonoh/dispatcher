// Operator-defined columns: what the arena's `columns(job)` function
// returned, turned into table columns.
//
// A column is identified by the PAIR (source arena, key), not by the
// key alone. Two arenas can each define `solved` meaning different
// things — one a pass rate, one a threshold — and putting those under
// one header would be the same "one name, two computations" collision
// a readout registration refuses outright, except silent. Keyed by the
// pair they are two columns, and the label says which is which:
// `solved:appworld` and `solved:bench/v7`, key first so the metric is
// what you scan for.
//
// The source is the ARENA NODE where the function is registered, not
// its hash: an operator editing a function in place still means the
// same column, and keying on the text would reset their column
// choices every time they tweaked it.

import type { JobRow } from "../../live/fold";

export interface OperatorColumn {
  /** The arena node whose `columns` function produced it. */
  source: string;
  key: string;
  /** `key`, or `key:source` when that key exists under more than one
   * source among the rows on screen. The key leads because the key is
   * what you are looking for — a list of headers then groups by
   * metric rather than by arena. */
  label: string;
  /** Every value seen is a number, so the column can be right-aligned
   * and sorted numerically. */
  numeric: boolean;
}

export interface OperatorSpec {
  columns: OperatorColumn[];
  /** More than one `columns` function feeds these rows, so their keys
   * cannot be assumed comparable and the compact cell earns its
   * place as the default instead. */
  multiSource: boolean;
}

export function operatorColumnId(source: string, key: string): string {
  return `col:${source}:${key}`;
}

/** Which (source, key) pairs the rows on screen carry.
 *
 * Order is first-seen: a `columns` function returns a dict, whose key
 * order survives JSON, so the operator decides what sits leftmost by
 * the order they wrote the literal. Sorting it alphabetically here
 * would throw that away. */
export function collectOperatorColumns(rows: JobRow[]): OperatorSpec {
  const sources = new Set<string>();
  const order: { source: string; key: string }[] = [];
  const seen = new Set<string>();
  const keyToSources = new Map<string, Set<string>>();
  const nonNumeric = new Set<string>();

  for (const row of rows) {
    const source = row.columns_source_arena ?? "";
    if (!source) continue;
    sources.add(source);
    for (const [key, value] of Object.entries(row.columns ?? {})) {
      const id = operatorColumnId(source, key);
      if (!seen.has(id)) {
        seen.add(id);
        order.push({ source, key });
      }
      let owners = keyToSources.get(key);
      if (!owners) keyToSources.set(key, (owners = new Set()));
      owners.add(source);
      // A null is "not applicable here", which says nothing about the
      // column's type — only a real non-number does.
      if (value === null || value === undefined) continue;
      if (typeof value !== "number" && typeof value !== "boolean") {
        nonNumeric.add(id);
      }
    }
  }

  return {
    multiSource: sources.size > 1,
    columns: order.map(({ source, key }) => {
      const id = operatorColumnId(source, key);
      return {
        source,
        key,
        label:
          (keyToSources.get(key)?.size ?? 0) > 1 ? `${key}:${source}` : key,
        // Numeric until proven otherwise, so a column whose values
        // are all null so far does not jump sides once the first
        // number lands.
        numeric: !nonNumeric.has(id),
      };
    }),
  };
}

/** One value as the table shows it. Booleans read better as a word
 * than as 1/0, and a long float is noise past four places. */
export function formatColumnValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return "—";
    if (Number.isInteger(value)) return String(value);
    return value.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
  }
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

/** The compact one-cell form: every column this row has, in the
 * function's own order. */
export function summariseColumns(row: JobRow): string {
  const entries = Object.entries(row.columns ?? {});
  if (entries.length === 0) return "";
  return entries
    .map(([k, v]) => `${k} ${formatColumnValue(v)}`)
    .join(" · ");
}
