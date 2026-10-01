/** Sidebar open/closed, persisted — and the media query that says
 * which KIND of sidebar is on screen.
 *
 * The two are separate on purpose. "Open" is one operator
 * preference; what open MEANS differs by width (an in-flow column
 * on a desktop, an overlay drawer on a phone), and only the second
 * depends on the viewport. Keeping them apart is what lets one
 * toggle button serve both without knowing which it is driving. */

const KEY = "sidebar";

/** Matches Tailwind's `md`. Hardcoded rather than read from the
 * stylesheet because the layout's own `md:` classes already encode
 * the same number — a mismatch between the two would show up as a
 * drawer that animates while sitting in the flow. */
export const DESKTOP_QUERY = "(min-width: 48rem)";

/** Open is the ABSENCE of a preference, not a stored "open": the
 * arena tree is the main way around, so a first visit that hides it
 * looks like an empty app. One less value that could later disagree
 * with the default. */
export function sidebarOpenFrom(stored: string | null): boolean {
  return stored !== "closed";
}

export function loadSidebarOpen(): boolean {
  return sidebarOpenFrom(localStorage.getItem(KEY));
}

export function saveSidebarOpen(open: boolean): void {
  if (open) localStorage.removeItem(KEY);
  else localStorage.setItem(KEY, "closed");
}
