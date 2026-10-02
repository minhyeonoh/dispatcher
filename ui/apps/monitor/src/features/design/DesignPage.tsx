// A page for writing the design down, served next to the thing it
// describes. Empty on purpose for now — the point of landing it blank
// is that the next design discussion has somewhere to go that is not a
// commit message or a docstring.
//
// Why here rather than a markdown file in the repo: the questions that
// started this ("is tick_interval the dispatch interval?", "what IS the
// dispatch interval?") are answered by reading code AND watching the
// running system, and this page sits beside the fleet and the job rows
// that show the behaviour. A doc that lives where the evidence is gets
// corrected; one in a docs/ directory goes stale quietly.

export function DesignPage() {
  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-2xl font-semibold tracking-tight">design</h1>
      <p className="max-w-prose text-sm text-fg-muted">
        Notes on how the dispatcher is put together, and on the parts
        that are the way they are by accident rather than by decision.
      </p>
      <p className="max-w-prose text-sm text-fg-faint">
        Empty for now.
      </p>
    </div>
  );
}
