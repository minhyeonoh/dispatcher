import { Badge } from "@lab/kit";
import { Link } from "@tanstack/react-router";
import { useLive } from "../../live/store";

/** The header carries only what is true of the whole cluster. Which
 * host is how full belongs on /hosts — repeating it here scaled
 * with the fleet and pushed everything else off the bar.
 *
 * The one per-host fact that survives is a host being down, because
 * that changes what you do next and would otherwise need a page
 * visit to notice. It renders only when something IS down.
 *
 * Narrow screens drop by the same rule, cheapest first: the host
 * name and the running count are context a page can give you, the
 * live dot and a host being down are things you need to see without
 * asking. Nothing here shrinks to fit — it has room or it is gone. */
export function ClusterBar() {
  const cluster = useLive((s) => s.cluster);
  const connected = useLive((s) => s.connected);
  const hosts = Object.entries(cluster?.settings.hosts ?? {});
  const down = hosts.filter(([, h]) => !h.active || !h.alive);
  return (
    <div className="flex min-w-0 items-center gap-2 text-sm sm:gap-3">
      <span
        title={connected ? "live" : "disconnected"}
        className={`size-2 shrink-0 rounded-full ${
          connected ? "bg-ok" : "bg-danger"
        }`}
      />
      <span className="truncate font-semibold tracking-tight">
        dispatcher
      </span>
      {cluster && (
        <>
          <span className="hidden text-fg-muted lg:inline">
            {cluster.self_host}
          </span>
          <Badge
            tone="accent"
            className="hidden shrink-0 sm:inline-flex"
          >
            running {cluster.running_total}/
            {cluster.settings.max_concurrent}
          </Badge>
          {down.length > 0 && (
            <Link to="/hosts" className="shrink-0">
              <Badge
                tone="warn"
                title={down
                  .map(
                    ([name, h]) =>
                      `${name}: ${!h.alive ? "dead" : "inactive"}`,
                  )
                  .join(", ")}
              >
                {down.length} host{down.length === 1 ? "" : "s"} down
              </Badge>
            </Link>
          )}
        </>
      )}
    </div>
  );
}
