import { Badge } from "@lab/kit";
import { Link } from "@tanstack/react-router";
import { useLive } from "../../live/store";

/** The header carries only what is true of the whole cluster. Which
 * host is how full belongs on /hosts — repeating it here scaled
 * with the fleet and pushed everything else off the bar.
 *
 * The one per-host fact that survives is a host being down, because
 * that changes what you do next and would otherwise need a page
 * visit to notice. It renders only when something IS down. */
export function ClusterBar() {
  const cluster = useLive((s) => s.cluster);
  const connected = useLive((s) => s.connected);
  const hosts = Object.entries(cluster?.settings.hosts ?? {});
  const down = hosts.filter(([, h]) => !h.active || !h.alive);
  return (
    <div className="flex items-center gap-3 text-sm">
      <span
        title={connected ? "live" : "disconnected"}
        className={`size-2 rounded-full ${
          connected ? "bg-ok" : "bg-danger"
        }`}
      />
      <span className="font-semibold tracking-tight">dispatcher</span>
      {cluster && (
        <>
          <span className="text-fg-muted">{cluster.self_host}</span>
          <Badge tone="accent">
            running {cluster.running_total}/
            {cluster.settings.max_concurrent}
          </Badge>
          {down.length > 0 && (
            <Link to="/hosts">
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
