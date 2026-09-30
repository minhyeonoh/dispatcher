import { Badge } from "@lab/kit";
import { useLive } from "../../live/store";

export function ClusterBar() {
  const cluster = useLive((s) => s.cluster);
  const connected = useLive((s) => s.connected);
  const hosts = cluster?.settings.hosts ?? {};
  const perHost = cluster?.running_per_host ?? {};
  return (
    <div className="flex items-center gap-3 text-sm">
      <span
        title={connected ? "live" : "disconnected"}
        className={`size-2 rounded-full ${
          connected ? "bg-ok" : "bg-danger"
        }`}
      />
      <span className="font-semibold">dispatcher</span>
      {cluster && (
        <>
          <span className="text-fg-muted">{cluster.self_host}</span>
          <Badge tone="accent">
            running {cluster.running_total}/
            {cluster.settings.max_concurrent}
          </Badge>
          <div className="flex items-center gap-1.5">
            {Object.entries(hosts).map(([host, hs]) => (
              <Badge
                key={host}
                tone={hs.active ? "neutral" : "warn"}
                title={hs.active ? undefined : "inactive"}
              >
                {host} {perHost[host] ?? 0}/{hs.max_concurrent}
              </Badge>
            ))}
          </div>
        </>
      )}
    </div>
  );
}
