import {
  Badge,
  Card,
  CardHeader,
  CardTitle,
  SegmentBar,
  Table,
  TD,
  TH,
  THead,
  TR,
} from "@lab/kit";
import { useLive } from "../../live/store";

export function HostsPage() {
  const cluster = useLive((s) => s.cluster);
  if (!cluster)
    return <div className="p-4 text-sm text-fg-faint">loading…</div>;
  const hosts = Object.entries(cluster.settings.hosts);
  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-2xl font-semibold tracking-tight">fleet</h1>
      <Card>
        <CardHeader>
          <CardTitle>hosts</CardTitle>
          <span className="text-xs text-fg-faint">
            global {cluster.running_total}/
            {cluster.settings.max_concurrent}
          </span>
        </CardHeader>
        <Table>
          <THead>
            <TR>
              <TH>host</TH>
              <TH>state</TH>
              <TH className="text-right">running</TH>
              <TH className="text-right">cap</TH>
              <TH className="w-48">utilisation</TH>
            </TR>
          </THead>
          <tbody>
            {hosts.map(([host, hs]) => {
              const running = cluster.running_per_host[host] ?? 0;
              return (
                <TR key={host}>
                  <TD className="font-medium">{host}</TD>
                  <TD className="flex gap-1.5">
                    {!hs.alive && <Badge tone="danger">dead</Badge>}
                    {!hs.active && <Badge tone="warn">inactive</Badge>}
                    {hs.alive && hs.active && (
                      <Badge tone="ok">ready</Badge>
                    )}
                  </TD>
                  <TD className="text-right tabular-nums">{running}</TD>
                  <TD className="text-right tabular-nums">
                    {hs.max_concurrent}
                  </TD>
                  <TD>
                    <SegmentBar
                      segments={[
                        { value: running, tone: "accent" },
                        {
                          value: Math.max(0, hs.max_concurrent - running),
                          tone: "muted",
                        },
                      ]}
                    />
                  </TD>
                </TR>
              );
            })}
          </tbody>
        </Table>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>pools</CardTitle>
        </CardHeader>
        <Table>
          <THead>
            <TR>
              <TH>pool</TH>
              <TH className="text-right">running</TH>
              <TH className="text-right">cap</TH>
            </TR>
          </THead>
          <tbody>
            {Object.entries(cluster.running_per_pool ?? {}).map(
              ([pool, n]) => {
                const cap = cluster.settings.pool_caps?.[pool];
                return (
                  <TR key={pool}>
                    <TD>{pool}</TD>
                    <TD className="text-right tabular-nums">{n}</TD>
                    <TD className="text-right tabular-nums">
                      {cap === undefined ? (
                        <span className="text-fg-faint">unbounded</span>
                      ) : (
                        cap
                      )}
                    </TD>
                  </TR>
                );
              },
            )}
          </tbody>
        </Table>
      </Card>
    </div>
  );
}
