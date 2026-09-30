import { Card, CardBody, CardHeader, CardTitle } from "@lab/kit";
import { useLive } from "../../live/store";

/** Read-only for now: the live Settings document, exactly as
 * PATCH /api/settings edits it. Editing lands with the v1 action
 * pass. */
export function SettingsPage() {
  const cluster = useLive((s) => s.cluster);
  if (!cluster)
    return <div className="p-4 text-sm text-fg-faint">loading…</div>;
  const { hosts, ...rest } = cluster.settings;
  return (
    <div className="flex flex-col gap-4">
      <h1 className="text-lg font-semibold">settings</h1>
      <p className="text-sm text-fg-muted">
        The operator-tunable document. Every change persists, so it
        survives restarts; host caps live under{" "}
        <span className="font-mono text-xs">hosts</span> (see fleet).
      </p>
      <Card>
        <CardHeader>
          <CardTitle>document</CardTitle>
          <span className="font-mono text-xs text-fg-faint">
            PATCH /api/settings
          </span>
        </CardHeader>
        <CardBody>
          <pre className="overflow-x-auto font-mono text-xs leading-relaxed">
            {JSON.stringify(rest, null, 2)}
          </pre>
        </CardBody>
      </Card>
    </div>
  );
}
