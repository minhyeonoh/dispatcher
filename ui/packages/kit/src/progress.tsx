import type { ComponentProps } from "react";
import { cn } from "./cn";

export interface Segment {
  /** share of the whole bar, any non-negative scale */
  value: number;
  tone: "ok" | "danger" | "accent" | "warn" | "muted";
}

const toneClass: Record<Segment["tone"], string> = {
  ok: "bg-ok",
  danger: "bg-danger",
  accent: "bg-accent",
  warn: "bg-warn",
  muted: "bg-strong",
};

/** A segmented progress bar (ok / err / running / pending …). */
export function SegmentBar({
  segments,
  className,
  ...props
}: { segments: Segment[] } & ComponentProps<"div">) {
  const total = segments.reduce((s, x) => s + Math.max(0, x.value), 0);
  return (
    <div
      className={cn(
        "flex h-1.5 w-full overflow-hidden rounded-full bg-sunken",
        className,
      )}
      {...props}
    >
      {total > 0 &&
        segments
          .filter((s) => s.value > 0)
          .map((s, i) => (
            <div
              key={i}
              className={toneClass[s.tone]}
              style={{ width: `${(100 * s.value) / total}%` }}
            />
          ))}
    </div>
  );
}
