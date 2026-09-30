import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps } from "react";
import { cn } from "./cn";

const badgeVariants = cva(
  [
    "inline-flex items-center gap-1 rounded-full border",
    "px-2 py-px text-xs font-medium whitespace-nowrap",
  ],
  {
    variants: {
      tone: {
        neutral: "border-line bg-sunken text-fg-muted",
        accent: "border-accent/30 bg-accent-surface text-accent",
        ok: "border-ok/30 bg-ok-surface text-ok",
        danger: "border-danger/30 bg-danger-surface text-danger",
        warn: "border-warn/40 bg-warn-surface text-warn",
      },
    },
    defaultVariants: { tone: "neutral" },
  },
);

export type BadgeProps = ComponentProps<"span"> &
  VariantProps<typeof badgeVariants>;

export function Badge({ className, tone, ...props }: BadgeProps) {
  return (
    <span className={cn(badgeVariants({ tone }), className)} {...props} />
  );
}
