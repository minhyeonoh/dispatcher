import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps } from "react";
import { cn } from "./cn";

// Fill and text colour only — no outline. The tinted surface is
// already the accent laid over the background, so an outline in the
// same hue just draws the shape twice, and at this size it reads as
// noise rather than definition.
const badgeVariants = cva(
  [
    "inline-flex items-center gap-1 rounded-full",
    "px-2 py-0.5 text-xs font-medium whitespace-nowrap",
  ],
  {
    variants: {
      tone: {
        neutral: "bg-sunken text-fg-muted",
        accent: "bg-accent-surface text-accent",
        ok: "bg-ok-surface text-ok",
        danger: "bg-danger-surface text-danger",
        warn: "bg-warn-surface text-warn",
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
