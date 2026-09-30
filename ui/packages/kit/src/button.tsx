import { cva, type VariantProps } from "class-variance-authority";
import type { ComponentProps } from "react";
import { cn } from "./cn";

const buttonVariants = cva(
  [
    "inline-flex items-center justify-center gap-1.5 whitespace-nowrap",
    "rounded-control text-sm font-medium select-none",
    "transition-colors outline-none",
    "focus-visible:ring-2 focus-visible:ring-focus/60",
    "disabled:pointer-events-none disabled:opacity-50",
  ],
  {
    variants: {
      variant: {
        primary: "bg-accent text-accent-fg hover:bg-accent-hover",
        outline:
          "border border-strong bg-raised text-fg hover:bg-sunken",
        ghost: "text-fg-muted hover:bg-sunken hover:text-fg",
        danger:
          "border border-danger/40 bg-danger-surface text-danger hover:border-danger",
      },
      size: {
        sm: "h-7 px-2.5 text-xs",
        md: "h-8 px-3",
      },
    },
    defaultVariants: { variant: "outline", size: "md" },
  },
);

export type ButtonProps = ComponentProps<"button"> &
  VariantProps<typeof buttonVariants>;

export function Button({
  className,
  variant,
  size,
  type = "button",
  ...props
}: ButtonProps) {
  return (
    <button
      type={type}
      className={cn(buttonVariants({ variant, size }), className)}
      {...props}
    />
  );
}
