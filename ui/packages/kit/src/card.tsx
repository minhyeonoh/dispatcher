import type { ComponentProps } from "react";
import { cn } from "./cn";

export function Card({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "rounded-panel border border-line bg-raised shadow-raised",
        className,
      )}
      {...props}
    />
  );
}

export function CardHeader({
  className,
  ...props
}: ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "flex items-center justify-between gap-2 border-b border-line px-4 py-2.5",
        className,
      )}
      {...props}
    />
  );
}

export function CardTitle({ className, ...props }: ComponentProps<"h2">) {
  return (
    <h2
      className={cn("text-sm font-semibold text-fg", className)}
      {...props}
    />
  );
}

export function CardBody({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("px-4 py-3", className)} {...props} />;
}
