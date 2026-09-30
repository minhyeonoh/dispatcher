import type { ComponentProps } from "react";
import { cn } from "./cn";

export function Table({ className, ...props }: ComponentProps<"table">) {
  return (
    <table
      className={cn("w-full border-collapse text-sm", className)}
      {...props}
    />
  );
}

export function THead({ className, ...props }: ComponentProps<"thead">) {
  return (
    <thead
      className={cn(
        "text-left text-xs font-medium text-fg-faint uppercase tracking-wide",
        className,
      )}
      {...props}
    />
  );
}

export function TR({ className, ...props }: ComponentProps<"tr">) {
  return (
    <tr
      className={cn("border-b border-line last:border-0", className)}
      {...props}
    />
  );
}

export function TH({ className, ...props }: ComponentProps<"th">) {
  return (
    <th className={cn("px-3 py-2 font-medium", className)} {...props} />
  );
}

export function TD({ className, ...props }: ComponentProps<"td">) {
  return <td className={cn("px-3 py-2", className)} {...props} />;
}
