import type { ComponentProps, ReactNode } from "react";
import { cn } from "./cn";

export type CheckboxProps = Omit<
  ComponentProps<"input">,
  "type" | "children"
> & {
  children?: ReactNode;
};

/** A labelled checkbox — the whole row is the label, so the click
 * target is the text too, not a 13px box. */
export function Checkbox({
  className,
  children,
  ...props
}: CheckboxProps) {
  return (
    <label
      className={cn(
        "flex cursor-pointer items-center gap-2 rounded-control",
        "px-2 py-1 text-sm select-none hover:bg-sunken",
        props.disabled && "cursor-not-allowed opacity-50",
        className,
      )}
    >
      <input
        type="checkbox"
        className={cn(
          "size-3.5 shrink-0 accent-accent outline-none",
          "focus-visible:ring-2 focus-visible:ring-focus/60",
        )}
        {...props}
      />
      {children}
    </label>
  );
}
