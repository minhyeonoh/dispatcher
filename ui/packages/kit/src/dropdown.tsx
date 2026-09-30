import {
  useEffect,
  useId,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { cn } from "./cn";

export interface DropdownProps {
  /** Rendered inside the trigger button. */
  trigger: ReactNode;
  children: ReactNode;
  className?: string;
  panelClassName?: string;
  /** Which side of the trigger the panel hangs from. */
  align?: "start" | "end";
}

/** Click-to-open panel anchored to a trigger. Closes on outside
 * pointerdown and on Escape; Escape returns focus to the trigger so
 * keyboard users are not dropped at the top of the document. */
export function Dropdown({
  trigger,
  children,
  className,
  panelClassName,
  align = "end",
}: DropdownProps) {
  const [open, setOpen] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const panelId = useId();

  useEffect(() => {
    if (!open) return;
    const onPointerDown = (e: PointerEvent) => {
      if (!root.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      setOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener("pointerdown", onPointerDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("pointerdown", onPointerDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [open]);

  return (
    <div ref={root} className={cn("relative", className)}>
      <button
        ref={triggerRef}
        type="button"
        aria-expanded={open}
        aria-haspopup="true"
        aria-controls={open ? panelId : undefined}
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "inline-flex items-center gap-1.5 rounded-control border",
          "border-strong bg-raised px-2.5 py-1 text-sm text-fg",
          "transition-colors hover:bg-sunken outline-none",
          "focus-visible:ring-2 focus-visible:ring-focus/60",
        )}
      >
        {trigger}
      </button>
      {open && (
        <div
          id={panelId}
          className={cn(
            "absolute z-20 mt-1 min-w-48 rounded-panel border",
            "border-line bg-raised p-1 shadow-floating",
            align === "end" ? "right-0" : "left-0",
            panelClassName,
          )}
        >
          {children}
        </div>
      )}
    </div>
  );
}
