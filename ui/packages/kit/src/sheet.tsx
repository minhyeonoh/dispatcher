import {
  useEffect,
  useId,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { cn } from "./cn";

export interface SheetProps {
  /** Rendered inside the trigger button. */
  trigger: ReactNode;
  children: ReactNode;
  /** Shown in the sheet's own header, beside the close button. */
  title: ReactNode;
  /** Trailing header content — a count, a status. */
  headerAside?: ReactNode;
  /** Pinned to the bottom, outside the scrolling body. */
  footer?: ReactNode;
  className?: string;
  panelClassName?: string;
}

/** A panel that slides in from the right and stays until dismissed.
 *
 * A dropdown is the wrong shape once a list needs explaining: it is
 * sized by its trigger, it closes on the first outside click, and
 * anything longer than a few lines turns into a scrolling stub. A
 * sheet has the full height, does not cover the table it is about, and
 * survives the clicks you make while reading it — which is what a
 * list you are comparing against the page behind it needs.
 *
 * Dismissal is deliberately narrower than a dropdown's: the scrim and
 * Escape, not any outside click. The point is to leave it open while
 * working in the page, so a stray click must not close it. Escape
 * returns focus to the trigger rather than dropping the keyboard at
 * the top of the document. */
export function Sheet({
  trigger,
  children,
  title,
  headerAside,
  footer,
  className,
  panelClassName,
}: SheetProps) {
  const [open, setOpen] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const panelId = useId();

  useEffect(() => {
    if (!open) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key !== "Escape") return;
      setOpen(false);
      triggerRef.current?.focus();
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [open]);

  return (
    <div className={cn("contents", className)}>
      <button
        ref={triggerRef}
        type="button"
        aria-expanded={open}
        aria-haspopup="dialog"
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
        <>
          <button
            type="button"
            aria-hidden="true"
            tabIndex={-1}
            onClick={() => setOpen(false)}
            className="fixed inset-0 z-40 bg-overlay"
          />
          <div
            id={panelId}
            role="dialog"
            aria-modal="false"
            aria-label={typeof title === "string" ? title : undefined}
            className={cn(
              "fixed inset-y-0 right-0 z-50 flex flex-col",
              "border-l border-line bg-raised shadow-floating",
              // Half the viewport where there is something worth
              // leaving visible, all of it where there is not. The
              // breakpoint is `md`, the same one the app shell uses
              // for "this is a desktop" — below it, half a phone is
              // too narrow to read a sentence in, and the thing
              // behind would be unusable anyway.
              "w-full md:w-1/2",
              panelClassName,
            )}
          >
            <div
              className={cn(
                "flex shrink-0 items-center gap-2 border-b border-line",
                "px-3 py-2.5",
              )}
            >
              <span className="text-sm font-medium">{title}</span>
              {headerAside}
              <button
                type="button"
                onClick={() => {
                  setOpen(false);
                  triggerRef.current?.focus();
                }}
                aria-label="close"
                className={cn(
                  "ml-auto flex size-6 shrink-0 items-center",
                  "justify-center rounded-control text-fg-muted",
                  "outline-none hover:bg-sunken hover:text-fg",
                  "focus-visible:ring-2 focus-visible:ring-focus/60",
                )}
              >
                <svg
                  viewBox="0 0 12 12"
                  aria-hidden="true"
                  className="size-3"
                >
                  <path
                    d="M3 3l6 6M9 3l-6 6"
                    fill="none"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                  />
                </svg>
              </button>
            </div>
            <div className="min-h-0 flex-1 overflow-y-auto p-1">
              {children}
            </div>
            {footer && (
              <div className="shrink-0 border-t border-line p-1">
                {footer}
              </div>
            )}
          </div>
        </>
      )}
    </div>
  );
}
