# ui/

pnpm workspace: the dispatcher monitor app plus the design-system
packages it consumes.

```
packages/tokens   @lab/tokens — design tokens as pure CSS custom
                  properties. Framework-agnostic: no React, no
                  Tailwind; any site (Astro included) imports
                  base.css and gets the theme.
packages/kit      @lab/kit — React components + the Tailwind
                  binding of the tokens. shadcn-compatible var
                  names, so `shadcn add` drops in themed.
apps/monitor      the dispatcher monitor SPA (lives here, not in
                  its own repo, because it must move in the same
                  commit as the server's wire.py — its TS types
                  are generated from the server's OpenAPI).
```

## Extraction contract

`@lab/tokens` and `@lab/kit` are destined for their own repo once
a second consumer exists. Until then these rules keep extraction
a `git mv`:

- dependency direction is one-way: apps → kit → tokens. A package
  never imports from an app, and tokens never imports anything.
- packages carry no dispatcher domain knowledge. A component that
  knows what a "job" is belongs in the app.
- packages typecheck standalone (`pnpm -r typecheck`).

## Theme system

Two token layers in `@lab/tokens`:

- **primitive** (`--gray-6`, `--radius-2`, …) — raw material.
  Components must never reference these.
- **semantic** (`--surface`, `--fg-muted`, `--accent`, …) — the
  only layer components and Tailwind utilities see. A THEME is a
  set of values for this layer.

Colors are defined once with `light-dark()`; switching is
`data-theme="light|dark"` on `<html>` (absent = follow the OS).
Retheming a site = shipping one CSS file that overrides semantic
tokens — no rebuild, no component changes.

## Commands

```
pnpm install
pnpm gen:api     # server OpenAPI → apps/monitor/src/api/types.gen.ts
pnpm dev         # Vite dev server, proxies API to :7200
pnpm build       # → apps/monitor/dist  (serve with --ui-dist)
pnpm typecheck && pnpm test
```
