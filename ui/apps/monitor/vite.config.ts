import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Dev: the SPA runs on Vite's port, the dispatcher on :7200 —
// every API path is proxied so the app code never knows the
// difference. Prod: `vite build` → dist/, served by the server
// itself via `dispatcher serve --ui-dist ui/apps/monitor/dist`.
const API_PATHS = [
  "/state",
  "/arenas",
  "/jobs",
  "/monitor",
  "/health",
  "/settings",
  "/filter-presets",
];

export default defineConfig({
  // The server mounts the built app at /ui (api/app.py _mount_ui);
  // dev serves under the same prefix so router basepath is one
  // value everywhere.
  base: "/ui/",
  plugins: [react(), tailwindcss()],
  server: {
    proxy: Object.fromEntries(
      API_PATHS.map((p) => [p, "http://127.0.0.1:7200"]),
    ),
  },
});
