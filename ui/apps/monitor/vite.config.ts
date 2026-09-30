import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The app owns the root url space (a page is `/jobs/abc`); the
// whole JSON surface lives under /api. In dev that means exactly
// one proxy rule; in prod the server mounts dist/ at / and serves
// /api itself.
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    proxy: {
      "/api": {
        target: "http://127.0.0.1:7200",
        changeOrigin: true,
        // SSE must stream through unbuffered.
        ws: false,
      },
    },
  },
});
