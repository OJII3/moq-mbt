import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import { moonbit } from "vite-plugin-moonbit";

const projectRoot = fileURLToPath(new URL("../../..", import.meta.url));

export default defineConfig({
  plugins: [moonbit({ root: projectRoot, target: "js" })],
});
