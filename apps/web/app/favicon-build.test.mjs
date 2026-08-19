import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const root = new URL("../../../", import.meta.url);

test("compose keeps the authenticated API private behind the web origin", async () => {
  const dockerfile = await readFile(new URL("../Dockerfile", import.meta.url), "utf8");
  const compose = await readFile(new URL("docker-compose.yml", root), "utf8");

  assert.match(dockerfile, /ARG NEXT_PUBLIC_API_URL/);
  assert.match(dockerfile, /ENV NEXT_PUBLIC_API_URL=\$NEXT_PUBLIC_API_URL/);
  assert.match(compose, /NEXT_PUBLIC_API_URL:\s*\/api[\s\S]*?API_INTERNAL_URL/);
  assert.match(compose, /READER_API_TOKEN:\s*\$\{READER_API_TOKEN:\?/);
  assert.match(compose, /127\.0\.0\.1:8007:8000/);
  assert.match(compose, /\.\/docs:\/app\/project-docs:ro/);
});
