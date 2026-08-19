import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { NextRequest } from "next/server.js";

import { cleanActionUrl } from "./actions/shared.ts";
import { proxy } from "../proxy.ts";

test("action redirects cannot leave the Reader origin", () => {
  const request = new NextRequest("http://reader.test/actions/theme", {
    headers: { referer: "https://evil.example/phish?keep=1&action_result=done" }
  });

  assert.equal(cleanActionUrl(request, request.headers.get("referer")).href, "http://reader.test/");
});

test("mutation proxy accepts same-origin browser submissions", () => {
  const response = proxy(new NextRequest("http://reader.test/actions/theme", {
    method: "POST",
    headers: { origin: "http://reader.test" }
  }));

  assert.equal(response.headers.get("x-middleware-next"), "1");
});

test("container-internal URLs use the browser-facing Host for mutation and redirect checks", () => {
  const originalDeployUrl = process.env.READER_DEPLOY_URL;
  process.env.READER_DEPLOY_URL = "http://192.0.2.10:43119";
  const request = new NextRequest("http://0.0.0.0:3000/actions/theme", {
    method: "POST",
    headers: {
      host: "192.0.2.10:43119",
      origin: "http://192.0.2.10:43119",
      referer: "http://192.0.2.10:43119/?view=settings&settings_section=general"
    }
  });

  try {
    assert.equal(proxy(request).headers.get("x-middleware-next"), "1");
    assert.equal(
      cleanActionUrl(request, request.headers.get("referer")).href,
      "http://192.0.2.10:43119/?view=settings&settings_section=general"
    );
  } finally {
    process.env.READER_DEPLOY_URL = originalDeployUrl;
  }
});

test("mutation proxy rejects a forged Host even when Origin matches it", () => {
  const originalDeployUrl = process.env.READER_DEPLOY_URL;
  process.env.READER_DEPLOY_URL = "http://reader.test";
  try {
    const response = proxy(new NextRequest("http://0.0.0.0:3000/api/sources/discover", {
      method: "POST",
      headers: {
        host: "evil.example",
        origin: "http://evil.example"
      }
    }));

    assert.equal(response.status, 403);
  } finally {
    process.env.READER_DEPLOY_URL = originalDeployUrl;
  }
});

test("mutation proxy rejects foreign and unidentified submitters", () => {
  const foreign = proxy(new NextRequest("http://reader.test/actions/theme", {
    method: "POST",
    headers: { referer: "https://evil.example/phish" }
  }));
  const unidentified = proxy(new NextRequest("http://reader.test/api/jobs/fetch", { method: "POST" }));

  assert.equal(foreign.status, 403);
  assert.equal(unidentified.status, 403);
});

test("mutation error surfaces never expose raw network exceptions", async () => {
  const files = await Promise.all([
    "./actions/client-user-state/route.ts",
    "./actions/event-user-state/route.ts",
    "./actions/uninterested/route.ts",
    "./filter-rule-manager.tsx",
    "./uninterested-actions.tsx"
  ].map((path) => readFile(new URL(path, import.meta.url), "utf8")));

  for (const source of files) {
    assert.match(source, /userFacingErrorMessage\(error,/);
    assert.doesNotMatch(source, /error instanceof Error \? error\.message/);
  }
});
