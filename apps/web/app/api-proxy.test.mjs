import assert from "node:assert/strict";
import test from "node:test";

import { NextRequest } from "next/server.js";

test("same-origin API proxy authenticates and forwards browser requests", async () => {
  const originalFetch = globalThis.fetch;
  const originalInternalUrl = process.env.API_INTERNAL_URL;
  const originalToken = process.env.READER_API_TOKEN;
  const calls = [];
  process.env.API_INTERNAL_URL = "http://api:8000";
  process.env.READER_API_TOKEN = "reader-secret";
  globalThis.fetch = async (input, init) => {
    calls.push({
      body: await new Response(init?.body).text(),
      method: init?.method,
      token: new Headers(init?.headers).get("X-Reader-API-Token"),
      url: String(input)
    });
    return Response.json(
      { updated: true },
      {
        headers: { "X-Content-Type-Options": "nosniff" },
        status: 201
      }
    );
  };
  try {
    const { POST } = await import("./api/[...path]/route.ts");
    const response = await POST(
      new NextRequest("http://reader.test/api/sources/7?view=settings", {
        body: JSON.stringify({ enabled: false }),
        headers: { "Content-Type": "application/json" },
        method: "POST"
      }),
      { params: Promise.resolve({ path: ["sources", "7"] }) }
    );

    assert.equal(response.status, 201);
    assert.equal(response.headers.get("x-content-type-options"), "nosniff");
    assert.deepEqual(calls, [
      {
        body: JSON.stringify({ enabled: false }),
        method: "POST",
        token: "reader-secret",
        url: "http://api:8000/sources/7?view=settings"
      }
    ]);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalInternalUrl === undefined) delete process.env.API_INTERNAL_URL;
    else process.env.API_INTERNAL_URL = originalInternalUrl;
    if (originalToken === undefined) delete process.env.READER_API_TOKEN;
    else process.env.READER_API_TOKEN = originalToken;
  }
});

test("same-origin API proxy rejects declared and streamed oversized request bodies", async () => {
  const originalFetch = globalThis.fetch;
  const originalInternalUrl = process.env.API_INTERNAL_URL;
  const originalToken = process.env.READER_API_TOKEN;
  let fetched = false;
  process.env.API_INTERNAL_URL = "http://api:8000";
  process.env.READER_API_TOKEN = "reader-secret";
  globalThis.fetch = async () => {
    fetched = true;
    return Response.json({ ok: true });
  };
  try {
    const { POST } = await import("./api/[...path]/route.ts");
    const response = await POST(
      new NextRequest("http://reader.test/api/imports/opml", {
        body: "x",
        headers: {
          "Content-Length": String(8 * 1024 * 1024 + 1),
          "Content-Type": "application/octet-stream"
        },
        method: "POST"
      }),
      { params: Promise.resolve({ path: ["imports", "opml"] }) }
    );

    assert.equal(response.status, 413);
    const streamed = await POST(
      new NextRequest("http://reader.test/api/imports/opml", {
        body: new Uint8Array(8 * 1024 * 1024 + 1),
        headers: {
          "Content-Length": "1",
          "Content-Type": "application/octet-stream"
        },
        method: "POST"
      }),
      { params: Promise.resolve({ path: ["imports", "opml"] }) }
    );
    assert.equal(streamed.status, 413);
    assert.equal(fetched, false);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalInternalUrl === undefined) delete process.env.API_INTERNAL_URL;
    else process.env.API_INTERNAL_URL = originalInternalUrl;
    if (originalToken === undefined) delete process.env.READER_API_TOKEN;
    else process.env.READER_API_TOKEN = originalToken;
  }
});
