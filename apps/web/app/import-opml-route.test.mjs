import assert from "node:assert/strict";
import test from "node:test";

import { NextRequest } from "next/server.js";

import { POST } from "./actions/import-opml/route.ts";


test("OPML import authenticates through the shared API client", async () => {
  const originalFetch = globalThis.fetch;
  const originalToken = process.env.READER_API_TOKEN;
  const calls = [];
  process.env.READER_API_TOKEN = "reader-secret";
  globalThis.fetch = async (_input, init) => {
    calls.push({
      body: init?.body,
      token: new Headers(init?.headers).get("X-Reader-API-Token")
    });
    return Response.json({ imported: 1 });
  };

  try {
    const form = new FormData();
    form.set(
      "file",
      new File(["<opml><body/></opml>"], "reader.opml", { type: "text/xml" })
    );
    const response = await POST(
      new NextRequest("http://reader.test/actions/import-opml?view=settings", {
        body: form,
        method: "POST"
      })
    );

    assert.equal(response.status, 303);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].token, "reader-secret");
    assert.ok(calls[0].body instanceof FormData);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalToken === undefined) delete process.env.READER_API_TOKEN;
    else process.env.READER_API_TOKEN = originalToken;
  }
});

test("OPML import rejects declared and streamed oversized bodies before parsing", async () => {
  const originalFetch = globalThis.fetch;
  let fetched = false;
  globalThis.fetch = async () => {
    fetched = true;
    return Response.json({ imported: 1 });
  };

  try {
    const declared = await POST(
      new NextRequest("http://reader.test/actions/import-opml?view=settings", {
        body: "x",
        headers: {
          "Content-Length": String(2 * 1024 * 1024 + 128 * 1024),
          "Content-Type": "multipart/form-data; boundary=reader"
        },
        method: "POST"
      })
    );
    assert.equal(declared.status, 303);
    assert.match(declared.headers.get("location"), /OPML.*2MB/);

    const request = new NextRequest("http://reader.test/actions/import-opml?view=settings", {
      body: new Uint8Array(2 * 1024 * 1024 + 128 * 1024),
      headers: { "Content-Type": "multipart/form-data; boundary=reader" },
      method: "POST"
    });
    assert.equal(request.headers.get("content-length"), null);

    const response = await POST(request);

    assert.equal(response.status, 303);
    assert.match(response.headers.get("location"), /OPML.*2MB/);
    assert.equal(fetched, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
