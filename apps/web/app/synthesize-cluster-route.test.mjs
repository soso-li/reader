import assert from "node:assert/strict";
import test from "node:test";

import { NextRequest } from "next/server.js";

import { POST } from "./actions/synthesize-cluster/route.ts";


test("the visible synthesis action delegates the configured provider to the API", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input, init) => {
    calls.push({ url: String(input), body: init?.body });
    return Response.json({ status: "missing", task_status: "blocked" });
  };

  try {
    const form = new FormData();
    form.set("event_uid", "11111111-1111-4111-8111-111111111111");
    form.set("redirect", "/?view=clusters");
    const response = await POST(
      new NextRequest("http://reader.test/actions/synthesize-cluster", {
        method: "POST",
        body: form
      })
    );

    assert.equal(response.status, 303);
    assert.equal(calls.length, 1);
    assert.deepEqual(JSON.parse(String(calls[0].body)), {});
  } finally {
    globalThis.fetch = originalFetch;
  }
});
