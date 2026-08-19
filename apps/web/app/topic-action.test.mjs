import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { NextRequest } from "next/server.js";

import { POST } from "./actions/topic/route.ts";

test("empty topic submissions return an in-place validation error", async () => {
  const originalFetch = globalThis.fetch;
  let fetched = false;
  try {
    globalThis.fetch = async () => {
      fetched = true;
      throw new Error("unexpected fetch");
    };
    const request = new NextRequest("http://reader.test/actions/topic", {
      method: "POST",
      headers: {
        "content-type": "application/x-www-form-urlencoded",
        referer: "http://reader.test/?view=topics"
      },
      body: "name=&query=&description="
    });

    const response = await POST(request);
    const location = new URL(response.headers.get("location"));

    assert.equal(response.status, 303);
    assert.equal(location.searchParams.get("action_error"), "主题名称和关键词不能为空");
    assert.equal(fetched, false);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("topic forms keep required metadata but send validation to the in-page error", async () => {
  const context = await readFile(new URL("./context-panel.tsx", import.meta.url), "utf8");
  const page = await readFile(new URL("./page.tsx", import.meta.url), "utf8");

  assert.match(context, /name="name"[^>]*required/);
  assert.match(context, /name="query"[^>]*required/);
  assert.match(page, /name="name"[^>]*required/);
  assert.match(page, /name="query"[^>]*required/);
  assert.match(context, /action="\/actions\/topic"[^>]*noValidate/);
  assert.match(page, /action="\/actions\/topic"[^>]*noValidate/);
});
