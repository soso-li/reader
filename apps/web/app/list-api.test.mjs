import assert from "node:assert/strict";
import test from "node:test";

import { loadBrowseList, loadClusterCount, loadClusterList, normalizeListFilter } from "./list-api.ts";

test("list filter normalization preserves the explicit all state", () => {
  assert.equal(normalizeListFilter("all"), "");
  assert.equal(normalizeListFilter(null), "unread");
});

test("cluster list client loads rows and count from the current filter and search state", async () => {
  const calls = [];
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    calls.push(url);
    if (url.pathname === "/clusters/count") return Response.json({ count: 23 });
    return Response.json([{ id: 7, title: "IT之家" }]);
  };
  try {
    const scope = {
      filter: "starred",
      folder_id: 4,
      q: "IT之家",
      order: "oldest"
    };
    const rows = await loadClusterList("http://reader.test", scope, 50);
    const count = await loadClusterCount("http://reader.test", scope);

    assert.deepEqual(rows, [{ id: 7, title: "IT之家" }]);
    assert.equal(count, 23);
    assert.equal(calls.length, 2);
    for (const url of calls) {
      assert.equal(url.searchParams.get("folder_id"), "4");
      assert.equal(url.searchParams.get("q"), "IT之家");
      assert.equal(url.searchParams.get("starred"), "true");
      assert.equal(url.searchParams.has("read_status"), false);
    }
    const rowsUrl = calls.find((url) => url.pathname === "/clusters");
    assert.equal(rowsUrl?.searchParams.get("limit"), "50");
    assert.equal(rowsUrl?.searchParams.get("offset"), "0");
    assert.equal(rowsUrl?.searchParams.get("order"), "oldest");
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test("browse list client preserves media and unread scope", async () => {
  const calls = [];
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    calls.push(new URL(String(input)));
    return Response.json([{ id: 9, title: "一条消息" }]);
  };
  try {
    const rows = await loadBrowseList("http://reader.test", {
      filter: "unread",
      media: "notification",
      source_id: 8,
      q: "发布"
    }, 80);

    assert.deepEqual(rows, [{ id: 9, title: "一条消息" }]);
    assert.equal(calls.length, 1);
    assert.equal(calls[0].pathname, "/items");
    assert.equal(calls[0].searchParams.get("read_status"), "unread");
    assert.equal(calls[0].searchParams.get("media_type"), "notification");
    assert.equal(calls[0].searchParams.get("source_id"), "8");
    assert.equal(calls[0].searchParams.get("q"), "发布");
    assert.equal(calls[0].searchParams.get("include_content"), "false");
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test("filtered browse scope uses the diagnostic item projection", async () => {
  const calls = [];
  const previousFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    calls.push(new URL(String(input)));
    return Response.json([{ id: 10, title: "已过滤条目", filtered: true }]);
  };
  try {
    const rows = await loadBrowseList("http://reader.test", {
      filtered: "1",
      media: "article",
      folder_id: 3,
      q: "推广"
    }, 80);

    assert.equal(rows[0].filtered, true);
    assert.equal(calls[0].pathname, "/items");
    assert.equal(calls[0].searchParams.get("filtered_only"), "true");
    assert.equal(calls[0].searchParams.get("media_type"), "article");
    assert.equal(calls[0].searchParams.get("folder_id"), "3");
    assert.equal(calls[0].searchParams.get("q"), "推广");
  } finally {
    globalThis.fetch = previousFetch;
  }
});
