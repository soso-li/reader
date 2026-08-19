import assert from "node:assert/strict";
import test from "node:test";

import { legacyInstalledLaunchTarget } from "./installed-reader-launch.tsx";
import readerManifest from "./manifest.ts";
import Page from "./page.tsx";

test("installed Reader launches in the unread list", () => {
  const manifest = readerManifest();
  assert.equal(manifest.start_url, "/?filter=unread&pane=list");
  assert.equal(manifest.scope, "/");
  assert.equal(manifest.display, "standalone");
});

test("legacy installed launch URLs migrate to unread without changing browser or explicit navigation", () => {
  assert.equal(
    legacyInstalledLaunchTarget("http://reader.test/?view=clusters&filter=all&pane=list&order=desc", true, "navigate"),
    "/?view=clusters&filter=unread&pane=list&order=desc"
  );
  assert.equal(legacyInstalledLaunchTarget("http://reader.test/?filter=all&pane=list", false, "navigate"), null);
  assert.equal(legacyInstalledLaunchTarget("http://reader.test/?filter=all&pane=list", true, "reload"), null);
  assert.equal(legacyInstalledLaunchTarget("http://reader.test/?filter=all&pane=detail&cluster_id=42", true, "navigate"), null);
});

test("settings pages fetch only navigation and current-section data", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input) => {
    const path = new URL(String(input)).pathname;
    calls.push(path);
    if (path === "/about") {
      return Response.json({ build_time: "development", commit: "", deploy_url: "", docs: [], health: {}, version: "dev" });
    }
    return Response.json(path === "/pipeline/status" ? { completed_at: null } : []);
  };
  try {
    await Page({ searchParams: Promise.resolve({ view: "settings", settings_section: "about" }) });
    assert.deepEqual(calls.sort(), ["/about", "/browse/summary", "/pipeline/status"]);

    calls.length = 0;
    await Page({ searchParams: Promise.resolve({ view: "settings", settings_section: "subscriptions" }) });
    assert.deepEqual(calls.sort(), ["/browse/summary", "/folders", "/pipeline/status", "/sources"]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("invalid settings deep links redirect to the canonical subscriptions URL", async () => {
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "settings", settings_section: "folders", settings_status: "active", dev: "1" }) }),
    /redirect:\/\?view=settings&settings_section=subscriptions&settings_status=active&dev=1/
  );
});

test("invalid report periods and impossible calendar dates redirect before API requests", async () => {
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "reports", period: "quarter", date: "2026-07-22" }) }),
    /redirect:\/\?view=reports&period=day&date=2026-07-22/
  );
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "reports", period: "day", date: "2026-99-99" }) }),
    /redirect:\/\?view=reports&period=day/
  );
});

test("invalid reading media filters and panes redirect without losing valid scope", async () => {
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "browse", media: "bogus", filter: "unread", pane: "list", source_id: "12", item_id: "34" }) }),
    /redirect:\/\?view=browse&media=social&source_id=12&item_id=34&filter=unread&pane=list/
  );
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "clusters", filter: "bogus", pane: "list", cluster_id: "56" }) }),
    /redirect:\/\?view=clusters&cluster_id=56&filter=unread&pane=list/
  );
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "clusters", filter: "starred", pane: "bogus", q: "AI" }) }),
    /redirect:\/\?view=clusters&q=AI&filter=starred/
  );
});

test("legacy article lists redirect to the clustered complete stream", async () => {
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "browse", media: "article", source_id: "12", q: "AI", filter: "starred", pane: "list", offset: "40" }) }),
    /redirect:\/\?view=clusters&source_id=12&q=AI&filter=starred&pane=list/
  );
});

test("invalid numeric deep links redirect to canonical scopes", async () => {
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "clusters", folder_id: "abc", source_id: "12", cluster_id: "-1", offset: "1.5", filter: "starred", pane: "list" }) }),
    /redirect:\/\?view=clusters&source_id=12&filter=starred&pane=list/
  );
  await assert.rejects(
    () => Page({ searchParams: Promise.resolve({ view: "topics", topic_id: "0" }) }),
    /redirect:\/\?view=topics/
  );
});

test("valid detail panes survive reload and fetch their selected item", async () => {
  const originalFetch = globalThis.fetch;
  const calls = [];
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    calls.push(`${url.pathname}${url.search}`);
    if (url.pathname === "/items/34") {
      return Response.json({ id: 34, title: "详情", source_name: "测试", read_status: "unread", starred: false, read_later: false, filtered: false, filter_rules: [] });
    }
    if (url.pathname === "/browse/summary") return Response.json([]);
    if (url.pathname === "/items/count") return Response.json({ count: 0 });
    return Response.json([]);
  };
  try {
    for (const media of ["social", "article"]) {
      await Page({ searchParams: Promise.resolve({ view: "browse", media, filter: "unread", pane: "detail", item_id: "34" }) });
    }
    assert.equal(calls.filter((call) => call.startsWith("/items/34")).length, 2);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("failed reading detail requests stay in the mobile detail pane", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    if (url.pathname === "/clusters/999999999") {
      return Response.json({ detail: "事件聚类不存在" }, { status: 404 });
    }
    if (url.pathname === "/items/999999999") {
      return Response.json({ detail: "条目不存在" }, { status: 404 });
    }
    if (url.pathname === "/pipeline/status") return Response.json({ completed_at: null });
    if (url.pathname === "/clusters/count") return Response.json({ count: 0 });
    return Response.json([]);
  };
  try {
    for (const searchParams of [
      { view: "clusters", cluster_id: "999999999", filter: "unread", pane: "detail" },
      { view: "browse", media: "social", item_id: "999999999", filter: "unread", pane: "detail" }
    ]) {
      const page = await Page({ searchParams: Promise.resolve(searchParams) });
      assert.match(page.props.className, /\bmobile-detail\b/);
      assert.doesNotMatch(page.props.className, /\bmobile-list\b/);
    }
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("item deep links redirect to the source media surface", async () => {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input) => {
    const url = new URL(String(input));
    if (url.pathname === "/sources/navigation") {
      return Response.json([{ id: 91, folder_id: null, media_type: "podcast", status: "active", enabled: true }]);
    }
    if (url.pathname === "/items/20594") {
      return Response.json({ id: 20594, source_id: 91, title: "播客", source_name: "测试", read_status: "unread", starred: false, read_later: false, filtered: false, filter_rules: [] });
    }
    if (url.pathname === "/pipeline/status") return Response.json({ completed_at: null });
    return Response.json([]);
  };
  try {
    await assert.rejects(
      () => Page({ searchParams: Promise.resolve({ view: "browse", media: "social", item_id: "20594", filter: "starred", pane: "detail", q: "AI" }) }),
      /redirect:\/\?view=browse&media=podcast&q=AI&item_id=20594&filter=starred&pane=detail/
    );
  } finally {
    globalThis.fetch = originalFetch;
  }
});
