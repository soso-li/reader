import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";

import SubscriptionManager, { isDesktopDragEnabled, needsAttention, sourceTypeCounts } from "./subscription-manager.tsx";
import { friendlyFetchError } from "./source-detail-dialog.tsx";
import { errorMessage, recentEntryLabel } from "./subscription-ui.ts";

test("subscription helpers keep type counts, attention union, and 30-day copy deterministic", () => {
  const now = Date.parse("2026-07-23T00:00:00Z");
  const expiredTrial = source({ status: "trial", status_changed_at: "2026-06-23T00:00:00Z" });
  assert.equal(needsAttention(expiredTrial, now), true);
  assert.equal(needsAttention(source({ status: "trial", status_changed_at: null }), now), false);
  assert.equal(needsAttention(source({ last_error: "timeout" }), now), true);
  assert.equal(needsAttention(source({ enabled: false }), now), true);
  assert.deepEqual(sourceTypeCounts([source({ media_type: "article" }), source({ id: 2, media_type: "video" })]), {
    article: 1,
    image: 0,
    notification: 0,
    podcast: 0,
    social: 0,
    video: 1
  });
  assert.equal(recentEntryLabel(0), "近 30 天无更新");
  assert.equal(recentEntryLabel(44), "近 30 天 44 条");
  assert.equal(isDesktopDragEnabled(1099), false);
  assert.equal(isDesktopDragEnabled(1100), true);
});

test("source detail management keeps external sharing unavailable until a source is public", async () => {
  const mounted = await mountManager();
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const privacy = mounted.container.querySelector('select[name="privacy_class"]');
    const external = mounted.container.querySelector('input[name="external_generation_allowed"]');
    assert.equal(privacy.value, "unclassified");
    assert.equal(external.disabled, true);
    assert.match(mounted.container.textContent, /允许把该来源的内容发送到外部 AI 服务/);

    await act(async () => setControlValue(privacy, "public"));
    assert.equal(external.disabled, false);
  } finally {
    await mounted.unmount();
  }
});

test("lanes use explicit folder type, card controls do not nest, and search does not change attention count", async () => {
  const mounted = await mountManager(async () => Response.json({}), [
    source({ id: 1, folder_id: 10, name: "错误源", last_error: "timeout", privacy_class: "private" }),
    source({ id: 2, folder_id: 10, name: "普通源", recent_entry_count_30d: 44 }),
    source({ id: 3, media_type: "video", name: "视频源" })
  ]);
  try {
    assert.match(mounted.container.textContent, /只看需处理 \(1\)/);
    assert.match(mounted.container.textContent, /Video 名称也属于文章/);
    const card = mounted.container.querySelector(".subscription-lane-card");
    const main = card.querySelector("button.subscription-card-main");
    const toggle = card.querySelector('button[role="switch"]');
    assert.ok(main);
    assert.ok(toggle);
    assert.equal(main.contains(toggle), false);
    assert.match(card.textContent, /example.com · 近 30 天无更新/);
    assert.ok(card.querySelector('[aria-label="私密来源"]'));
    assert.match(card.className, /is-error/);

    const search = mounted.container.querySelector('input[name="source_search"]');
    await act(async () => {
      search.focus();
      setControlValue(search, "普通", "input");
      const propertyChange = new window.Event("propertychange", { bubbles: true });
      Object.defineProperty(propertyChange, "propertyName", { value: "value" });
      search.dispatchEvent(propertyChange);
    });
    assert.match(mounted.container.textContent, /只看需处理 \(1\)/);
    assert.doesNotMatch(mounted.container.querySelector(".subscription-lane-board").textContent, /错误源/);
  } finally {
    await mounted.unmount();
  }
});

test("lane selection retains move, pause, restore, status, privacy, and external-sharing actions", async () => {
  const mounted = await mountManager();
  try {
    await act(async () => laneButton(mounted.container, "未分类", "全选本列").click());
    const bulk = mounted.container.querySelector(".subscription-bulk-bar");
    assert.match(bulk.textContent, /移动/);
    assert.match(bulk.textContent, /暂停/);
    assert.match(bulk.textContent, /恢复/);
    assert.match(bulk.textContent, /更多操作/);
    bulk.querySelector("summary").click();
    assert.match(bulk.textContent, /改状态/);
    assert.match(bulk.textContent, /改隐私/);
    assert.match(bulk.textContent, /允许外发/);
    assert.match(bulk.textContent, /禁止外发/);
  } finally {
    await mounted.unmount();
  }
});

test("adding a source reads only after the explicit Feed action, then keeps discovery and feedback inside the dialog", async () => {
  const calls = [];
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ body: init.body ? JSON.parse(String(init.body)) : null, method: init.method, url: String(input) });
    if (String(input) === "/api/sources/discover") return Response.json(feedDiscovery());
    return Response.json(source({ id: 9, name: "Found Feed", media_type: "video" }));
  }, [source({ id: 1 })], [{ id: 10, name: "文章文件夹", media_type: "article" }, { id: 20, name: "视频文件夹", media_type: "video" }]);
  try {
    await act(async () => buttonWithText(mounted.container, "添加订阅源").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    const input = dialog.querySelector('input[name="feed_url"]');
    const feedForm = dialog.querySelector(".source-add-read-form");
    assert.equal(input.type, "url");
    assert.equal(input.required, true);
    assert.equal(feedForm.noValidate, true);
    assert.equal(dialog.querySelector('input[name="source_name"]'), null);
    assert.equal(calls.length, 0);

    await act(async () => setControlValue(input, "not-a-feed-url", "input"));
    await submit(feedForm);
    assert.equal(calls.length, 0);
    assert.match(dialog.textContent, /请输入以 http:\/\/ 或 https:\/\/ 开头的有效 Feed 地址/);

    await act(async () => setControlValue(input, "https://example.com/feed.xml", "input"));
    await submit(feedForm);
    assert.deepEqual(calls, [{ body: { url: "https://example.com/feed.xml" }, method: "POST", url: "/api/sources/discover" }]);
    assert.match(dialog.textContent, /发现名称/);
    assert.match(dialog.textContent, /Found Feed/);
    assert.match(dialog.textContent, /状态[\s\S]*考察/);
    assert.equal(dialog.querySelectorAll(".source-add-preview-tabs button").length, 6);
    assert.equal(dialog.querySelector(".source-add-preview").querySelectorAll("a").length, 0);

    for (const [label, selector] of [["文章", ".source-add-preview-list .item-row"], ["社交", ".browse-social-card"], ["图片", ".browse-image-card"], ["视频", ".browse-video-card"], ["播客", ".source-add-preview-list .item-row"], ["通知", ".source-add-preview-list .item-row"]]) {
      await act(async () => previewTab(dialog, label).click());
      assert.ok(dialog.querySelector(selector), label);
      assert.equal(dialog.querySelector(".source-add-preview").querySelectorAll("a[href], audio, video, .browse-card-actions").length, 0, label);
    }
    assert.equal(calls.length, 1);
    await act(async () => setControlValue(dialog.querySelector('select[name="add_media_type"]'), "video"));
    assert.equal(previewTab(dialog, "视频").getAttribute("aria-pressed"), "true");
    await act(async () => setControlValue(dialog.querySelector('select[name="add_folder_id"]'), "20"));
    await act(async () => buttonWithText(dialog, "确认添加").click());
    assert.deepEqual(calls[1], {
      body: { folder_id: 20, media_type: "video", name: "Found Feed", status: "trial", url: "https://example.com/feed.xml" },
      method: "POST",
      url: "/api/sources"
    });
    assert.match(dialog.textContent, /已添加订阅源/);

    await act(async () => setControlValue(input, "https://example.com/next.xml", "input"));
    assert.equal(dialog.querySelector('button[type="button"].source-add-create'), null);
    assert.equal(calls.length, 2);
  } finally {
    await mounted.unmount();
  }
});

test("preview tells the truth about missing image, thumbnail, and audio without adding interactivity", async () => {
  const mounted = await mountManager(async (input) => String(input) === "/api/sources/discover" ? Response.json(feedDiscovery({ entries: [missingPreviewEntry()] })) : Response.json({}));
  try {
    await act(async () => buttonWithText(mounted.container, "添加订阅源").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    await act(async () => setControlValue(dialog.querySelector('input[name="feed_url"]'), "https://example.com/feed.xml", "input"));
    await submit(dialog.querySelector(".source-add-read-form"));
    for (const [label, missing] of [["图片", "无图片"], ["视频", "无缩略图"], ["播客", "无音频"]]) {
      await act(async () => previewTab(dialog, label).click());
      assert.match(dialog.querySelector(".source-add-preview").textContent, new RegExp(missing));
      assert.equal(dialog.querySelector(".source-add-preview").querySelectorAll("a[href], audio, video, .browse-card-actions").length, 0);
      assert.equal(dialog.querySelector(".source-add-preview").querySelectorAll("button").length, 6);
    }
  } finally {
    await mounted.unmount();
  }
});

test("source creation failure keeps the found Feed, URL, and dialog for correction or retry", async () => {
  let createFails = true;
  const mounted = await mountManager(async (input) => {
    if (String(input) === "/api/sources/discover") return Response.json(feedDiscovery());
    if (String(input) === "/api/sources") return createFails ? Response.json({ detail: "来源已存在" }, { status: 409 }) : Response.json(source({ id: 12 }));
    return Response.json({});
  });
  try {
    await act(async () => buttonWithText(mounted.container, "添加订阅源").click());
    const dialog = () => mounted.container.querySelector('[role="dialog"]');
    const input = dialog().querySelector('input[name="feed_url"]');
    await act(async () => setControlValue(input, "https://example.com/feed.xml", "input"));
    await submit(dialog().querySelector(".source-add-read-form"));
    await act(async () => buttonWithText(dialog(), "确认添加").click());
    assert.ok(dialog());
    assert.equal(input.value, "https://example.com/feed.xml");
    assert.match(dialog().textContent, /Found Feed/);
    assert.match(dialog().textContent, /来源已存在/);
    createFails = false;
    await act(async () => buttonWithText(dialog(), "确认添加").click());
    assert.match(dialog().textContent, /已添加订阅源/);
  } finally {
    await mounted.unmount();
  }
});

test("lane add locks the current type and exact folder, while Feed failure preserves the URL and dialog", async () => {
  const calls = [];
  let fails = true;
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ body: init.body ? JSON.parse(String(init.body)) : null, method: init.method, url: String(input) });
    if (String(input) === "/api/sources/discover") return fails ? Response.json({ detail: "Feed 格式无效" }, { status: 400 }) : Response.json(feedDiscovery({ title: "", candidateTitle: "" }));
    return Response.json(source({ id: 5 }));
  }, [source({ folder_id: 10 })]);
  try {
    await act(async () => laneButton(mounted.container, "Video 名称也属于文章", "添加订阅源").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    const input = dialog.querySelector('input[name="feed_url"]');
    await act(async () => setControlValue(input, "https://example.com/feed.xml", "input"));
    await submit(dialog.querySelector(".source-add-read-form"));
    assert.ok(mounted.container.querySelector('[role="dialog"]'));
    assert.equal(input.value, "https://example.com/feed.xml");
    assert.match(dialog.textContent, /Feed 格式无效/);

    fails = false;
    await submit(dialog.querySelector(".source-add-read-form"));
    assert.equal(dialog.querySelector('select[name="add_media_type"]'), null);
    assert.equal(dialog.querySelector('select[name="add_folder_id"]'), null);
    assert.match(dialog.textContent, /类型[\s\S]*文章/);
    assert.match(dialog.textContent, /文件夹[\s\S]*Video 名称也属于文章/);
    await act(async () => buttonWithText(dialog, "确认添加").click());
    assert.deepEqual(calls.at(-1), {
      body: { folder_id: 10, media_type: "article", name: "example.com", status: "trial", url: "https://example.com/feed.xml" },
      method: "POST",
      url: "/api/sources"
    });
  } finally {
    await mounted.unmount();
  }
});

test("the add dialog keeps native focus lifecycle and OPML separate from its Feed URL", async () => {
  const mounted = await mountManager();
  try {
    const trigger = buttonWithText(mounted.container, "添加订阅源");
    await act(async () => {
      trigger.click();
      await nextTick();
    });
    const dialog = mounted.container.querySelector('[role="dialog"]');
    const input = dialog.querySelector('input[name="feed_url"]');
    const opml = dialog.querySelector('form.source-add-opml');
    assert.equal(document.activeElement, input);
    assert.equal(opml.getAttribute("action"), "/actions/import-opml");
    assert.equal(opml.getAttribute("enctype"), "multipart/form-data");
    assert.equal(opml.noValidate, true);
    const file = opml.querySelector('input[name="file"]');
    assert.equal(file.type, "file");
    assert.equal(file.required, true);
    assert.equal(input.type, "url");
    assert.equal(input.name, "feed_url");

    await submit(opml);
    assert.match(dialog.textContent, /请选择一个 OPML 文件/);

    const last = buttonWithText(opml, "导入 OPML");
    last.focus();
    await act(async () => {
      last.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Tab", bubbles: true }));
      await nextTick();
    });
    assert.equal(document.activeElement, dialog.querySelector('button[aria-label="关闭添加订阅源"]'));
    const close = dialog.querySelector('button[aria-label="关闭添加订阅源"]');
    close.focus();
    await act(async () => {
      close.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Tab", shiftKey: true, bubbles: true }));
      await nextTick();
    });
    assert.equal(document.activeElement, last);
    await act(async () => {
      dialog.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
      await nextTick();
    });
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);
    assert.equal(document.activeElement, trigger);

    await act(async () => {
      trigger.click();
      await nextTick();
    });
    await act(async () => {
      mounted.container.querySelector(".source-add-overlay").dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true }));
      await nextTick();
    });
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);
  } finally {
    await mounted.unmount();
  }
});

test("restoring trial sources preserves trial while legacy archived sources become active", async () => {
  const calls = [];
  const mounted = await mountManager(async (input, init) => {
    calls.push({ body: JSON.parse(String(init.body)), url: String(input) });
    return Response.json({ updated: 1 });
  }, [
    source({ id: 1, enabled: false, name: "考察源", status: "trial" }),
    source({ id: 2, enabled: false, name: "旧归档源", status: "archived" })
  ]);
  try {
    await act(async () => laneButton(mounted.container, "未分类", "全选本列").click());
    await act(async () => buttonWithText(mounted.container, "恢复抓取").click());
    assert.deepEqual(calls, [
      { body: { ids: [1], set: { enabled: true } }, url: "/api/sources/bulk" },
      { body: { ids: [2], set: { enabled: true, status: "active" } }, url: "/api/sources/bulk" }
    ]);
  } finally {
    await mounted.unmount();
  }
});

test("legacy cards project as paused and restore atomically while a normal card pauses with enabled only", async () => {
  for (const status of ["muted", "archived"]) {
    const calls = [];
    const mounted = await mountManager(async (input, init = {}) => {
      calls.push({ body: JSON.parse(String(init.body)), method: init.method, url: String(input) });
      return Response.json({});
    }, [source({ enabled: true, name: `${status} 来源`, status })]);
    try {
      const card = mounted.container.querySelector(".subscription-lane-card");
      const control = card.querySelector(".subscription-card-switch");
      assert.match(card.className, /is-paused/);
      assert.equal(control.getAttribute("aria-checked"), "false");
      assert.equal(control.getAttribute("aria-label"), `${status} 来源恢复抓取`);
      assert.match(card.textContent, /已暂停/);

      await act(async () => {
        control.click();
        await Promise.resolve();
      });
      assert.deepEqual(calls, [{ body: { enabled: true, status: "active" }, method: "PATCH", url: "/api/sources/1" }]);
    } finally {
      await mounted.unmount();
    }
  }

  const calls = [];
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ body: JSON.parse(String(init.body)), method: init.method, url: String(input) });
    return Response.json({});
  });
  try {
    const control = mounted.container.querySelector(".subscription-card-switch");
    assert.equal(control.getAttribute("aria-checked"), "true");
    assert.equal(control.getAttribute("aria-label"), "来源暂停抓取");
    await act(async () => {
      control.click();
      await Promise.resolve();
    });
    assert.deepEqual(calls, [{ body: { enabled: false }, method: "PATCH", url: "/api/sources/1" }]);
  } finally {
    await mounted.unmount();
  }
});

test("every bulk action uses the direct JSON endpoint and cancel only clears selection", async () => {
  const cases = [
    { action: "移动", expected: { folder_id: 10 }, prepare: (container) => setControlValue(container.querySelector('select[aria-label="移动到文件夹"]'), "10") },
    { action: "暂停抓取", expected: { enabled: false } },
    { action: "恢复抓取", expected: { enabled: true }, sourceItem: source({ enabled: false }) },
    { action: "改状态", expected: { status: "trial" }, prepare: (container) => { openBulkMore(container); setControlValue(container.querySelector('select[aria-label="批量状态"]'), "trial"); } },
    { action: "改隐私", expected: { privacy_class: "private" }, prepare: (container) => { openBulkMore(container); setControlValue(container.querySelector('select[aria-label="批量来源隐私分类"]'), "private"); } },
    { action: "允许外发", expected: { external_generation_allowed: true }, sourceItem: source({ privacy_class: "public" }), prepare: openBulkMore },
    { action: "禁止外发", expected: { external_generation_allowed: false }, prepare: openBulkMore }
  ];

  for (const item of cases) {
    const call = await invokeBulkAction(item);
    assert.deepEqual(call, {
      body: { ids: [1], set: item.expected },
      url: "/api/sources/bulk"
    }, item.action);
  }

  const mounted = await mountManager();
  try {
    await act(async () => laneButton(mounted.container, "未分类", "全选本列").click());
    await act(async () => buttonWithText(mounted.container, "取消").click());
    assert.equal(mounted.container.querySelector(".subscription-bulk-bar"), null);
  } finally {
    await mounted.unmount();
  }
});

test("deleting a filtered folder moves every actual folder source before deleting it", async () => {
  const calls = [];
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ body: init.body ? JSON.parse(String(init.body)) : null, url: String(input) });
    return Response.json({});
  }, [
    source({ id: 1, folder_id: 10, name: "错误来源", last_error: "timeout" }),
    source({ id: 2, folder_id: 10, name: "隐藏的普通来源" })
  ]);
  try {
    await act(async () => buttonWithText(mounted.container, "只看需处理 (1)").click());
    await act(async () => {
      laneButton(mounted.container, "Video 名称也属于文章", "删除文件夹").click();
      await Promise.resolve();
    });
    const confirmation = mounted.container.querySelector("dialog.action-dialog[open]");
    assert.ok(confirmation);
    await act(async () => buttonWithText(confirmation, "删除文件夹").click());
    assert.deepEqual(calls, [
      { body: { ids: [1, 2], set: { folder_id: null } }, url: "/api/sources/bulk" },
      { body: null, url: "/api/folders/10" }
    ]);
  } finally {
    await mounted.unmount();
  }
});

test("source cards expose explicit status rails for error, paused, and trial states", async () => {
  const mounted = await mountManager(async () => Response.json({}), [
    source({ id: 1, name: "错误", last_error: "timeout" }),
    source({ id: 2, name: "暂停", enabled: false }),
    source({ id: 3, name: "考察", status: "trial" })
  ]);
  try {
    const cards = Array.from(mounted.container.querySelectorAll(".subscription-lane-card"));
    assert.match(cards[0].className, /is-error/);
    assert.match(cards[1].className, /is-paused/);
    assert.match(cards[2].className, /is-trial/);
    assert.match(cards[0].textContent, /抓取错误/);
    assert.match(cards[1].textContent, /已暂停/);
    assert.match(cards[2].textContent, /考察/);
  } finally {
    await mounted.unmount();
  }
});

test("formal source detail dialog has a single form, desktop controls, and complete close lifecycle", async () => {
  const mounted = await mountManager();
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    await nextTick();
    const dialog = mounted.container.querySelector('[role="dialog"]');
    assert.ok(dialog);
    assert.equal(dialog.closest("dialog")?.open, true);
    assert.equal(dialog.getAttribute("aria-modal"), "true");
    assert.equal(dialog.getAttribute("aria-labelledby"), "source-detail-title");
    assert.equal(dialog.querySelectorAll("form").length, 1);
    assert.equal(dialog.querySelectorAll('button[type="submit"]').length, 1);
    assert.equal(document.activeElement, dialog.querySelector('input[name="source_name"]'));
    assert.match(dialog.textContent, /管理/);
    assert.match(dialog.textContent, /RSS\/Atom\/Newsletter Feed 地址/);
    assert.match(dialog.textContent, /抓取网页全文/);
    assert.match(dialog.textContent, /永久删除订阅源/);
    assert.match(dialog.textContent, /监控/);
    assert.match(dialog.textContent, /价值/);
    assert.match(dialog.textContent, /信任分范围 0–100，按（已读 \+ 2×打开原文 \+ 3×星标 \+ 稍后读 \+ 入簇 - 重复）×100 \/ max（抓取数，1）计算。/);
    assert.equal(dialog.querySelector("a").getAttribute("rel"), "noopener noreferrer");
    assert.match(dialog.textContent, /重新抓取/);
    assert.match(dialog.textContent, /修改链接/);

    const closeButton = dialog.querySelector('button[aria-label="关闭来源详情"]');
    const saveButton = dialog.querySelector('button[type="submit"]');
    closeButton.focus();
    await act(async () => closeButton.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Tab", shiftKey: true, bubbles: true })));
    assert.equal(document.activeElement, saveButton);
    saveButton.focus();
    await act(async () => saveButton.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Tab", bubbles: true })));
    assert.equal(document.activeElement, closeButton);

    const name = dialog.querySelector('input[name="source_name"]');
    await act(async () => setControlValue(name, "草稿名称", "input"));
    await act(async () => dialog.querySelector('button[aria-label="关闭来源详情"]').click());
    let confirmation = mounted.container.querySelector("dialog.action-dialog[open]");
    assert.ok(confirmation);
    await act(async () => buttonWithText(confirmation, "继续编辑").click());
    assert.ok(mounted.container.querySelector('[role="dialog"]'));
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "草稿名称");

    await act(async () => dialog.querySelector('button[aria-label="关闭来源详情"]').click());
    confirmation = mounted.container.querySelector("dialog.action-dialog[open]");
    await act(async () => buttonWithText(confirmation, "放弃更改").click());
    await nextTick();
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);
    assert.equal(document.activeElement, mounted.container.querySelector('[data-source-card-id="1"]'));
  } finally {
    await mounted.unmount();
  }
});

test("source extraction preview exposes only the latest six choices and submits no arbitrary URL", async () => {
  const calls = [];
  const entries = Array.from({ length: 6 }, (_, index) => ({
    raw_entry_id: index + 10,
    title: `最近文章 ${index + 1}`,
    published_at: `2026-07-30T0${index}:00:00Z`
  }));
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({
      body: init.body ? JSON.parse(String(init.body)) : null,
      method: init.method,
      url: String(input)
    });
    if (!init.method) {
      return Response.json({
        entries,
        public_rules: {
          version: `fivefilters@${"a".repeat(40)}`,
          commit: "a".repeat(40),
          activated_at: null,
          bundled: true
        }
      });
    }
    return Response.json({
      raw_entry_id: 10,
      title: "最近文章 1",
      reading_html: "<p>安全预览正文</p>",
      rss_characters: 20,
      webpage_characters: 480,
      method: "manual",
      version: "manual-selector-v1",
      body_source: "webpage",
      web_fetch_status: "succeeded",
      adopted_webpage: true,
      matched_elements: 1,
      removed_elements: 2,
      diagnostics: [],
      fallback_reason: ""
    });
  }, [source({
    article_selector: "css:article",
    fetch_full_content: true,
    remove_selector: "css:.ad"
  })]);
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    const extraction = dialog.querySelector(".source-extraction-settings");
    assert.ok(extraction);
    assert.equal(extraction.querySelector('input[name="article_selector"]').value, "css:article");
    assert.equal(extraction.querySelector('input[name="remove_selector"]').value, "css:.ad");
    assert.equal(extraction.querySelector('input[type="url"]'), null);

    await act(async () => buttonWithText(extraction, "加载最近 6 篇").click());
    const choices = extraction.querySelector('select[name="preview_raw_entry_id"]');
    assert.equal(choices.options.length, 6);
    await act(async () => buttonWithText(extraction, "测试正文提取").click());

    assert.deepEqual(calls, [
      { body: null, method: undefined, url: "/api/sources/1/article-preview" },
      {
        body: {
          article_selector: "css:article",
          fetch_full_content: true,
          raw_entry_id: 10,
          remove_selector: "css:.ad"
        },
        method: "POST",
        url: "/api/sources/1/article-preview"
      }
    ]);
    assert.match(extraction.textContent, /手工规则/);
    assert.match(extraction.textContent, /RSS 20 字 · 网页 480 字/);
    assert.match(extraction.textContent, /匹配 1 个 · 删除 2 个/);
    assert.equal(extraction.querySelector(".source-article-preview-body").innerHTML, "<p>安全预览正文</p>");
  } finally {
    await mounted.unmount();
  }
});

test("public rule updates stay two-step and activate only the checked commit", async () => {
  const calls = [];
  const commit = "b".repeat(40);
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({
      body: init.body ? JSON.parse(String(init.body)) : null,
      method: init.method,
      url: String(input)
    });
    if (String(input).endsWith("/check")) {
      return Response.json({
        current_version: `fivefilters@${"a".repeat(40)}`,
        current_commit: "a".repeat(40),
        candidate_version: `fivefilters@${commit}`,
        candidate_commit: commit,
        rules_count: 630,
        skipped_count: 1263,
        subscribed_domains: 12,
        covered_subscribed_domains: 8,
        changed_subscribed_domains: 3,
        tested_subscribed_domains: 3,
        invalid_subscribed_domains: [],
        failed_subscribed_domains: [],
        preview: {
          hostname: "candidate.example",
          title: "候选文章",
          reading_html: "<p>候选安全正文</p>",
          rss_characters: 40,
          webpage_characters: 560,
          method: "fivefilters",
          version: `fivefilters@${commit}`,
          adopted_webpage: true,
          matched_elements: 1,
          removed_elements: 2,
          diagnostics: [],
          fallback_reason: "",
          passed: true
        },
        passed: true,
        can_activate: true
      });
    }
    return Response.json({
      version: `fivefilters@${commit}`,
      commit,
      activated_at: "2026-07-30T12:00:00Z",
      bundled: false
    });
  });
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const extraction = mounted.container.querySelector(".source-extraction-settings");
    await act(async () => buttonWithText(extraction, "检查公共规则更新").click());
    assert.match(extraction.textContent, /630 条安全规则/);
    assert.match(extraction.textContent, /覆盖 8 \/ 12 个已订阅域名/);
    assert.match(extraction.textContent, /实测 3 个/);
    assert.match(extraction.textContent, /aaaaaaaaaaaa → bbbbbbbbbbbb/);
    assert.equal(extraction.querySelector('[aria-label="候选公共规则预览"] .source-article-preview-body').innerHTML, "<p>候选安全正文</p>");
    assert.ok(buttonWithText(extraction, "采用此更新"));

    await act(async () => buttonWithText(extraction, "采用此更新").click());

    assert.deepEqual(calls, [
      { body: null, method: "POST", url: "/api/article-rules/check" },
      { body: { commit }, method: "POST", url: "/api/article-rules/activate" }
    ]);
    assert.match(mounted.container.textContent, /已采用公共规则更新/);
    assert.equal(Array.from(extraction.querySelectorAll("button")).find((button) => button.textContent.trim() === "采用此更新"), undefined);
  } finally {
    await mounted.unmount();
  }
});

test("invalid selectors block save while a quality warning stays saveable", async () => {
  const calls = [];
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({
      body: init.body ? JSON.parse(String(init.body)) : null,
      method: init.method,
      url: String(input)
    });
    if (!init.method) {
      return Response.json({
        entries: [{ raw_entry_id: 10, title: "最近文章", url: "https://example.com/article", published_at: null }],
        public_rules: { version: `fivefilters@${"a".repeat(40)}`, commit: "a".repeat(40), activated_at: null, bundled: true }
      });
    }
    if (String(input).endsWith("/article-preview")) {
      return Response.json({
        raw_entry_id: 10,
        title: "最近文章",
        reading_html: "<p>RSS 正文</p>",
        rss_characters: 400,
        webpage_characters: 120,
        method: "rss",
        version: "rss-v1",
        body_source: "rss",
        web_fetch_status: "failed",
        adopted_webpage: false,
        matched_elements: 1,
        removed_elements: 0,
        diagnostics: ["manual_quality_rejected"],
        fallback_reason: "手工正文未通过质量检查"
      });
    }
    return Response.json(source(JSON.parse(String(init.body))));
  }, [source({ fetch_full_content: true })]);
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    const selector = dialog.querySelector('input[name="article_selector"]');
    const save = dialog.querySelector('button[type="submit"]');

    await act(async () => setControlValue(selector, "css:article[", "input"));
    assert.equal(save.disabled, true);
    assert.match(dialog.textContent, /CSS 选择器语法无效/);

    await act(async () => setControlValue(selector, "  css:article  ", "input"));
    assert.equal(save.disabled, false);
    await act(async () => buttonWithText(dialog, "加载最近 6 篇").click());
    await act(async () => buttonWithText(dialog, "测试正文提取").click());
    assert.match(dialog.textContent, /手工正文未通过质量检查；仍可保存当前规则/);
    assert.equal(save.disabled, false);

    await act(async () => save.click());

    const saved = calls.find((call) => call.url === "/api/sources/1" && call.method === "PATCH");
    assert.equal(saved.body.article_selector, "css:article");
    assert.equal(saved.body.remove_selector, null);
    assert.equal(saved.body.fetch_full_content, true);
  } finally {
    await mounted.unmount();
  }
});

test("changing a source type immediately clears an incompatible folder and refreshes folder choices", async () => {
  const mounted = await mountManager(async () => Response.json({}), [source({ folder_id: 10 })], [
    { id: 10, name: "文章文件夹", media_type: "article" },
    { id: 20, name: "视频文件夹", media_type: "video" }
  ]);
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const folder = mounted.container.querySelector('select[name="source_folder_id"]');
    assert.equal(folder.value, "10");
    await act(async () => setControlValue(mounted.container.querySelector('select[name="source_media_type"]'), "video"));
    assert.equal(folder.value, "");
    assert.match(folder.textContent, /视频文件夹/);
    assert.doesNotMatch(folder.textContent, /文章文件夹/);
  } finally {
    await mounted.unmount();
  }
});

test("dirty dialog refuses a card or type switch until in-page discard confirmation passes", async () => {
  const mounted = await mountManager(async () => Response.json({}), [source({ id: 1, name: "第一个" }), source({ id: 2, name: "第二个" }), source({ id: 3, media_type: "video", name: "视频源" })]);
  try {
    await act(async () => mounted.container.querySelectorAll(".subscription-card-main")[0].click());
    await act(async () => setControlValue(mounted.container.querySelector('input[name="source_name"]'), "未保存", "input"));
    await act(async () => mounted.container.querySelectorAll(".subscription-card-main")[1].click());
    await act(async () => buttonWithText(mounted.container.querySelector("dialog.action-dialog[open]"), "继续编辑").click());
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "未保存");
    await act(async () => typeTab(mounted.container, "视频").click());
    await act(async () => buttonWithText(mounted.container.querySelector("dialog.action-dialog[open]"), "继续编辑").click());
    assert.ok(mounted.container.querySelector('[role="dialog"]'));

    await act(async () => mounted.container.querySelectorAll(".subscription-card-main")[1].click());
    await act(async () => buttonWithText(mounted.container.querySelector("dialog.action-dialog[open]"), "放弃更改").click());
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "第二个");
    await act(async () => setControlValue(mounted.container.querySelector('input[name="source_name"]'), "第二个草稿", "input"));
    await act(async () => typeTab(mounted.container, "视频").click());
    await act(async () => buttonWithText(mounted.container.querySelector("dialog.action-dialog[open]"), "放弃更改").click());
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);
    assert.equal(typeTab(mounted.container, "视频").getAttribute("aria-pressed"), "true");
  } finally {
    await mounted.unmount();
  }
});

test("incoming refresh preserves a dirty source draft but adopts a clean source baseline", async () => {
  const mounted = await mountManager();
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    await act(async () => setControlValue(mounted.container.querySelector('input[name="source_name"]'), "本地草稿", "input"));
    await mounted.rerender([source({ name: "服务端刷新" })]);
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "本地草稿");

    await act(async () => setControlValue(mounted.container.querySelector('input[name="source_name"]'), "来源", "input"));
    await mounted.rerender([source({ name: "干净刷新" })]);
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "干净刷新");
  } finally {
    await mounted.unmount();
  }
});

test("source detail overlay only closes on its own surface and Escape closes a clean dialog", async () => {
  const mounted = await mountManager();
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    await act(async () => dialog.querySelector(".source-detail-dialog").dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true })));
    assert.ok(mounted.container.querySelector('[role="dialog"]'));
    await act(async () => dialog.dispatchEvent(new window.KeyboardEvent("keydown", { key: "Escape", bubbles: true })));
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);

    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    await act(async () => mounted.container.querySelector(".source-detail-overlay").dispatchEvent(new window.MouseEvent("mousedown", { bubbles: true })));
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);
  } finally {
    await mounted.unmount();
  }
});

test("source detail save keeps the active dialog and draft on failure, then adopts the server baseline on success", async () => {
  let mode = "failure";
  const mounted = await mountManager(async (input, init = {}) => {
    if (String(input) === "/api/sources/1" && init.method === "PATCH") {
      if (mode === "failure") return Response.json({ detail: "保存失败" }, { status: 500 });
      return Response.json(source({ name: "服务端名称", url: "https://saved.example/feed.xml", media_type: "video" }));
    }
    return Response.json({});
  });
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const dialog = () => mounted.container.querySelector('[role="dialog"]');
    await act(async () => setControlValue(mounted.container.querySelector('input[name="source_name"]'), "本地草稿", "input"));
    await act(async () => setControlValue(mounted.container.querySelector('select[name="source_media_type"]'), "video"));
    await act(async () => dialog().querySelector('button[type="submit"]').click());
    assert.ok(dialog());
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "本地草稿");
    assert.equal(mounted.container.querySelector('select[name="source_media_type"]').value, "video");
    assert.match(dialog().textContent, /保存失败/);
    assert.equal(typeTab(mounted.container, "文章").getAttribute("aria-pressed"), "true");

    mode = "success";
    await act(async () => dialog().querySelector('button[type="submit"]').click());
    assert.ok(dialog());
    assert.equal(mounted.container.querySelector('input[name="source_name"]').value, "服务端名称");
    assert.equal(mounted.container.querySelector('input[name="source_url"]').value, "https://saved.example/feed.xml");
    assert.equal(typeTab(mounted.container, "视频").getAttribute("aria-pressed"), "true");
    assert.match(dialog().textContent, /已保存来源/);
  } finally {
    await mounted.unmount();
  }
});

test("legacy detail preserves its stored lifecycle until an explicit restore", async () => {
  for (const status of ["muted", "archived"]) {
    const calls = [];
    const mounted = await mountManager(async (input, init = {}) => {
      if (String(input) !== "/api/sources/1" || init.method !== "PATCH") return Response.json({});
      const body = JSON.parse(String(init.body));
      calls.push(body);
      return Response.json(source({ enabled: true, status, ...body }));
    }, [source({ enabled: true, status })]);
    try {
      await act(async () => mounted.container.querySelector(".subscription-card-main").click());
      const dialog = () => mounted.container.querySelector('[role="dialog"]');
      const toggle = () => dialog().querySelector(".source-detail-switch");
      assert.equal(toggle().getAttribute("aria-checked"), "true");
      assert.equal(toggle().getAttribute("aria-label"), "恢复抓取");

      await act(async () => setControlValue(dialog().querySelector('input[name="source_name"]'), "只改名称", "input"));
      await act(async () => dialog().querySelector('button[type="submit"]').click());
      assert.equal(calls[0].name, "只改名称");
      assert.equal(calls[0].status, undefined);
      assert.equal(calls[0].enabled, undefined);

      await act(async () => toggle().click());
      assert.equal(toggle().getAttribute("aria-checked"), "false");
      await act(async () => dialog().querySelector('button[type="submit"]').click());
      assert.equal(calls[1].status, "active");
      assert.equal(calls[1].enabled, true);
    } finally {
      await mounted.unmount();
    }
  }
});

test("monitor actions always show, refetch targets only that source, and diagnosis never leaks raw errors", async () => {
  const calls = [];
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ method: init.method, url: String(input) });
    return Response.json({});
  }, [source({ last_error: "nodename nor servname: failed" })]);
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    const dialog = mounted.container.querySelector('[role="dialog"]');
    assert.match(dialog.textContent, /无法解析来源域名/);
    assert.doesNotMatch(dialog.textContent, /nodename nor servname/);
    await act(async () => buttonWithText(dialog, "重新抓取").click());
    assert.deepEqual(calls, [{ method: "POST", url: `/api/sources/${source().id}/fetch` }]);
    assert.match(dialog.textContent, /已安排重新抓取/);
  } finally {
    await mounted.unmount();
  }
});

test("friendly fetch error mapper normalizes known diagnosis families", () => {
  assert.equal(friendlyFetchError("getaddrinfo ENOTFOUND"), "无法解析来源域名");
  assert.equal(friendlyFetchError("nodename nor servname"), "无法解析来源域名");
  assert.equal(friendlyFetchError("temporary failure in name resolution"), "无法解析来源域名");
  assert.equal(friendlyFetchError("request timed out"), "抓取超时");
  assert.equal(friendlyFetchError("HTTP 404 not found"), "来源地址不存在（404）");
  assert.equal(friendlyFetchError("TLS certificate verify failed"), "来源证书验证失败");
  assert.equal(friendlyFetchError("invalid rss xml feed"), "Feed 格式无效");
  assert.equal(friendlyFetchError("unknown internal detail"), "抓取失败");
});

test("subscription failures hide raw network diagnostics", () => {
  assert.equal(errorMessage(new Error("fetch failed for https://internal.example")), "操作失败");
  assert.equal(errorMessage(new Error("来源名称不能为空")), "来源名称不能为空");
});

test("source deletion leaves the dialog open on failure and closes only after success", async () => {
  let succeeds = false;
  const mounted = await mountManager(async (input, init = {}) => {
    if (String(input) === "/api/sources/1" && init.method === "DELETE") return succeeds ? Response.json({}) : Response.json({ detail: "无法删除" }, { status: 500 });
    return Response.json({});
  });
  try {
    await act(async () => mounted.container.querySelector(".subscription-card-main").click());
    await act(async () => buttonWithText(mounted.container, "永久删除").click());
    await act(async () => buttonWithText(mounted.container.querySelector("dialog.action-dialog[open]"), "永久删除").click());
    assert.ok(mounted.container.querySelector('[role="dialog"]'));
    assert.match(mounted.container.textContent, /无法删除/);
    succeeds = true;
    await act(async () => buttonWithText(mounted.container, "永久删除").click());
    await act(async () => buttonWithText(mounted.container.querySelector("dialog.action-dialog[open]"), "永久删除").click());
    await act(async () => { await nextTick(); });
    assert.equal(mounted.container.querySelector('[role="dialog"]'), null);
    assert.match(mounted.container.textContent, /已永久删除订阅源/);
    assert.equal(document.activeElement, buttonWithText(mounted.container, "添加订阅源"));
  } finally {
    await mounted.unmount();
  }
});

test("folder mutations serialize immediately and disable competing folder controls", async () => {
  const calls = [];
  let resolveRequest;
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ method: init.method, url: String(input) });
    return new Promise((resolve) => { resolveRequest = resolve; });
  });
  try {
    const create = buttonWithText(mounted.container, "新建文件夹");
    await act(async () => create.click());
    const prompt = mounted.container.querySelector("dialog.action-dialog[open]");
    await act(async () => setControlValue(prompt.querySelector('input[name="action_dialog_value"]'), "新文件夹", "input"));
    await act(async () => buttonWithText(prompt, "新建").click());
    await act(async () => create.click());
    assert.deepEqual(calls, [{ method: "POST", url: "/api/folders" }]);
    assert.equal(create.disabled, true);
    assert.equal(buttonWithText(mounted.container, "重命名").disabled, true);
    await act(async () => resolveRequest(Response.json({})));
  } finally {
    await mounted.unmount();
  }
});

function buttonWithText(container, label) {
  const button = Array.from(container.querySelectorAll("button")).find((candidate) => candidate.textContent.trim() === label);
  assert.ok(button, `missing button: ${label}`);
  return button;
}

function laneButton(container, laneName, label) {
  const lane = Array.from(container.querySelectorAll(".subscription-lane")).find((candidate) => candidate.querySelector(".subscription-lane-header strong")?.textContent === laneName);
  assert.ok(lane, `missing lane: ${laneName}`);
  return buttonWithText(lane, label);
}

function typeTab(container, label) {
  const button = Array.from(container.querySelectorAll(".subscription-type-tabs button")).find((candidate) => candidate.textContent.startsWith(label));
  assert.ok(button, `missing type tab: ${label}`);
  return button;
}

function previewTab(container, label) {
  const button = Array.from(container.querySelectorAll(".source-add-preview-tabs button")).find((candidate) => candidate.textContent === label);
  assert.ok(button, `missing preview tab: ${label}`);
  return button;
}

function nextTick() {
  return new Promise((resolve) => window.setTimeout(resolve, 0));
}

async function submit(form) {
  await act(async () => {
    form.dispatchEvent(new window.Event("submit", { bubbles: true, cancelable: true }));
    await nextTick();
  });
}

function feedDiscovery({ title = "Found Feed", candidateTitle = "Found Feed", entries = null } = {}) {
  return {
    candidates: [{ title: candidateTitle, url: "https://example.com/feed.xml" }],
    entries: entries || [
      {
        title: "预览条目",
        summary: "预览摘要",
        image_url: "",
        media_url: "https://example.com/preview.jpg",
        media_kind: "image",
        media_duration: 0,
        url: "https://example.com/posts/1",
        published_at: "2026-07-23T00:00:00Z"
      }
    ],
    site_url: "https://example.com",
    title
  };
}

function missingPreviewEntry() {
  return {
    title: "无媒体预览",
    summary: "没有图片、缩略图或音频",
    image_url: "",
    media_url: "",
    media_kind: "",
    media_duration: 0,
    url: "https://example.com/posts/missing",
    published_at: "2026-07-23T00:00:00Z"
  };
}

async function invokeBulkAction({ action, expected: _expected, prepare, sourceItem = source() }) {
  const calls = [];
  const mounted = await mountManager(async (input, init = {}) => {
    calls.push({ body: JSON.parse(String(init.body)), url: String(input) });
    return Response.json({ updated: 1 });
  }, [sourceItem]);
  try {
    await act(async () => laneButton(mounted.container, "未分类", "全选本列").click());
    if (prepare) await act(async () => prepare(mounted.container));
    await act(async () => {
      buttonWithText(mounted.container, action).click();
      await Promise.resolve();
    });
    assert.equal(calls.length, 1, `${action} should make one request`);
    return calls[0];
  } finally {
    await mounted.unmount();
  }
}

function openBulkMore(container) {
  const details = container.querySelector(".subscription-bulk-bar details");
  if (!details.open) details.querySelector("summary").click();
}

function setControlValue(control, value, eventName = "change") {
  const prototype = control.tagName === "SELECT" ? window.HTMLSelectElement.prototype : window.HTMLInputElement.prototype;
  const valueSetter = Object.getOwnPropertyDescriptor(prototype, "value").set;
  if (control.tagName === "INPUT") control.focus();
  valueSetter.call(control, value);
  control.dispatchEvent(new window.Event(eventName, { bubbles: true }));
  if (control.tagName === "INPUT") {
    const propertyChange = new window.Event("propertychange", { bubbles: true });
    Object.defineProperty(propertyChange, "propertyName", { value: "value" });
    control.dispatchEvent(propertyChange);
  }
}

async function mountManager(fetchImpl = async () => Response.json({}), sources = [source({ id: 1 })], folders = [{ id: 10, name: "Video 名称也属于文章", media_type: "article" }]) {
  const dom = installDom(fetchImpl);
  const root = createRoot(dom.container);
  await act(async () => {
    root.render(React.createElement(SubscriptionManager, {
      folders,
      sources
    }));
  });
  return {
    container: dom.container,
    async rerender(nextSources, nextFolders = folders) {
      await act(async () => {
        root.render(React.createElement(SubscriptionManager, { folders: nextFolders, sources: nextSources }));
      });
    },
    async unmount() {
      await act(async () => root.unmount());
      dom.restore();
    }
  };
}

function source(overrides = {}) {
  return {
    id: 1,
    folder_id: null,
    name: "来源",
    url: "https://example.com/feed.xml",
    site_url: "https://example.com",
    status: "active",
    media_type: "article",
    enabled: true,
    fetch_full_content: false,
    article_selector: null,
    remove_selector: null,
    privacy_class: "unclassified",
    external_generation_allowed: false,
    feed_trust_score: 0,
    fetched_count: 0,
    read_count: 0,
    opened_count: 0,
    starred_count: 0,
    read_later_count: 0,
    cluster_count: 0,
    duplicate_count: 0,
    recent_entry_count_30d: 0,
    last_error: "",
    last_fetched_at: null,
    status_changed_at: null,
    ...overrides
  };
}

function installDom(fetchImpl) {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", { url: "http://reader.test/?view=settings&settings_section=subscriptions" });
  dom.window.HTMLElement.prototype.attachEvent = function attachEvent(name, listener) {
    this.addEventListener(name.slice(2), listener);
  };
  dom.window.HTMLElement.prototype.detachEvent = function detachEvent(name, listener) {
    this.removeEventListener(name.slice(2), listener);
  };
  const previous = new Map();
  const setGlobal = (name, value) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  };
  setGlobal("window", dom.window);
  setGlobal("document", dom.window.document);
  setGlobal("navigator", dom.window.navigator);
  setGlobal("HTMLElement", dom.window.HTMLElement);
  setGlobal("HTMLInputElement", dom.window.HTMLInputElement);
  setGlobal("HTMLSelectElement", dom.window.HTMLSelectElement);
  setGlobal("Element", dom.window.Element);
  setGlobal("Node", dom.window.Node);
  setGlobal("Event", dom.window.Event);
  setGlobal("MouseEvent", dom.window.MouseEvent);
  setGlobal("fetch", fetchImpl);
  setGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  return {
    container: dom.window.document.getElementById("root"),
    restore() {
      for (const [name, descriptor] of previous) {
        if (descriptor) Object.defineProperty(globalThis, name, descriptor);
        else delete globalThis[name];
      }
      dom.window.close();
    }
  };
}
