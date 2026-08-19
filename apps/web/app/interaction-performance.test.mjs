import assert from "node:assert/strict";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot, hydrateRoot } from "react-dom/client";
import { renderToString } from "react-dom/server";

import { restoreAssistantTriggerFocus } from "./assistant-chat-window.tsx";
import Favicon from "./favicon.tsx";
import {
  BrowseImageCard,
  BrowseListRow,
  BrowseSocialCard,
  BrowseVideoCard
} from "./browse-item-card.tsx";
import BrowseView, { selectBrowseDetailAfterListLoad } from "./browse-view.tsx";
import ClusterRowLink from "./cluster-row-link.tsx";
import { previewText } from "./text-preview.ts";
import ReportReminder from "./report-reminder.tsx";
import SearchBox from "./search-box.tsx";
import { displaySourceName } from "./source-name.ts";
import StateFilterBar from "./state-filter-bar.tsx";
import { dispatchReaderListNavigationCommitted } from "./reader-list-navigation.ts";
import { testRouter } from "./test-next-navigation.mjs";
import { TimeText } from "./time-text.tsx";
import { TranslatedTitle } from "./translated-article-content.tsx";
import { pullRefreshEnabled } from "./use-pull-refresh.ts";

test("assistant Escape closes and restores focus to its trigger", async () => {
  const dom = installDom(async () => { throw new Error("unexpected fetch"); });
  try {
    const trigger = document.createElement("a");
    trigger.dataset.assistantTrigger = "cluster";
    trigger.href = "#assistant";
    document.body.append(trigger);
    document.body.focus();
    restoreAssistantTriggerFocus();
    await new Promise((resolve) => setTimeout(resolve, 0));
    assert.equal(document.activeElement, trigger);
  } finally {
    dom.restore();
  }
});

test("favicon uses the native image and falls back only after an error", async () => {
  const mounted = await mount(React.createElement(Favicon, { label: "MacStories", url: "https://www.macstories.net" }));
  try {
    const image = mounted.container.querySelector("img");
    assert.equal(mounted.container.textContent, "");
    assert.equal(image?.getAttribute("loading"), "lazy");
    assert.equal(image?.getAttribute("width"), "14");
    assert.equal(image?.getAttribute("height"), "14");

    await act(async () => image?.dispatchEvent(new window.Event("load")));
    assert.equal(mounted.container.textContent, "");

    await act(async () => image?.dispatchEvent(new window.Event("error")));
    assert.equal(mounted.container.querySelector("img"), null);
    assert.equal(mounted.container.textContent, "M");
  } finally {
    await mounted.unmount();
  }
});

test("favicon detects a cached failure that finished before hydration", async () => {
  const element = React.createElement(Favicon, { label: "MacStories", url: "https://www.macstories.net" });
  const dom = installDom(async () => { throw new Error("unexpected fetch"); });
  dom.container.innerHTML = renderToString(element);
  const image = dom.container.querySelector("img");
  Object.defineProperties(image, {
    complete: { configurable: true, value: true },
    naturalWidth: { configurable: true, value: 0 },
  });
  let root;
  try {
    await act(async () => { root = hydrateRoot(dom.container, element); });
    assert.equal(dom.container.querySelector("img"), null);
    assert.equal(dom.container.textContent, "M");
  } finally {
    if (root) await act(async () => root.unmount());
    dom.restore();
  }
});

test("above-the-fold favicons can opt into eager loading", async () => {
  const mounted = await mount(React.createElement(Favicon, { eager: true, label: "IT之家", url: "https://www.ithome.com" }));
  try {
    const image = mounted.container.querySelector("img");
    assert.equal(image?.getAttribute("loading"), "eager");
    assert.equal(image?.getAttribute("fetchpriority"), "high");
  } finally {
    await mounted.unmount();
  }
});

test("follow-up favicons stay lazy when the primary event favicon is eager", async () => {
  const mounted = await mount(
    React.createElement(ClusterRowLink, {
      active: false,
      apiUrl: "http://reader.test/api",
      eagerFavicons: true,
      href: "/?cluster_id=1",
      id: 1,
      meta: React.createElement(Favicon, { eager: true, label: "主来源", url: "https://primary.test" }),
      readLater: false,
      readStatus: "unread",
      sources: [
        { id: 1, published_at: null, source_name: "主来源", source_site_url: "https://primary.test", title: "Primary title" },
        { id: 2, published_at: null, source_name: "后续一", source_site_url: "https://followup-one.test", title: "Follow-up one" },
        { id: 3, published_at: null, source_name: "后续二", source_site_url: "https://followup-two.test", title: "Follow-up two" }
      ],
      starred: false,
      summary: "摘要",
      title: "Primary title"
    })
  );
  try {
    const loadingModes = [...mounted.container.querySelectorAll("img")].map((image) => image.getAttribute("loading"));
    assert.deepEqual(loadingModes, ["eager", "lazy", "lazy"]);
  } finally {
    await mounted.unmount();
  }
});

test("cluster row body clicks reuse the title link without hijacking controls", async () => {
  let selections = 0;
  const mounted = await mount(
    React.createElement(ClusterRowLink, {
      active: false,
      href: "/?cluster_id=1",
      id: 1,
      meta: React.createElement(
        React.Fragment,
        null,
        React.createElement(TimeText, { value: "2026-07-18T08:00:00Z" }),
        React.createElement("button", { type: "button" }, "操作")
      ),
      onSelect: (event) => {
        event.preventDefault();
        selections += 1;
      },
      readLater: false,
      readStatus: "unread",
      starred: false,
      summary: "可点击摘要",
      title: "条目标题"
    })
  );
  try {
    await act(async () => mounted.container.querySelector(".item-summary")?.click());
    assert.equal(selections, 1);
    await act(async () => mounted.container.querySelector("time")?.click());
    assert.equal(mounted.container.querySelector("time")?.getAttribute("title"), "点击切换相对时间");
    assert.equal(selections, 1);
    await act(async () => mounted.container.querySelector("button")?.click());
    assert.equal(selections, 1);
  } finally {
    await mounted.unmount();
  }
});

test("browse card bodies reuse their title links without hijacking controls", async () => {
  const item = browseItem({
    title: "Original browse title",
    title_translation: "浏览标题",
    summary: "Clickable browse summary"
  });
  const cases = [
    {
      component: BrowseListRow,
      props: { showThumbnail: false },
      body: ".item-summary"
    },
    {
      component: BrowseImageCard,
      props: {
        actions: React.createElement(
          "button",
          { type: "button" },
          "图片操作"
        )
      },
      body: ".browse-image-thumb"
    },
    {
      component: BrowseSocialCard,
      props: {},
      body: ".browse-social-content > p"
    },
    {
      component: BrowseVideoCard,
      props: {
        actions: React.createElement(
          "button",
          { type: "button" },
          "视频操作"
        )
      },
      body: ".browse-video-thumb"
    }
  ];

  for (const card of cases) {
    let selections = 0;
    const mounted = await mount(
      React.createElement(card.component, {
        ...card.props,
        href: "/?view=browse&item_id=7",
        item,
        onNavigate: (event) => {
          event.preventDefault();
          selections += 1;
        }
      })
    );
    try {
      assert.match(mounted.container.textContent, /Original browse title/);
      assert.match(mounted.container.textContent, /浏览标题/);
      await act(async () => mounted.container.querySelector(card.body)?.click());
      assert.equal(selections, 1);
      await act(async () => mounted.container.querySelector("time")?.click());
      assert.equal(selections, 1);
      await act(async () => mounted.container.querySelector("button")?.click());
      assert.equal(selections, 1);
    } finally {
      await mounted.unmount();
    }
  }
});

test("list loads hydrate detail only for an explicit item id", () => {
  const rows = [{ id: 41, content_text: "" }, { id: 42, content_text: "" }];
  assert.equal(selectBrowseDetailAfterListLoad(rows, null), null);
  assert.equal(selectBrowseDetailAfterListLoad(rows, 42)?.id, 42);
});

test("filtered articles return to the clustered complete stream", async () => {
  const mounted = await mount(
    React.createElement(BrowseView, {
      apiUrl: "/api",
      currentFilter: "",
      detailError: "",
      filteredOnly: true,
      initialPageCount: 0,
      initialSelectedItemId: null,
      items: [],
      listBackHref: "/?view=browse&media=article&pane=sources",
      listError: "",
      media: "article",
      offset: 0,
      pageSize: 80,
      query: "",
      scope: { view: "browse", media: "article", filtered: "1", pane: "list" },
      selectedItem: null,
      thumbnailMode: "auto"
    }),
    async () => { throw new Error("unexpected fetch"); },
    prepareBrowseDom
  );
  try {
    const link = [...mounted.container.querySelectorAll("a")].find((item) => item.textContent === "返回完整流");
    assert.ok(link);
    const url = new URL(link.href);
    assert.equal(url.searchParams.get("view"), "clusters");
    assert.equal(url.searchParams.has("media"), false);
    assert.equal(url.searchParams.has("filtered"), false);
  } finally {
    await mounted.unmount();
  }
});

test("every browse stream exposes pull to refresh", async () => {
  for (const media of ["social", "image", "video", "podcast", "notification"]) {
    const item = browseItem();
    const mounted = await mount(
      React.createElement(
        BrowseView,
        browseViewProps(item, {
          initialSelectedItemId: null,
          listBackHref: `/?view=browse&media=${media}&pane=sources`,
          media,
          scope: { view: "browse", media, pane: "list" },
          selectedItem: null
        })
      ),
      async () => {
        throw new Error("unexpected fetch");
      },
      prepareBrowseDom
    );
    try {
      const listPane = mounted.container.querySelector(".list-pane");
      assert.ok(listPane, media);
      const move = touchEvent("touchmove", 180);
      await act(async () => {
        listPane.dispatchEvent(touchEvent("touchstart", 100));
        listPane.dispatchEvent(move);
      });
      assert.equal(move.defaultPrevented, true, media);
      const indicator = listPane.querySelector(".pull-refresh");
      assert.equal(indicator?.getAttribute("data-state"), "pulling", media);
      assert.equal(indicator?.getAttribute("data-ready"), "true", media);
      assert.equal(
        indicator?.querySelector("span:not(.rail-label)"),
        null,
        media
      );
    } finally {
      await mounted.unmount();
    }
  }
});

test("pull refresh scope excludes searches and non-stream state lists", () => {
  assert.equal(pullRefreshEnabled("", ""), true);
  assert.equal(pullRefreshEnabled("all", ""), true);
  assert.equal(pullRefreshEnabled("unread", ""), true);
  assert.equal(pullRefreshEnabled("starred", ""), false);
  assert.equal(pullRefreshEnabled("read_later", ""), false);
  assert.equal(pullRefreshEnabled("dismissed", ""), false);
  assert.equal(pullRefreshEnabled("unread", "reader"), false);
  assert.equal(pullRefreshEnabled("unread", "", true), false);
});

test("pull refresh requires a released vertical threshold", async () => {
  const calls = [];
  const item = browseItem();
  const mounted = await mount(
    React.createElement(
      BrowseView,
      browseViewProps(item, {
        initialSelectedItemId: null,
        selectedItem: null
      })
    ),
    async (...args) => {
      calls.push(args);
      throw new Error("unexpected fetch");
    },
    prepareBrowseDom
  );
  try {
    const listPane = mounted.container.querySelector(".list-pane");
    assert.ok(listPane);
    await act(async () => {
      listPane.dispatchEvent(touchEvent("touchstart", 100));
      listPane.dispatchEvent(touchEvent("touchmove", 171));
      listPane.dispatchEvent(touchEvent("touchend", 171));
      listPane.dispatchEvent(touchEvent("touchstart", 100));
      listPane.dispatchEvent(touchEvent("touchmove", 180));
      listPane.dispatchEvent(touchEvent("touchcancel", 180));
      listPane.dispatchEvent(touchEvent("touchstart", 100, 100));
      listPane.dispatchEvent(touchEvent("touchmove", 180, 190));
      listPane.dispatchEvent(touchEvent("touchend", 180, 190));
    });
    assert.equal(calls.length, 0);
    assert.equal(listPane.querySelector(".pull-refresh"), null);
  } finally {
    await mounted.unmount();
  }
});

test("pull refresh spins once until its tracked job completes", async () => {
  const calls = [];
  let finishStatus;
  const statusResponse = new Promise((resolve) => {
    finishStatus = resolve;
  });
  const item = browseItem();
  const mounted = await mount(
    React.createElement(
      BrowseView,
      browseViewProps(item, {
        initialSelectedItemId: null,
        selectedItem: null
      })
    ),
    async (input, init = {}) => {
      const url = String(input);
      calls.push({ method: init.method ?? "GET", url });
      if (url === "/api/jobs/fetch") {
        return Response.json({ mode: "queued", job_id: "fetch-1" });
      }
      if (url === "/api/jobs/fetch/fetch-1") return statusResponse;
      throw new Error(`unexpected fetch ${url}`);
    },
    prepareBrowseDom
  );
  try {
    const listPane = mounted.container.querySelector(".list-pane");
    assert.ok(listPane);
    await act(async () => {
      listPane.dispatchEvent(touchEvent("touchstart", 100));
      listPane.dispatchEvent(touchEvent("touchmove", 180));
      listPane.dispatchEvent(touchEvent("touchend", 180));
      await Promise.resolve();
      await Promise.resolve();
    });
    assert.equal(
      listPane.querySelector(".pull-refresh")?.getAttribute("data-state"),
      "refreshing"
    );
    assert.equal(window.sessionStorage.getItem("reader:pull-refresh-job"), "fetch-1");

    await act(async () => {
      listPane.dispatchEvent(touchEvent("touchstart", 100));
      listPane.dispatchEvent(touchEvent("touchmove", 180));
      listPane.dispatchEvent(touchEvent("touchend", 180));
    });
    assert.equal(
      calls.filter((call) => call.method === "POST").length,
      1
    );

    await act(async () => {
      finishStatus(Response.json({ status: "complete" }));
      await statusResponse;
      await Promise.resolve();
      await Promise.resolve();
    });
    assert.equal(
      listPane.querySelector(".pull-refresh")?.getAttribute("data-state"),
      "success"
    );
  } finally {
    await mounted.unmount();
  }
});

test("pull refresh exposes a visible failure instead of reloading", async () => {
  const item = browseItem();
  const mounted = await mount(
    React.createElement(
      BrowseView,
      browseViewProps(item, {
        initialSelectedItemId: null,
        selectedItem: null
      })
    ),
    async (input) => {
      const url = String(input);
      if (url === "/api/jobs/fetch") {
        return Response.json({ mode: "queued", job_id: "fetch-failed" });
      }
      if (url === "/api/jobs/fetch/fetch-failed") {
        return Response.json({ status: "failed" });
      }
      throw new Error(`unexpected fetch ${url}`);
    },
    prepareBrowseDom
  );
  try {
    const listPane = mounted.container.querySelector(".list-pane");
    await act(async () => {
      listPane.dispatchEvent(touchEvent("touchstart", 100));
      listPane.dispatchEvent(touchEvent("touchmove", 180));
      listPane.dispatchEvent(touchEvent("touchend", 180));
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });
    const indicator = listPane.querySelector(".pull-refresh");
    assert.equal(indicator?.getAttribute("data-state"), "error");
    assert.match(indicator?.textContent ?? "", /刷新失败，请重试/);
  } finally {
    await mounted.unmount();
  }
});

test("browse detail renders cached bilingual content without translation requests", async () => {
  const item = browseItem({
    title: "Cached English title",
    title_translation: "缓存中文标题",
    content_text: "Cached English detail body.",
    content_translation: "缓存中文正文。",
    url: ""
  });
  const calls = [];
  const mounted = await mount(
    React.createElement(BrowseView, browseViewProps(item)),
    async (...args) => {
      calls.push(args);
      throw new Error("unexpected fetch");
    },
    prepareBrowseDom
  );
  try {
    await act(async () => undefined);
    assert.match(mounted.container.textContent, /Cached English title/);
    assert.match(mounted.container.textContent, /缓存中文标题/);
    assert.match(mounted.container.textContent, /Cached English detail body/);
    assert.match(mounted.container.textContent, /缓存中文正文/);
    assert.equal(calls.length, 0);
    assert.equal(
      mounted.container.querySelector("button[aria-label='阅读模式']"),
      null
    );
  } finally {
    await mounted.unmount();
  }
});

test("server reading policy suppresses stale bilingual content without a request", async () => {
  const item = browseItem({
    title: "2026 世界电容大会",
    content_text: "2026 世界电容大会・全球演讲嘉宾招募中",
    content_translation: "这是旧判定生成的中文改写。",
    reading_translation_needed: false,
    url: ""
  });
  const calls = [];
  const mounted = await mount(
    React.createElement(BrowseView, browseViewProps(item)),
    async (...args) => {
      calls.push(args);
      throw new Error("unexpected fetch");
    },
    prepareBrowseDom
  );
  try {
    await act(async () => undefined);
    assert.match(mounted.container.textContent, /2026 世界电容大会・全球演讲嘉宾招募中/);
    assert.doesNotMatch(mounted.container.textContent, /旧判定生成的中文改写/);
    assert.equal(mounted.container.querySelector(".translation-toolbar"), null);
    assert.equal(calls.length, 0);
  } finally {
    await mounted.unmount();
  }
});

test("only the selected browse detail requests a missing translation", async () => {
  const calls = [];
  const item = browseItem({
    title: "A list title stays cache only",
    content_text: "The selected detail body requests one translation.",
    content_translation: ""
  });
  const mounted = await mount(
    React.createElement(BrowseView, browseViewProps(item)),
    async (input) => {
      calls.push(String(input));
      return Response.json({
        status: "ready",
        translation: "当前详情正文只请求一次翻译。",
        model_version: "test",
        updated_at: null
      });
    },
    prepareBrowseDom
  );
  try {
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    assert.deepEqual(calls, ["/api/translations"]);
    assert.match(
      mounted.container.textContent,
      /当前详情正文只请求一次翻译/
    );
  } finally {
    await mounted.unmount();
  }
});

test("browse detail keeps the original and retry control when translation fails", async () => {
  const item = browseItem({
    title: "Translation failure",
    content_text: "The original detail remains visible after translation fails.",
    content_translation: ""
  });
  const mounted = await mount(
    React.createElement(BrowseView, browseViewProps(item)),
    async () => new Response("", { status: 503 }),
    prepareBrowseDom
  );
  try {
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });
    assert.match(
      mounted.container.textContent,
      /The original detail remains visible/
    );
    assert.match(mounted.container.textContent, /翻译失败/);
    assert.ok(
      mounted.container.querySelector(
        "button:not([disabled])"
      )
    );
  } finally {
    await mounted.unmount();
  }
});

test("list titles use cached translations without starting a request storm", async () => {
  const calls = [];
  const mounted = await mount(
    React.createElement(TranslatedTitle, {
      initialTranslation: "",
      text: "An untranslated list title"
    }),
    async (...args) => {
      calls.push(args);
      return Response.json({ status: "ready", translation: "列表标题" });
    }
  );
  try {
    await act(async () => undefined);
    assert.equal(calls.length, 0);
    assert.equal(mounted.container.textContent, "An untranslated list title");
  } finally {
    await mounted.unmount();
  }
});

test("cached title translations do not expose markdown emphasis markers", async () => {
  const mounted = await mount(
    React.createElement(TranslatedTitle, {
      initialTranslation: "**列表标题**",
      text: "An untranslated list title"
    })
  );
  try {
    assert.equal(mounted.container.textContent, "An untranslated list title列表标题");
  } finally {
    await mounted.unmount();
  }
});

test("cached title translations remove inline markdown emphasis markers", async () => {
  const mounted = await mount(
    React.createElement(TranslatedTitle, {
      initialTranslation: "这款 **PDNob Pro PDF Editor** 的优惠价格仅需 **$39.97**（使用优惠券）。",
      text: "This PDNob Pro PDF Editor deal is just $39.97 with a coupon"
    })
  );
  try {
    assert.equal(
      mounted.container.textContent,
      "This PDNob Pro PDF Editor deal is just $39.97 with a coupon这款 PDNob Pro PDF Editor 的优惠价格仅需 $39.97（使用优惠券）。"
    );
  } finally {
    await mounted.unmount();
  }
});

test("cached title translations keep only the translated side of transport dividers", async () => {
  const original = "Apple highlights 2026 sales tax holidays for Macs, iPads, and more in 10 states";
  const translated = "Apple 在 10 个州推出 2026 年 Mac、iPad 及其他产品的销售税假日优惠";
  const mounted = await mount(
    React.createElement(TranslatedTitle, {
      initialTranslation: `${original}\\n-> \n\n${translated}`,
      text: original
    })
  );
  try {
    assert.equal(mounted.container.textContent, `${original}${translated}`);
  } finally {
    await mounted.unmount();
  }
});

test("list previews remove feed HTML and translation transport artifacts", async () => {
  const mounted = await mount(
    React.createElement(TranslatedTitle, {
      initialTranslation: "苹果发布新系统\\n->",
      text: "Apple releases a new system"
    })
  );
  try {
    assert.equal(previewText("<p>Windows Central 报道 &amp; 分析</p>"), "Windows Central 报道 & 分析");
    assert.equal(displaySourceName("https://www.scientificamerican.com/platform/syndication/rss/"), "scientificamerican.com");
    assert.equal(mounted.container.textContent, "Apple releases a new system苹果发布新系统");
  } finally {
    await mounted.unmount();
  }
});

test("report reminder has stable server and hydration visibility", () => {
  const html = renderToString(
    React.createElement(ReportReminder, {
      date: "2026-07-18",
      error: "",
      initialDismissed: false,
      report: { status: "ready", title: "7 月 18 日日报" }
    })
  );
  assert.match(html, /前日报告已生成/);
});

test("report reminder dismissal reuses one dated cookie", async () => {
  const mounted = await mount(
    React.createElement(ReportReminder, {
      date: "2026-07-18",
      error: "",
      initialDismissed: false,
      report: null
    })
  );
  try {
    await act(async () => mounted.container.querySelector("button[aria-label='关闭提醒']")?.click());
    assert.match(document.cookie, /(?:^|; )reader_report_reminder_dismissed_date=2026-07-18(?:;|$)/);
    assert.doesNotMatch(document.cookie, /reader_report_reminder_2026-07-18/);
  } finally {
    await mounted.unmount();
  }
});

test("report reminder migrates and removes legacy per-day cookies", async () => {
  const mounted = await mount(
    React.createElement(ReportReminder, {
      date: "2026-07-18",
      error: "",
      initialDismissed: true,
      report: null
    }),
    async () => { throw new Error("unexpected fetch"); },
    () => {
      document.cookie = "reader_report_reminder_2026-07-17=1; Path=/";
      document.cookie = "reader_report_reminder_2026-07-18=1; Path=/";
    }
  );
  try {
    assert.match(document.cookie, /(?:^|; )reader_report_reminder_dismissed_date=2026-07-18(?:;|$)/);
    assert.doesNotMatch(document.cookie, /reader_report_reminder_2026-07-/);
  } finally {
    await mounted.unmount();
  }
});

test("search clear navigates once immediately and filters expose their current state", async () => {
  const pushes = [];
  const previousPush = testRouter.push;
  testRouter.push = (href) => pushes.push(href);
  const mounted = await mount(
    React.createElement(
      React.Fragment,
      null,
      React.createElement(SearchBox, {
        placeholder: "搜索事件、正文、来源",
        query: "seed",
        scope: { view: "clusters", filter: "unread" }
      }),
      React.createElement(StateFilterBar, {
        currentFilter: "unread",
        scope: { view: "clusters", filter: "unread" }
      })
    )
  );
  try {
    const input = mounted.container.querySelector("input");
    assert.equal(input?.getAttribute("aria-label"), "搜索事件、正文、来源");
    await act(async () => mounted.container.querySelector("button[aria-label='清空搜索']")?.click());
    assert.equal(pushes.length, 1);
    assert.doesNotMatch(pushes[0], /q=/);
    await new Promise((resolve) => setTimeout(resolve, 1100));
    assert.equal(pushes.length, 1);
    assert.equal(mounted.container.querySelector("a.filter-unread")?.getAttribute("aria-current"), "page");
  } finally {
    testRouter.push = previousPush;
    await mounted.unmount();
  }
});

test("search icon only expands; form submission starts the search state", async () => {
  const navigations = [];

  function Harness() {
    const [pending, setPending] = React.useState(false);
    return React.createElement(SearchBox, {
      onNavigate: (navigation) => {
        navigations.push(navigation);
        setPending(true);
      },
      pending,
      placeholder: "搜索事件、正文、来源",
      query: "",
      scope: { view: "clusters", filter: "unread" }
    });
  }

  const mounted = await mount(
    React.createElement(Harness),
    async () => { throw new Error("unexpected fetch"); },
    () => { window.HTMLElement.prototype.attachEvent = () => undefined; }
  );
  try {
    const searchButton = mounted.container.querySelector("button[aria-label='搜索']");
    assert.equal(searchButton?.getAttribute("type"), "button");
    await act(async () => searchButton?.click());
    await act(async () => searchButton?.click());
    const input = mounted.container.querySelector("input");
    assert.ok(input);
    assert.equal(navigations.length, 0);
    assert.doesNotMatch(mounted.container.textContent, /正在搜索/);

    await act(async () => input.dispatchEvent(new window.KeyboardEvent("keydown", { bubbles: true, cancelable: true, key: "Enter" })));
    assert.equal(navigations.length, 1);
    assert.doesNotMatch(navigations[0].href, /\bnl=/);
    assert.match(mounted.container.textContent, /正在搜索/);
  } finally {
    await mounted.unmount();
  }
});

test("client-owned search and filters report navigation without asking Next to rerender the page", async () => {
  const pushes = [];
  const navigations = [];
  const previousPush = testRouter.push;
  testRouter.push = (href) => pushes.push(href);
  const mounted = await mount(
    React.createElement(
      React.Fragment,
      null,
      React.createElement(SearchBox, {
        onNavigate: (navigation) => navigations.push(navigation),
        pending: true,
        placeholder: "搜索事件、正文、来源",
        query: "seed",
        scope: { view: "clusters", filter: "unread" }
      }),
      React.createElement(StateFilterBar, {
        currentFilter: "unread",
        onNavigate: (navigation) => navigations.push(navigation),
        pending: true,
        scope: { view: "clusters", filter: "unread" }
      })
    )
  );
  try {
    const input = mounted.container.querySelector("input");
    await act(async () => {
      Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set.call(input, "IT之家");
      input.dispatchEvent(new window.Event("input", { bubbles: true }));
    });
    await act(async () => mounted.container.querySelector("form")?.requestSubmit());
    await act(async () => mounted.container.querySelector("a.filter-all")?.click());

    assert.equal(pushes.length, 0);
    assert.equal(navigations.length, 2);
    assert.equal(navigations[0].query, "seed");
    assert.equal(navigations[1].filter, "all");
    assert.match(mounted.container.textContent, /正在搜索/);
    assert.equal(mounted.container.querySelector("nav[aria-label='状态筛选']")?.getAttribute("aria-busy"), "true");
  } finally {
    testRouter.push = previousPush;
    await mounted.unmount();
  }
});

test("folder view reveals the existing mobile source pane without a Next navigation", async () => {
  const pushes = [];
  const navigations = [];
  const previousPush = testRouter.push;
  testRouter.push = (href) => pushes.push(href);
  const mounted = await mount(
    React.createElement(StateFilterBar, {
      currentFilter: "unread",
      onNavigate: (navigation) => navigations.push(navigation),
      scope: { view: "clusters", filter: "unread", pane: "list" }
    }),
    undefined,
    () => { document.getElementById("root").className = "app-shell mobile-list mobile-detail"; }
  );
  try {
    await act(async () => mounted.container.querySelector("a.filter-sources")?.click());

    assert.equal(pushes.length, 0);
    assert.equal(navigations.length, 0);
    assert.match(window.location.search, /pane=sources/);
    assert.doesNotMatch(mounted.container.className, /mobile-(?:list|detail)/);
  } finally {
    testRouter.push = previousPush;
    await mounted.unmount();
  }
});

test("both mobile filter bars follow the latest committed list scope", async () => {
  const mounted = await mount(React.createElement(StateFilterBar, {
    currentFilter: "unread",
    scope: { view: "clusters", filter: "unread", pane: "sources" }
  }));
  try {
    await act(async () => {
      dispatchReaderListNavigationCommitted("/?view=clusters&filter=all&folder_id=2&pane=list");
    });

    assert.match(mounted.container.querySelector("a.filter-all").className, /active/);
    assert.match(mounted.container.querySelector("a.filter-sources").href, /folder_id=2/);
  } finally {
    await mounted.unmount();
  }
});

test("shared filter state cannot leak from clusters into browse navigation", async () => {
  function Harness() {
    const [browse, setBrowse] = React.useState(false);
    return React.createElement(
      React.Fragment,
      null,
      React.createElement("button", { onClick: () => setBrowse(true) }, "浏览"),
      React.createElement(StateFilterBar, {
        currentFilter: "unread",
        scope: browse
          ? { view: "browse", media: "article", filter: "unread", pane: "list" }
          : { view: "clusters", filter: "unread", pane: "list" }
      })
    );
  }

  const mounted = await mount(React.createElement(Harness));
  try {
    await act(async () => {
      dispatchReaderListNavigationCommitted("/?view=clusters&filter=all&folder_id=2&pane=list");
    });
    await act(async () => mounted.container.querySelector("button")?.click());

    const href = mounted.container.querySelector("a.filter-all").href;
    assert.match(href, /view=browse/);
    assert.doesNotMatch(href, /folder_id=2/);
  } finally {
    await mounted.unmount();
  }
});

test("time toggles are keyboard reachable and invalid preview text is hidden", async () => {
  const mounted = await mount(React.createElement(TimeText, { value: "2026-07-18T08:00:00Z" }));
  try {
    const time = mounted.container.querySelector("time");
    assert.equal(time?.getAttribute("role"), "button");
    assert.equal(time?.getAttribute("tabindex"), "0");
    assert.equal(previewText("null"), "无摘要");
  } finally {
    await mounted.unmount();
  }
});

function browseItem(overrides = {}) {
  return {
    id: 7,
    source_id: 3,
    source_name: "Example Source",
    source_site_url: "https://example.com",
    title: "Example title",
    title_translation: "",
    summary: "Example summary",
    summary_translation: "",
    image_url: "",
    media_url: "",
    media_kind: "",
    media_duration: 0,
    content_text: "Example content",
    content_translation: "",
    reading_translation_needed: true,
    url: "https://example.com/article",
    published_at: "2026-07-18T08:00:00Z",
    read_status: "unread",
    read_later: false,
    starred: false,
    filtered: false,
    filter_rules: [],
    ...overrides
  };
}

function browseViewProps(item, overrides = {}) {
  return {
    apiUrl: "/api",
    currentFilter: "",
    detailError: "",
    filteredOnly: false,
    initialPageCount: 1,
    initialSelectedItemId: item.id,
    items: [item],
    listBackHref: "/?view=browse&media=notification&pane=sources",
    listError: "",
    media: "notification",
    offset: 0,
    pageSize: 80,
    query: "",
    scope: {
      view: "browse",
      media: "notification",
      pane: "detail",
      item_id: item.id
    },
    selectedItem: item,
    thumbnailMode: "auto",
    ...overrides
  };
}

function prepareBrowseDom() {
  window.requestAnimationFrame = (callback) => {
    callback();
    return 1;
  };
  window.cancelAnimationFrame = () => undefined;
  window.HTMLElement.prototype.scrollTo = () => undefined;
}

function touchEvent(type, clientY, clientX = 0) {
  const event = new window.Event(type, { bubbles: true, cancelable: true });
  Object.defineProperty(event, "touches", {
    configurable: true,
    value: [{ clientX, clientY }]
  });
  return event;
}

async function mount(element, fetchImpl = async () => { throw new Error("unexpected fetch"); }, prepareDom = () => undefined) {
  const dom = installDom(fetchImpl);
  prepareDom();
  const root = createRoot(dom.container);
  await act(async () => root.render(element));
  return {
    container: dom.container,
    async unmount() {
      await act(async () => root.unmount());
      dom.restore();
    }
  };
}

function installDom(fetchImpl) {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://reader.test/"
  });
  const previous = new Map();
  const setGlobal = (name, value) => {
    previous.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
    Object.defineProperty(globalThis, name, { configurable: true, writable: true, value });
  };
  setGlobal("window", dom.window);
  setGlobal("document", dom.window.document);
  setGlobal("navigator", dom.window.navigator);
  setGlobal("HTMLElement", dom.window.HTMLElement);
  setGlobal("Element", dom.window.Element);
  setGlobal("Node", dom.window.Node);
  setGlobal("Event", dom.window.Event);
  setGlobal("KeyboardEvent", dom.window.KeyboardEvent);
  setGlobal("FormData", dom.window.FormData);
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
