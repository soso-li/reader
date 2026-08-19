import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { JSDOM } from "jsdom";
import React, { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";

import ArticleContent, {
  applyBionicReading,
  extractTranslationBlocks,
  insertBlockTranslations
} from "./article-content.tsx";
import TranslatedArticleContent from "./translated-article-content.tsx";

const richHtml = [
  '<p data-reader-block-id="block-aaaaaaaaaaaaaaaa">OpenAI released a <a href="https://example.com/report" target="_blank" rel="noopener noreferrer">new report</a>.</p>',
  '<figure><img src="/images/rss?src=x" alt="chart"><figcaption data-reader-block-id="block-bbbbbbbbbbbbbbbb">Quarterly results</figcaption></figure>',
  '<table><tbody><tr><td data-reader-block-id="block-cccccccccccccccc">Revenue grew</td></tr></tbody></table>',
  '<pre data-reader-block-id="block-dddddddddddddddd"><code>const url = "https://example.com";</code></pre>',
  '<p data-reader-block-id="block-eeeeeeeeeeeeeeee">Read https://example.com/details today</p>'
].join("");

test("reading HTML wins over legacy text and keeps the safe structure", () => {
  const output = renderToStaticMarkup(
    React.createElement(ArticleContent, {
      html: richHtml,
      text: "legacy fallback must not render"
    })
  );

  assert.match(output, /data-reader-block-id="block-aaaaaaaaaaaaaaaa"/);
  assert.match(output, /target="_blank"/);
  assert.match(output, /rel="noopener noreferrer"/);
  assert.match(output, /<figure>/);
  assert.match(output, /<table>/);
  assert.match(output, /<pre/);
  assert.doesNotMatch(output, /legacy fallback/);
});

test("failed article images never expose their original URL", async () => {
  const secretUrl = "https://images.example/private.png?token=secret";
  const dom = installDom(() => Promise.reject(new Error("unexpected request")));
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(ArticleContent, {
          html: `<img src="/images/rss?src=private" data-reader-original-src="${secretUrl}">`,
          text: "legacy fallback"
        })
      );
    });
    await act(async () => {
      dom.container.querySelector("img").dispatchEvent(new window.Event("error", { bubbles: true }));
    });

    assert.equal(dom.container.textContent, "图片不可用");
    assert.doesNotMatch(dom.container.textContent, /token=secret/);

    await act(async () => {
      root.render(React.createElement(ArticleContent, { text: `![](${secretUrl})` }));
    });
    await act(async () => {
      dom.container.querySelector("img")?.dispatchEvent(new window.Event("error", { bubbles: true }));
    });

    assert.equal(dom.container.textContent, "图片不可用");
    assert.doesNotMatch(dom.container.textContent, /token=secret/);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("article images reserve a visible loading position until they finish", async () => {
  const dom = installDom(() => Promise.reject(new Error("unexpected request")));
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(ArticleContent, {
          html: '<p>正文先显示</p><img src="/images/rss?src=slow" alt="图表">',
          text: "legacy fallback"
        })
      );
    });

    const frame = dom.container.querySelector(".article-image-frame");
    assert.ok(frame);
    assert.equal(frame.getAttribute("aria-busy"), "true");
    assert.match(frame.textContent, /图片正在加载/);

    await act(async () => {
      frame.querySelector("img").dispatchEvent(new window.Event("load"));
    });

    assert.equal(frame.getAttribute("aria-busy"), null);
    assert.equal(frame.querySelector(".article-image-loading"), null);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("article images that fail before hydration do not stay loading", async () => {
  const dom = installDom(() => Promise.reject(new Error("unexpected request")));
  const root = createRoot(dom.container);
  Object.defineProperty(window.HTMLImageElement.prototype, "complete", {
    configurable: true,
    get: () => true
  });
  Object.defineProperty(window.HTMLImageElement.prototype, "naturalWidth", {
    configurable: true,
    get: () => 0
  });
  try {
    await act(async () => {
      root.render(
        React.createElement(ArticleContent, {
          html: '<img src="/images/rss?src=failed" alt="图表">',
          text: "legacy fallback"
        })
      );
    });

    assert.equal(dom.container.textContent, "图片不可用：图表");
    assert.equal(dom.container.querySelector(".article-image-loading"), null);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("block translations and Bionic preserve DOM while skipping code and URLs", () => {
  const dom = new JSDOM(`<div id="root">${richHtml}</div>`);
  const root = dom.window.document.querySelector("#root");
  const blocks = extractTranslationBlocks(root);

  assert.deepEqual(blocks, [
    { id: "block-aaaaaaaaaaaaaaaa", text: "OpenAI released a new report." },
    { id: "block-bbbbbbbbbbbbbbbb", text: "Quarterly results" },
    { id: "block-cccccccccccccccc", text: "Revenue grew" },
    { id: "block-eeeeeeeeeeeeeeee", text: "Read today" }
  ]);
  assert.equal(
    insertBlockTranslations(
      root,
      blocks.map((block, index) => ({ id: block.id, text: `译文 ${index + 1}` }))
    ),
    true
  );
  applyBionicReading(root);

  assert.equal(root.querySelectorAll("img").length, 1);
  assert.equal(root.querySelectorAll("table").length, 1);
  assert.equal(root.querySelectorAll(".bilingual-translation").length, 4);
  assert.equal(root.querySelector("a")?.getAttribute("href"), "https://example.com/report");
  assert.equal(root.querySelector("code")?.textContent, 'const url = "https://example.com";');
  assert.equal(
    root.querySelector('[data-reader-block-id="block-eeeeeeeeeeeeeeee"]')?.textContent,
    "Read https://example.com/details today译文 4"
  );
  assert.ok(root.querySelector('[data-reader-block-id="block-aaaaaaaaaaaaaaaa"] strong'));
});

test("nested list items translate their own text without duplicating child items", () => {
  const dom = new JSDOM(
    '<ul><li data-reader-block-id="block-1111111111111111">Parent text<ul><li data-reader-block-id="block-2222222222222222">Child text</li></ul>Parent tail</li></ul>'
  );
  const root = dom.window.document.body;

  assert.deepEqual(extractTranslationBlocks(root), [
    { id: "block-1111111111111111", text: "Parent text Parent tail" },
    { id: "block-2222222222222222", text: "Child text" }
  ]);
  assert.equal(
    insertBlockTranslations(root, [
      { id: "block-1111111111111111", text: "父级译文" },
      { id: "block-2222222222222222", text: "子级译文" }
    ]),
    true
  );
  const parent = root.querySelector('[data-reader-block-id="block-1111111111111111"]');
  assert.equal(parent?.children[0]?.className, "bilingual-translation rich-block-translation");
  assert.equal(parent?.children[1]?.tagName, "UL");
});

test("rich translations and Bionic survive an unrelated parent rerender", async () => {
  const dom = installDom(() => Promise.reject(new Error("unexpected request")));
  const root = createRoot(dom.container);
  const blocks = [
    { id: "block-aaaaaaaaaaaaaaaa", text: "译文 1" },
    { id: "block-bbbbbbbbbbbbbbbb", text: "译文 2" },
    { id: "block-cccccccccccccccc", text: "译文 3" },
    { id: "block-eeeeeeeeeeeeeeee", text: "译文 4" }
  ];
  const render = () =>
    React.createElement(ArticleContent, {
      bionic: true,
      html: richHtml,
      text: "legacy fallback",
      translations: blocks
    });
  try {
    await act(async () => root.render(render()));
    assert.equal(dom.container.querySelectorAll(".rich-block-translation").length, 4);
    assert.ok(dom.container.querySelectorAll(".bionic-word").length > 0);

    await act(async () => root.render(render()));
    assert.equal(dom.container.querySelectorAll(".rich-block-translation").length, 4);
    assert.ok(dom.container.querySelectorAll(".bionic-word").length > 0);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("switching articles never commits the previous article translation", async () => {
  const dom = installDom(() => new Promise(() => {}));
  const root = createRoot(dom.container);
  const commits = [];
  function Probe({ html, initialTranslation, text }) {
    React.useLayoutEffect(() => {
      commits.push(dom.container.textContent);
    }, [html]);
    return React.createElement(TranslatedArticleContent, {
      apiUrl: "/api",
      html,
      initialTranslation,
      text,
      translationNeeded: true
    });
  }
  try {
    await act(async () => {
      root.render(
        React.createElement(Probe, {
          html: '<p data-reader-block-id="block-aaaaaaaaaaaaaaaa">Article A</p>',
          initialTranslation: "译文 A",
          text: "Article A"
        })
      );
    });
    await act(async () => {
      root.render(
        React.createElement(Probe, {
          html: '<p data-reader-block-id="block-bbbbbbbbbbbbbbbb">Article B</p>',
          initialTranslation: "",
          text: "Article B"
        })
      );
    });

    assert.match(commits.at(-1), /Article B/);
    assert.doesNotMatch(commits.at(-1), /译文 A/);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("rich HTML with no text blocks stays readable without sending legacy text", async () => {
  let requests = 0;
  const dom = installDom(async () => {
    requests += 1;
    throw new Error("unexpected request");
  });
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(TranslatedArticleContent, {
          apiUrl: "/api",
          html: '<pre><code>const answer = 42;</code></pre>',
          text: "legacy text must not be translated",
          translationNeeded: true
        })
      );
    });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    assert.equal(requests, 0);
    assert.match(dom.container.textContent, /const answer = 42/);
    assert.doesNotMatch(dom.container.textContent, /legacy text must not be translated/);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("rich HTML replaces a legacy full-text translation with block translations", async () => {
  let requestBody;
  const translatedBlocks = [
    { id: "block-aaaaaaaaaaaaaaaa", text: "译文 1" },
    { id: "block-bbbbbbbbbbbbbbbb", text: "译文 2" },
    { id: "block-cccccccccccccccc", text: "译文 3" },
    { id: "block-eeeeeeeeeeeeeeee", text: "译文 4" }
  ];
  const dom = installDom(async (input, init) => {
    assert.equal(String(input), "/api/translations");
    requestBody = JSON.parse(String(init?.body));
    return Response.json({
      status: "ready",
      translation: translatedBlocks.map((block) => block.text).join("\n"),
      blocks: translatedBlocks,
      model_version: "test",
      updated_at: null
    });
  });
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(TranslatedArticleContent, {
          apiUrl: "/api",
          sourceId: 42,
          html: richHtml,
          initialTranslation: "旧的整篇译文",
          text: "OpenAI released a new report.",
          translationNeeded: true
        })
      );
    });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    assert.deepEqual(requestBody, {
      source_id: 42,
      blocks: [
        { id: "block-aaaaaaaaaaaaaaaa", text: "OpenAI released a new report." },
        { id: "block-bbbbbbbbbbbbbbbb", text: "Quarterly results" },
        { id: "block-cccccccccccccccc", text: "Revenue grew" },
        { id: "block-eeeeeeeeeeeeeeee", text: "Read today" }
      ]
    });
    assert.equal(dom.container.querySelectorAll(".rich-block-translation").length, 4);
    assert.doesNotMatch(dom.container.textContent, /旧的整篇译文/);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("a bad block mapping falls back to plain translated text without duplicating images", async () => {
  const dom = installDom(async (input, init) => {
    assert.equal(String(input), "/api/translations");
    assert.deepEqual(JSON.parse(String(init?.body)), {
      blocks: [
        { id: "block-aaaaaaaaaaaaaaaa", text: "OpenAI released a new report." },
        { id: "block-bbbbbbbbbbbbbbbb", text: "Quarterly results" },
        { id: "block-cccccccccccccccc", text: "Revenue grew" },
        { id: "block-eeeeeeeeeeeeeeee", text: "Read today" }
      ]
    });
    return Response.json({
      status: "ready",
      translation: "映射异常时显示纯文本译文。",
      blocks: [{ id: "block-wrongwrongwrong", text: "错误映射" }],
      model_version: "test",
      updated_at: null
    });
  });
  const root = createRoot(dom.container);
  try {
    await act(async () => {
      root.render(
        React.createElement(TranslatedArticleContent, {
          apiUrl: "/api",
          html: richHtml,
          text: "OpenAI released a new report.",
          translationNeeded: true
        })
      );
    });
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
    });

    assert.equal(dom.container.querySelectorAll("img").length, 1);
    assert.equal(dom.container.querySelectorAll("table").length, 1);
    assert.match(dom.container.textContent, /映射异常时显示纯文本译文/);
    assert.doesNotMatch(dom.container.textContent, /错误映射/);
  } finally {
    await act(async () => root.unmount());
    dom.restore();
  }
});

test("rich tables and code scroll locally instead of widening the page", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(
    css,
    /\.article-rich-content :is\(table, pre\)\s*\{[^}]*max-width:\s*100%;[^}]*overflow-x:\s*auto;/s
  );
  assert.match(
    css,
    /\.article-rich-content img\s*\{[^}]*max-width:\s*100%;[^}]*height:\s*auto;/s
  );
});

function installDom(fetchImpl) {
  const dom = new JSDOM("<!doctype html><html><body><div id='root'></div></body></html>", {
    url: "http://reader.test/"
  });
  const previous = new Map();
  for (const [key, value] of Object.entries({
    window: dom.window,
    document: dom.window.document,
    HTMLElement: dom.window.HTMLElement,
    HTMLImageElement: dom.window.HTMLImageElement,
    Node: dom.window.Node,
    NodeFilter: dom.window.NodeFilter,
    MutationObserver: dom.window.MutationObserver,
    navigator: dom.window.navigator,
    fetch: fetchImpl,
    IS_REACT_ACT_ENVIRONMENT: true
  })) {
    previous.set(key, Object.getOwnPropertyDescriptor(globalThis, key));
    Object.defineProperty(globalThis, key, {
      configurable: true,
      writable: true,
      value
    });
  }
  return {
    container: dom.window.document.querySelector("#root"),
    restore() {
      for (const [key, descriptor] of previous) {
        if (descriptor === undefined) delete globalThis[key];
        else Object.defineProperty(globalThis, key, descriptor);
      }
      dom.window.close();
    }
  };
}
