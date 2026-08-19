import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("navigation badges and settings inputs expose stable accessible names", async () => {
  const nav = await readFile(new URL("./nav-rail.tsx", import.meta.url), "utf8");
  const layout = await readFile(new URL("./layout.tsx", import.meta.url), "utf8");
  const page = await readFile(new URL("./page.tsx", import.meta.url), "utf8");
  const contextPanel = await readFile(new URL("./context-panel.tsx", import.meta.url), "utf8");
  const uninterested = await readFile(new URL("./uninterested/page.tsx", import.meta.url), "utf8");
  const assistant = await readFile(new URL("./assistant-chat-window.tsx", import.meta.url), "utf8");
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");
  assert.match(nav, /const displayedCount = count > 99 \? "99\+" : String\(count\)/);
  assert.match(nav, /aria-label=\{count > 0 \? `\$\{label\}，未读 \$\{displayedCount\}` : label\}/);
  assert.match(nav, /className="rail-badge" aria-hidden="true"/);
  assert.match(layout, /className="skip-link" href="#reader-main">跳到主要内容/);
  assert.match(page, /<main id="reader-main" tabIndex=\{-1\}/);
  assert.match(css, /\.skip-link:focus-visible \{\s*transform: none;/);
  assert.match(page, /OPML 文件[\s\S]*?<input name="file" type="file"/);
  assert.match(page, /<SubscriptionManager folders=\{folders\} sources=\{sources\} themeControl=\{<ThemeControl \/>\} \/>/);
  assert.match(contextPanel, /type="date" name="date" defaultValue=\{dateValue\} aria-label="报告日期"/);
  assert.match(contextPanel, /disabled=\{Boolean\(unavailableReason\)\}/);
  assert.match(contextPanel, /role="status">暂不可生成：\{unavailableReason\}/);
  assert.match(page, /aria-label="议题说明" name="description"/);
  assert.match(contextPanel, /aria-label="议题说明" name="description"/);
  assert.match(assistant, /aria-label="向 Assistant 提问" name="assistant_ask"/);
  assert.match(css, /\.subscription-search:focus-within \{[\s\S]*?border-color: var\(--accent\)/);
  assert.match(uninterested, /aria-label="搜索不感兴趣内容" name="q"/);
  assert.match(uninterested, /aria-label="按原因筛选" name="reason"/);
  assert.match(uninterested, /aria-label="按来源筛选" name="source_id"/);
});

test("historical report citations use immutable event revisions", async () => {
  const page = await readFile(new URL("./page.tsx", import.meta.url), "utf8");
  const history = await readFile(new URL("./events/[event_uid]/revisions/[revision_uid]/page.tsx", import.meta.url), "utf8");

  assert.match(page, /citation\.event_uid && citation\.event_revision_uid[\s\S]*?\/events\/\$\{encodeURIComponent\(citation\.event_uid\)\}\/revisions\//);
  assert.match(page, /\[\{citation\.citation_no \?\? index \+ 1\}\]/);
  assert.match(history, /报告生成时引用的不可变事件版本/);
  assert.match(history, /<ArticleContent html=\{evidence\.reading_html\} text=\{evidence\.content/);
});

test("layout preference offers automatic and compact modes without forced desktop", async () => {
  const control = await readFile(new URL("./layout-mode-control.tsx", import.meta.url), "utf8");
  const layout = await readFile(new URL("./layout.tsx", import.meta.url), "utf8");
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(control, /type LayoutMode = "auto" \| "compact"/);
  assert.match(control, /label: "紧凑\/单栏版式"/);
  assert.match(control, /if \(value === "mobile"\) return "compact"/);
  assert.doesNotMatch(control, /mode: "desktop"/);
  assert.match(layout, /savedLayoutMode === "compact" \|\| savedLayoutMode === "mobile"/);
  assert.doesNotMatch(css, /reader-force-desktop|force-mode-popover|force-mode-menu/);
  assert.match(css, /@media \(hover: none\), \(pointer: coarse\) \{[\s\S]*?\.layout-mode-control button \{\s*min-height: 44px;/);
  assert.match(css, /@media \(max-width: 1179px\)/);
  assert.doesNotMatch(css, /@media \(max-width: 1180px\)/);
});

test("coarse-pointer controls keep wide-tablet touch targets", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(
    css,
    /@media \(hover: none\), \(pointer: coarse\) \{\s*button,\s*summary,\s*\.toggle-time:not\(\.is-static\) \{\s*min-width: 44px !important;\s*min-height: 44px !important;\s*\}\s*button\.icon,\s*\.icon-link \{\s*min-width: 44px !important;\s*min-height: 44px !important;\s*width: 44px;\s*height: 44px;/
  );
  assert.match(css, /input:not\(\[type="checkbox"\]\):not\(\[type="radio"\]\):not\(\[type="hidden"\]\),\s*select,\s*textarea \{\s*min-height: 44px !important;/);
  assert.match(css, /@media \(max-width: 799px\) \{[\s\S]*?button,\s*summary,\s*\.toggle-time:not\(\.is-static\) \{\s*min-width: 44px !important;\s*min-height: 44px !important;/);
  assert.match(css, /@media \(max-width: 799px\) \{[\s\S]*?input:not\(\[type="checkbox"\]\):not\(\[type="radio"\]\):not\(\[type="hidden"\]\),\s*select,\s*textarea \{\s*min-height: 44px !important;/);
});

test("narrow two-column layouts keep status and list tools on separate rows", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(
    css,
    /@media \(min-width: 800px\) and \(max-width: 899px\) \{[\s\S]*?\.app-shell:not\(\.settings-mode\) \.list-header \{[\s\S]*?"status search"[\s\S]*?"tools tools"/
  );
});

test("status indicators expose labels through permitted semantics", async () => {
  const pipeline = await readFile(new URL("./pipeline-overview-panel.tsx", import.meta.url), "utf8");
  assert.match(pipeline, /className=\{`pipeline-status-dot[\s\S]*?role="img" aria-label=/);
});

test("translation settings fields expose stable browser form names", async () => {
  const translation = await readFile(new URL("./translation-settings-control.tsx", import.meta.url), "utf8");
  const synthesis = await readFile(new URL("./synthesis-settings-control.tsx", import.meta.url), "utf8");
  const generation = await readFile(new URL("./generation-control-panel.tsx", import.meta.url), "utf8");
  const subscriptions = await readFile(new URL("./subscription-manager.tsx", import.meta.url), "utf8");
  const filters = await readFile(new URL("./filter-rule-manager.tsx", import.meta.url), "utf8");
  for (const name of ["translation_provider", "translation_base_url", "translation_model", "translation_api_key"]) {
    assert.match(translation, new RegExp(`name="${name}"`));
  }
  for (const name of ["synthesis_provider", "synthesis_base_url", "synthesis_model", "synthesis_api_key"]) {
    assert.match(synthesis, new RegExp(`name="${name}"`));
  }
  for (const name of ["global_pause", "auto_run", "daily_budget_tokens", "input_estimator", "output_token_allowance", "day_timezone"]) {
    assert.match(generation, new RegExp(`name="${name}"`));
  }
  assert.match(subscriptions, /name="source_search"/);
  assert.match(subscriptions, /role="switch"/);
  assert.match(subscriptions, /className="subscription-add-source"/);
  assert.match(subscriptions, /name="feed_url"[\s\S]*?type="url"/);
  assert.match(subscriptions, /className="subscription-theme-control"/);
  for (const name of ["filter_source_scope", "filter_match_type", "filter_pattern"]) {
    assert.match(filters, new RegExp(`name="${name}"`));
  }
});

test("resizers expose their current value and semantic range", async () => {
  const source = await readFile(new URL("./list-pane-resizer.tsx", import.meta.url), "utf8");
  assert.match(source, /aria-valuemin=\{0\}/);
  assert.match(source, /aria-valuemax=\{100\}/);
  assert.match(source, /aria-valuenow=\{ariaValue\.now\}/);
  assert.match(source, /aria-valuetext=\{ariaValue\.text\}/);
});

test("wide browse media surfaces are not capped by the split-pane width", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");
  assert.match(css, /\.app-shell:not\(\.settings-mode\):not\(\.content-mode\):not\(\.browse-surface-mode\) \.list-pane\s*\{/);
});

test("mobile lists override the desktop split-pane width at equal specificity", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");
  assert.match(
    css,
    /@media \(max-width: 799px\)[\s\S]*?\.app-shell:not\(\.settings-mode\):not\(\.content-mode\):not\(\.browse-surface-mode\) \.list-pane\s*\{[^}]*width: 100%;[^}]*min-width: 0;[^}]*max-width: none;/s
  );
});

test("compact settings reserve space for the scrollable settings content", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(
    css,
    /@media \(max-width: 899px\) and \(min-width: 701px\)[\s\S]*?\.app-shell\.settings-mode\s*\{[^}]*grid-template-rows: clamp\(220px, 40dvh, 340px\) minmax\(0, 1fr\);/s
  );
  assert.match(
    css,
    /html\.reader-force-mobile \.app-shell\.settings-mode\s*\{[^}]*grid-template-rows: clamp\(220px, 40dvh, 340px\) minmax\(0, 1fr\);/s
  );
  assert.match(
    css,
    /@media \(max-width: 700px\)[\s\S]*?\.app-shell\.settings-mode\s*\{[^}]*grid-template-rows: clamp\(220px, 40dvh, 340px\) minmax\(0, 1fr\);/s
  );
  assert.match(
    css,
    /@media \(max-width: 899px\) and \(max-height: 500px\)[\s\S]*?\.app-shell\.settings-mode\s*\{[^}]*grid-template-rows: clamp\(112px, 38dvh, 160px\) minmax\(0, 1fr\);/s
  );
  assert.match(
    css,
    /@media \(max-height: 500px\)[\s\S]*?html\.reader-force-mobile \.app-shell\.settings-mode\s*\{[^}]*grid-template-rows: clamp\(112px, 38dvh, 160px\) minmax\(0, 1fr\);/s
  );
});

test("custom modal backdrops keep fake dialogs inside the dynamic viewport", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.match(
    css,
    /\.toolbar-modal-backdrop\s*\{[^}]*position: fixed;[^}]*inset: 0;[^}]*display: grid;[^}]*place-items: center;[^}]*overflow: auto;/s
  );
  assert.match(css, /\.toolbar-modal\s*\{[^}]*max-height: calc\(100dvh - 36px\);/s);
  assert.match(
    css,
    /\.filter-rule-modal\s*\{[^}]*width: min\(720px, calc\(100vw - 36px\)\);[^}]*max-height: min\(780px, calc\(100dvh - 36px\)\);/s
  );
  assert.match(css, /\.uninterested-modal\s*\{[^}]*width: min\(620px, calc\(100vw - 36px\)\);/s);
});

test("mobile toolbar scrolling does not clip the more-actions popover", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");
  assert.match(css, /\.pane\.detail \.custom-toolbar\s*\{[^}]*position: sticky;[^}]*overflow: visible;/s);
  assert.match(css, /html\.reader-force-mobile \.pane\.detail \.custom-toolbar\s*\{[^}]*overflow: visible;/s);
  assert.match(css, /\.custom-toolbar > \.toolbar\s*\{[^}]*overflow-x: auto;/s);
});

test("production accessibility colors and targets reuse semantic tokens", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");
  assert.doesNotMatch(css, /subscription[^}]*#f17e88/s);
  assert.match(css, /\.subscription-card-switch\s*\{[^}]*min-height: 44px/s);
  assert.match(css, /\.settings-block \.subscription-card-main\s*\{[^}]*display: grid;[^}]*grid-template-columns: minmax\(0, 1fr\)/s);
  assert.match(css, /\.subscription-card-switch::before\s*\{[^}]*border-radius: 999px[^}]*background: var\(--border\)/s);
  assert.match(css, /\.subscription-card-switch\[aria-checked="true"\]::before\s*\{[^}]*background: var\(--accent\)/s);
  assert.match(css, /\.source-detail-dialog button,\s*\.source-detail-dialog input,\s*\.source-detail-dialog select\s*\{[^}]*min-height: 44px/s);
  assert.match(css, /\.generation-control \.editor-toggle-row\s*\{[^}]*min-height: 44px/s);
  assert.match(css, /\.subscription-lane-card\.is-error\s*\{[^}]*border-left-color: var\(--danger\)/s);
  assert.match(css, /\.source-detail-dialog\s*\{[^}]*width: min\(760px, calc\(100vw - 48px\)\)/s);
  assert.match(css, /@media \(max-width: 1099px\)[\s\S]*?\.source-detail-dialog\s*\{[^}]*width: 100vw[^}]*height: 100dvh/s);
  for (const selector of ["subscription-workspace", "source-row-v2", "select-all", "source-link", "subscription-row-check", "subscription-source-link"]) {
    assert.doesNotMatch(css, new RegExp(`\\.${selector}`));
  }
  assert.match(css, /\.toggle-time\s*\{[^}]*min-height: 24px/s);
  assert.match(css, /\.about-doc-links a\s*\{[^}]*min-height: 44px/s);
  assert.match(css, /html\.reader-force-mobile summary\s*\{[^}]*min-width: 44px;[^}]*min-height: 44px/s);
  assert.match(css, /html\.reader-force-mobile input:not\(\[type="checkbox"\]\):not\(\[type="radio"\]\):not\(\[type="hidden"\]\),\s*html\.reader-force-mobile select,\s*html\.reader-force-mobile textarea\s*\{[^}]*min-height: 44px !important/s);
});

test("list-card timestamps remain interactive outside stretched row links", async () => {
  const clusterList = await readFile(new URL("./cluster-list.tsx", import.meta.url), "utf8");
  const clusterRow = await readFile(new URL("./cluster-row-link.tsx", import.meta.url), "utf8");
  const browseView = await readFile(new URL("./browse-view.tsx", import.meta.url), "utf8");
  const browseCard = await readFile(new URL("./browse-item-card.tsx", import.meta.url), "utf8");
  const timeText = await readFile(new URL("./time-text.tsx", import.meta.url), "utf8");

  assert.match(timeText, /interactive = true/);
  assert.match(timeText, /event\.stopPropagation\(\)/);
  assert.doesNotMatch(clusterList, /interactive=\{false\}/);
  assert.doesNotMatch(clusterRow, /interactive=\{false\}/);
  assert.doesNotMatch(browseView, /interactive=\{false\}/);
  assert.match(clusterRow, /className="stretched-row-link"/);
  assert.match(browseView, /from "\.\/browse-item-card"/);
  for (const primitive of ["BrowseListRow", "BrowseImageCard", "BrowseSocialCard", "BrowseVideoCard"]) assert.match(browseCard, new RegExp(`export function ${primitive}`));
  assert.match(browseCard, /className="stretched-row-link"/);
  assert.match(browseCard, /<TimeText interactive=\{!staticPreview\}/);
  assert.match(clusterRow, /className="item-title">\s*\{readStatus === "unread"[\s\S]*?className="unread-dot"[\s\S]*?<a className="stretched-row-link"/);
  assert.match(browseCard, /className="item-title">\s*\{!staticPreview && item\.read_status === "unread"[\s\S]*?<BrowseTitle/);
});

test("unread markers never participate in title layout", async () => {
  const css = await readFile(new URL("./globals.css", import.meta.url), "utf8");

  assert.doesNotMatch(css, /\.item-title:has\(\.unread-dot\)/);
  assert.match(
    css,
    /\.item-title > \.unread-dot\s*\{[^}]*left: -11px;[^}]*top: 0\.71em;[^}]*transform: translateY\(-50%\);/s
  );
});

test("secondary navigation and filters expose their selected state", async () => {
  const contextPanel = await readFile(new URL("./context-panel.tsx", import.meta.url), "utf8");
  const subscriptions = await readFile(new URL("./subscription-manager.tsx", import.meta.url), "utf8");

  assert.equal((contextPanel.match(/aria-current=/g) || []).length, 7);
  assert.match(contextPanel, /aria-current=\{active \? "page" : undefined\}/);
  assert.match(contextPanel, /aria-current=\{currentPeriod === value \? "page" : undefined\}/);
  assert.match(contextPanel, /aria-current=\{currentSection === section \? "page" : undefined\}/);
  assert.match(subscriptions, /aria-pressed=\{attentionOnly\}/);
  assert.match(subscriptions, /aria-pressed=\{currentType === mediaType\}/);
});

test("source error badges never expose raw fetch diagnostics", async () => {
  const contextPanel = await readFile(new URL("./context-panel.tsx", import.meta.url), "utf8");

  assert.match(contextPanel, /const error = friendlyFetchError\(source\.last_error\)/);
  assert.match(contextPanel, /title=\{error\} aria-label=\{`源抓取错误：\$\{error\}`\}/);
  assert.doesNotMatch(contextPanel, /title=\{source\.last_error\}/);
});

test("custom settings dialogs support complete keyboard focus lifecycle", async () => {
  const filters = await readFile(new URL("./filter-rule-manager.tsx", import.meta.url), "utf8");

  assert.match(filters, /event\.key === "Escape" && !pending[\s\S]*?closeEditor\(\)/);
  assert.match(filters, /editorReturnFocusRef\.current\?\.focus\(\)/);
  assert.match(filters, /filterFocusableElements\(editorDialogRef\.current\)/);
});
