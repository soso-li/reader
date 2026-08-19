import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { parseReaderListNavigation } from "./reader-list-navigation.ts";

test("reading scope links preserve client filter history semantics", () => {
  assert.deepEqual(
    parseReaderListNavigation("/?view=clusters&folder_id=4&source_id=8&filter=unread&q=IT%E4%B9%8B%E5%AE%B6"),
    {
      filter: "unread",
      folderId: 4,
      href: "/?view=clusters&folder_id=4&source_id=8&filter=unread&q=IT%E4%B9%8B%E5%AE%B6",
      query: "IT之家",
      sourceId: 8,
      view: "clusters"
    }
  );
  assert.equal(parseReaderListNavigation("/")?.view, "clusters");
});

test("client list owners invalidate stale pagination and detail requests", async () => {
  const browse = await readFile(new URL("./browse-view.tsx", import.meta.url), "utf8");
  const browseCard = await readFile(new URL("./browse-item-card.tsx", import.meta.url), "utf8");
  const clusterList = await readFile(new URL("./cluster-list.tsx", import.meta.url), "utf8");
  const clusterRow = await readFile(new URL("./cluster-row-link.tsx", import.meta.url), "utf8");
  const clusterView = await readFile(new URL("./cluster-view.tsx", import.meta.url), "utf8");
  const contextPanel = await readFile(new URL("./context-panel.tsx", import.meta.url), "utf8");

  assert.match(browse, /pageAbortController\.current\?\.abort\(\)/);
  assert.match(browse, /pageRequestId\.current/);
  assert.match(browse, /invalidateDetailRequest\(/);
  assert.doesNotMatch(browse, /dispatchEvent\(new window\.PopStateEvent\("popstate"\)\)/);
  assert.match(browse, /requestId === listRequestId\.current[\s\S]{0,160}setClientListError\("列表加载失败，请重试。"\)/);
  assert.match(clusterList, /pageAbortController\.current\?\.abort\(\)/);
  assert.match(clusterList, /pageRequestId\.current/);
  assert.doesNotMatch(clusterList, /useEffect\(\(\) => \{\s*onRowsChange\?\.\(rows\)/);
  assert.doesNotMatch(browse, /\.filter\(\(item\) => matchesListFilter/);
  assert.match(browse, /\|\| serverDetailError/);
  assert.match(browse, /阅读状态保存失败，请重试。/);
  assert.match(browse, /listFilterQuery\(clientScope\.filter\)/);
  assert.match(clusterList, /listFilterQuery\(scope\.filter\)/);
  assert.doesNotMatch(clusterView, /dispatchEvent\(new window\.PopStateEvent\("popstate"\)\)/);
  assert.match(clusterView, /requestId === listRequestId\.current[\s\S]{0,160}setClientListError\("列表加载失败，请重试。"\)/);
  assert.match(clusterView, /事件状态保存失败，请重试。/);
  assert.match(clusterView, /document\.querySelector\('dialog\[open\], \[role="dialog"\]\[aria-modal="true"\]'\)/);
  assert.match(clusterView, /event\.composedPath\(\)\.some\(\(target\) => target instanceof HTMLDialogElement\)/);
  assert.match(clusterView, /selectedId && \(serverDetailError \|\| clientDetailError\)/);
  assert.match(clusterView, /source_id: selectedSourceItem\.source_id, filter: "unread", pane: "list"/);
  assert.match(clusterView, /dispatchReaderListNavigation\(selectedSourceHref\)/);
  assert.doesNotMatch(clusterView, /urlSyncTimer/);
  assert.match(contextPanel, /READER_LIST_NAVIGATION_COMMITTED_EVENT/);
  assert.match(contextPanel, /READER_UNREAD_COUNT_CHANGED_EVENT/);
  assert.match(contextPanel, /applyAllUnreadCountDelta\(current, delta\)/);
  assert.match(contextPanel, /\/sources\/navigation/);
  assert.doesNotMatch(contextPanel, /addEventListener\("popstate"/);
  assert.match(contextPanel, /\{filteredOnly \? null : <CountBadges unread=\{allUnreadCount\(sources\)\}/);
  assert.match(contextPanel, /<UnreadDot show=\{!filteredOnly && source\.unread_count > 0\}/);
  assert.match(contextPanel, /<SourceBadges source=\{source\} showCounts=\{!filteredOnly\} \/>/);
  assert.match(clusterView, /if \(updateHistory\) \{\s*window\.history\.pushState[\s\S]*?\}\s*dispatchReaderListNavigationCommitted\(href\)/);
  assert.match(browse, /if \(updateHistory\) \{\s*window\.history\.pushState[\s\S]*?\}\s*dispatchReaderListNavigationCommitted\(href\)/);
  assert.match(
    clusterView,
    /function cancelPendingListNavigation\(\)[\s\S]*?listAbortController\.current\?\.abort\(\)[\s\S]*?listRequestId\.current \+= 1[\s\S]*?setListPending\(false\)/
  );
  assert.match(clusterView, /function openCluster[\s\S]*?\{\s*cancelPendingListNavigation\(\);/);
  assert.match(
    browse,
    /function cancelPendingListNavigation\(\)[\s\S]*?listAbortController\.current\?\.abort\(\)[\s\S]*?listRequestId\.current \+= 1[\s\S]*?setListPending\(false\)/
  );
  assert.match(browse, /function openItem[\s\S]*?\{\s*cancelPendingListNavigation\(\);/);
  assert.match(browse, /\/items\/\$\{itemId\}[\s\S]{0,160}priority: "high"/);
  assert.match(clusterView, /\/clusters\/\$\{selectedId\}[\s\S]{0,160}priority: "high"/);
  assert.match(browseCard, /fetchPriority="low"/);
  assert.match(clusterRow, /fetchPriority="low"/);
  assert.doesNotMatch(
    contextPanel,
    /const navigateReading[\s\S]*?setActiveFolder\(navigation\.folderId\)[\s\S]*?dispatchReaderListNavigation\(href\)/
  );
});
