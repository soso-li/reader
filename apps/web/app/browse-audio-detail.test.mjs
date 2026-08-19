import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { embeddedVideoUrl } from "./browse-view.tsx";

test("podcast details render an in-page audio player with an external fallback", async () => {
  const source = await readFile(new URL("./browse-view.tsx", import.meta.url), "utf8");
  assert.match(source, /if \(media === "podcast"\) return renderAudioDetail\(item\)/);
  assert.match(source, /<audio controls preload="metadata" src=\{audioUrl\}>/);
  assert.match(source, /className="browse-media-link"[\s\S]*?打开音频/);
  assert.match(source, /media === "podcast" \? "browse-media-detail browse-audio-detail"/);
});

test("opening any unread browse item records it as seen", async () => {
  const source = await readFile(new URL("./browse-view.tsx", import.meta.url), "utf8");
  assert.match(source, /if \(item\.read_status === "unread"\) markSelectionSeen\(item, "detail"\)/);
  assert.match(source, /markSelectionSeen\(item, "detail"\)/);
});

test("video details embed only supported YouTube and Bilibili URLs", async () => {
  assert.equal(
    embeddedVideoUrl({ url: "https://www.youtube.com/watch?v=1K_O8-RyZ_c", media_url: "" }),
    "https://www.youtube-nocookie.com/embed/1K_O8-RyZ_c"
  );
  assert.equal(
    embeddedVideoUrl({ url: "https://www.bilibili.com/video/BV1UXAJzbEQT", media_url: "" }),
    "https://player.bilibili.com/player.html?bvid=BV1UXAJzbEQT&autoplay=0"
  );
  assert.equal(
    embeddedVideoUrl({ url: "https://evil.example/watch?v=1K_O8-RyZ_c", media_url: "javascript:alert(1)" }),
    ""
  );
  const source = await readFile(new URL("./browse-view.tsx", import.meta.url), "utf8");
  assert.match(source, /<iframe[\s\S]*?allowFullScreen/);
});
