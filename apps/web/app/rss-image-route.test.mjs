import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import sharp from "sharp";

import { GET } from "./images/rss/route.ts";

test("RSS image failures stay same-origin so the client can show its placeholder", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => new Response("missing", { status: 404 });
    const response = await GET(new Request("http://reader.test/images/rss?src=https%3A%2F%2F93.184.216.34%2Fmissing.jpg"));
    assert.equal(response.status, 502);
    assert.equal(response.headers.get("location"), null);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("RSS image proxy delegates untrusted URLs to the pinned API downloader", async () => {
  const originalFetch = globalThis.fetch;
  const originalToken = process.env.READER_API_TOKEN;
  let fetchedUrl = "";
  let fetchedToken = "";
  let fetchedSource = "";
  try {
    process.env.READER_API_TOKEN = "reader-test-token";
    globalThis.fetch = async (input, init) => {
      fetchedUrl = String(input);
      const headers = new Headers(init?.headers);
      fetchedToken = headers.get("X-Reader-API-Token") || "";
      fetchedSource = headers.get("X-Reader-Image-Source") || "";
      return new Response(new Uint8Array([1]), { headers: { "content-type": "image/png" } });
    };
    const src = "http://127.0.0.1:3000/private.png";
    const response = await GET(new Request(`http://reader.test/images/rss?src=${encodeURIComponent(src)}`));
    const key = createHash("sha256").update(src).digest("hex");
    assert.equal(response.status, 200);
    assert.match(fetchedUrl, new RegExp(`/images/article/${key}$`));
    assert.equal(new URL(fetchedUrl).search, "");
    assert.equal(fetchedToken, "reader-test-token");
    assert.equal(fetchedSource, src);
    assert.equal(fetchedUrl.startsWith("http://127.0.0.1:3000/"), false);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalToken === undefined) delete process.env.READER_API_TOKEN;
    else process.env.READER_API_TOKEN = originalToken;
  }
});

test("RSS image proxy propagates cancellation to the API downloader", async () => {
  const originalFetch = globalThis.fetch;
  const controller = new AbortController();
  controller.abort();
  try {
    globalThis.fetch = async (_input, init) => {
      assert.equal(init?.signal?.aborted, true);
      throw new DOMException("aborted", "AbortError");
    };
    const request = new Request(
      "http://reader.test/images/rss?src=https%3A%2F%2F93.184.216.34%2Fslow.jpg",
      { signal: controller.signal }
    );
    const response = await GET(request);
    assert.equal(response.status, 502);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("RSS image proxy stops oversized bodies at its byte limit", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => new Response(new Uint8Array(12 * 1024 * 1024 + 1), { headers: { "content-type": "image/png" } });
    const response = await GET(new Request("http://reader.test/images/rss?src=https%3A%2F%2F93.184.216.34%2Flarge.png"));
    assert.equal(response.status, 413);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("RSS image proxy accepts octet-stream only when it contains a supported image", async () => {
  const originalFetch = globalThis.fetch;
  try {
    const webp = await sharp({
      create: { width: 2, height: 2, channels: 3, background: "#000000" }
    }).webp().toBuffer();
    globalThis.fetch = async () => new Response(webp, { headers: { "content-type": "application/octet-stream" } });
    const imageResponse = await GET(new Request("http://reader.test/images/rss?src=https%3A%2F%2F93.184.216.34%2Fimage"));
    assert.equal(imageResponse.status, 200);
    assert.equal(imageResponse.headers.get("content-type"), "image/webp");

    globalThis.fetch = async () => new Response("not an image", { headers: { "content-type": "application/octet-stream" } });
    const invalidResponse = await GET(new Request("http://reader.test/images/rss?src=https%3A%2F%2F93.184.216.34%2Ffile"));
    assert.equal(invalidResponse.status, 502);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("RSS image proxy rejects active SVG content", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = async () => new Response("<svg><script>alert(1)</script></svg>", { headers: { "content-type": "image/svg+xml" } });
    const response = await GET(new Request("http://reader.test/images/rss?src=https%3A%2F%2F93.184.216.34%2Factive.svg"));
    assert.equal(response.status, 502);
  } finally {
    globalThis.fetch = originalFetch;
  }
});
