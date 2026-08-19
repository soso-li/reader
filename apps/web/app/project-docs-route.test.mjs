import assert from "node:assert/strict";
import test from "node:test";

import { GET } from "./docs/[document]/route.ts";

test("about-page project documents are served from the synced repository", async () => {
  const response = await GET(new Request("http://reader/docs/PRODUCT.md"), {
    params: Promise.resolve({ document: "PRODUCT.md" })
  });

  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/markdown/);
  assert.match(await response.text(), /Reader/);

  const missing = await GET(new Request("http://reader/docs/secret.txt"), {
    params: Promise.resolve({ document: "secret.txt" })
  });
  assert.equal(missing.status, 404);
});
