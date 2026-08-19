import assert from "node:assert/strict";
import test from "node:test";

import { GET } from "./health/route.ts";

test("web health route is independent of the Reader page and API", async () => {
  const response = GET();

  assert.equal(response.status, 200);
  assert.equal(await response.text(), "ok");
});
