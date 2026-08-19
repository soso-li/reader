import assert from "node:assert/strict";
import test from "node:test";

import { userFacingErrorMessage } from "./lib/api.ts";

test("cloud translation diagnostics stay provider-specific", () => {
  const message = "云端翻译服务不可用，请检查地址、模型和密钥";

  assert.equal(userFacingErrorMessage(message, "翻译测试失败"), message);
});

test("raw local model connection errors keep the existing friendly message", () => {
  assert.equal(
    userFacingErrorMessage("LLM 服务不可用: <urlopen error Connection refused>", "LLM 测试失败"),
    "本地模型服务未连接，请检查 LM Studio"
  );
});
