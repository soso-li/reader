import assert from "node:assert/strict";
import test from "node:test";

import { synthesisTaskMessage } from "./generation-task-status.ts";

const blockedMessage = "生成任务受阻，请调整设置。";

test("centralizes event synthesis task messages for every presentation context", () => {
  assert.equal(
    synthesisTaskMessage("apply_pending", "missing", blockedMessage),
    "结果已生成，等待应用。请在设置 → 任务分配中重新应用已有结果。"
  );
  assert.equal(
    synthesisTaskMessage("apply_pending", "unreviewed", blockedMessage),
    "结果已生成，等待应用；请在设置 → 任务分配中重新应用已有结果。"
  );
  assert.equal(
    synthesisTaskMessage("apply_failed", "stale", blockedMessage),
    "结果已生成，但更新应用失败；请在设置 → 任务分配中重试应用。"
  );
  assert.equal(
    synthesisTaskMessage("blocked", "missing", blockedMessage),
    `${blockedMessage} 来源原文仍可阅读。`
  );
  assert.equal(
    synthesisTaskMessage("blocked", "unreviewed", blockedMessage),
    blockedMessage
  );
  assert.equal(
    synthesisTaskMessage("running", "stale", blockedMessage),
    "正在生成更新稿；旧合成稿和当前来源仍可阅读。"
  );
  assert.equal(
    synthesisTaskMessage("failed", "unreviewed", blockedMessage),
    "审阅或更新失败；旧合成稿和当前来源仍可阅读，可重试更新。"
  );
  assert.equal(
    synthesisTaskMessage("idle", "missing", blockedMessage),
    "尚未生成合成稿。生成只会在你明确点击后调用所选模型。"
  );
});
