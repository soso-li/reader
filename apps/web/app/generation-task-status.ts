export type GenerationTaskStatus =
  | "blocked"
  | "pending"
  | "running"
  | "failed"
  | "apply_pending"
  | "apply_failed"
  | "stale_result"
  | "complete"
  | "canceled";

export type EventGenerationTaskStatus = "idle" | GenerationTaskStatus;

type SynthesisTaskMessageContext = "missing" | "unreviewed" | "stale";

function byContext(
  context: SynthesisTaskMessageContext,
  messages: Record<SynthesisTaskMessageContext, string>
) {
  return messages[context];
}

export function synthesisTaskMessage(
  status: EventGenerationTaskStatus,
  context: SynthesisTaskMessageContext,
  blockedMessage: string
) {
  switch (status) {
    case "pending":
    case "running":
      return byContext(context, {
        missing: "合成任务已进入队列。来源原文仍可随时阅读。",
        unreviewed: "正在审阅新证据，并会按需生成更新稿；旧合成稿和当前来源仍可阅读。",
        stale: "正在生成更新稿；旧合成稿和当前来源仍可阅读。"
      });
    case "blocked":
      return context === "missing" ? `${blockedMessage} 来源原文仍可阅读。` : blockedMessage;
    case "apply_pending":
      return context === "missing"
        ? "结果已生成，等待应用。请在设置 → 任务分配中重新应用已有结果。"
        : "结果已生成，等待应用；请在设置 → 任务分配中重新应用已有结果。";
    case "apply_failed":
      return context === "missing"
        ? "结果已生成，但应用失败。请在设置 → 任务分配中重试应用。"
        : "结果已生成，但更新应用失败；请在设置 → 任务分配中重试应用。";
    case "stale_result":
      return context === "missing"
        ? "旧结果已过期，不能应用到当前证据。请重新生成。"
        : "旧结果已过期，不能应用到当前证据；可重新生成更新稿。";
    case "failed":
      return byContext(context, {
        missing: "上次合成失败，没有发布不完整结果。你可以重试或切回来源。",
        unreviewed: "审阅或更新失败；旧合成稿和当前来源仍可阅读，可重试更新。",
        stale: "更新失败；旧合成稿仍是当前最佳版本，可重试或切换到来源。"
      });
    case "canceled":
      return byContext(context, {
        missing: "合成任务已取消；可在设置 → 任务分配中明确重试，来源原文仍可阅读。",
        unreviewed: "审阅或更新已取消；旧合成稿和当前来源仍可阅读，可在任务分配中明确重试。",
        stale: "更新已取消；旧合成稿仍是当前最佳版本，可在任务分配中明确重试。"
      });
    default:
      return byContext(context, {
        missing: "尚未生成合成稿。生成只会在你明确点击后调用所选模型。",
        unreviewed: "当前合成稿仍基于上述快照；切换到来源可查看最新证据。",
        stale: "当前合成稿尚未纳入已确认的实质变化，可更新或切换到来源。"
      });
  }
}
