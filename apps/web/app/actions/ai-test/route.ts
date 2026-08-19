import { NextRequest, NextResponse } from "next/server";

import { apiFetch } from "../../lib/api";
import { apiErrorMessage, backToSettings } from "../shared";

type ModelType = "llm" | "translation" | "embedding";
type AIChatOut = { endpoint: string; model: string; provider: string; result: unknown };

const SAMPLES: Record<ModelType, { input: string; label: string; system_prompt: string }> = {
  llm: {
    input: "Return a short JSON object with status ok.",
    label: "LLM",
    system_prompt: "You are a connection test. Keep the response short."
  },
  translation: {
    input: "Translate this sentence into Chinese: Inbox Assistant is ready.",
    label: "翻译",
    system_prompt: "You are a translation test. Return only the translated sentence."
  },
  embedding: {
    input: "Inbox Assistant embedding connection test.",
    label: "Embedding",
    system_prompt: ""
  }
};

export async function POST(request: NextRequest) {
  const form = await request.formData();
  const modelType = modelTypeParam(form.get("model_type"));
  const sample = SAMPLES[modelType];
  const target = new URL(backToSettings(request), request.url);
  try {
    const result = await apiFetch<AIChatOut>("/ai/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        input: sample.input,
        model_type: modelType,
        system_prompt: sample.system_prompt
      })
    });
    target.searchParams.set("action_result", "ok");
    target.searchParams.set("action_message", `${sample.label} 测试成功：${result.provider} · ${result.model} · ${result.endpoint}`);
  } catch (error) {
    target.searchParams.set("action_result", "error");
    target.searchParams.set("action_message", apiErrorMessage(error, `${sample.label} 测试失败`));
  }
  return NextResponse.redirect(target, 303);
}

function modelTypeParam(value: FormDataEntryValue | null): ModelType {
  return value === "translation" || value === "embedding" ? value : "llm";
}
