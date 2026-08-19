"use client";

import { FormEvent, useState } from "react";

import { useActionDialog } from "./action-dialog";
import { userFacingErrorMessage } from "./lib/api";

type TranslationSettings = {
  translation_provider: string;
  translation_base_url: string;
  translation_local_base_url: string;
  translation_local_model: string;
  translation_cloud_base_url: string;
  translation_cloud_model: string;
  translation_api_key_configured: boolean;
  translation_endpoint: string;
  translation_model: string;
};

type Provider = "local" | "openai_compatible";

export default function TranslationSettingsControl({ apiUrl, settings }: { apiUrl: string; settings: TranslationSettings }) {
  const actionDialog = useActionDialog();
  const initialProvider = providerValue(settings.translation_provider);
  const [provider, setProvider] = useState<Provider>(initialProvider);
  const [localBaseUrl, setLocalBaseUrl] = useState(settings.translation_local_base_url || (initialProvider === "local" ? settings.translation_base_url : ""));
  const [localModel, setLocalModel] = useState(settings.translation_local_model || (initialProvider === "local" ? settings.translation_model : ""));
  const [cloudBaseUrl, setCloudBaseUrl] = useState(settings.translation_cloud_base_url || (initialProvider === "openai_compatible" ? settings.translation_base_url : ""));
  const [cloudModel, setCloudModel] = useState(settings.translation_cloud_model || (initialProvider === "openai_compatible" ? settings.translation_model : ""));
  const [apiKey, setApiKey] = useState("");
  const [keyConfigured, setKeyConfigured] = useState(settings.translation_api_key_configured);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  const baseUrl = provider === "local" ? localBaseUrl : cloudBaseUrl;
  const model = provider === "local" ? localModel : cloudModel;
  const setBaseUrl = provider === "local" ? setLocalBaseUrl : setCloudBaseUrl;
  const setModel = provider === "local" ? setLocalModel : setCloudModel;

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await patchSettings({
      translation_provider: provider,
      translation_base_url: baseUrl.trim(),
      translation_model: model.trim(),
      translation_api_key: apiKey.trim()
    });
  }

  async function clearCloudKey() {
    if (!await actionDialog.confirm({
      title: "清除翻译云端密钥",
      message: "这会切回本地翻译并永久清除已保存的云端 API Key；清除后无法恢复。",
      confirmLabel: "清除密钥",
      danger: true
    })) return;
    await patchSettings({
      translation_provider: "local",
      translation_base_url: localBaseUrl.trim(),
      translation_model: localModel.trim(),
      clear_translation_api_key: true
    });
  }

  async function patchSettings(payload: Record<string, string | boolean>) {
    setSaving(true);
    setMessage("");
    setError("");
    try {
      const response = await fetch(`${apiUrl.replace(/\/$/, "")}/ai/settings`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      let body: TranslationSettings & { detail?: unknown };
      try {
        body = (await response.json()) as TranslationSettings & { detail?: unknown };
      } catch {
        throw new Error("翻译设置保存失败");
      }
      if (!response.ok) throw new Error(typeof body.detail === "string" ? body.detail : `保存失败（${response.status}）`);
      applySavedSettings(body);
      setApiKey("");
      setMessage("翻译设置已保存");
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "翻译设置保存失败"));
    } finally {
      setSaving(false);
    }
  }

  function applySavedSettings(saved: TranslationSettings) {
    const savedProvider = providerValue(saved.translation_provider);
    setProvider(savedProvider);
    setLocalBaseUrl(saved.translation_local_base_url);
    setLocalModel(saved.translation_local_model);
    setCloudBaseUrl(saved.translation_cloud_base_url);
    setCloudModel(saved.translation_cloud_model);
    setKeyConfigured(saved.translation_api_key_configured);
  }

  return (
    <>
      <form className="form-stack" onSubmit={save}>
        <label>
          翻译提供方
          <select name="translation_provider" value={provider} onChange={(event) => setProvider(providerValue(event.target.value))}>
            <option value="local">本地 LM Studio</option>
            <option value="openai_compatible">云端 OpenAI-compatible API</option>
          </select>
        </label>
        <label>
          翻译 API 地址
          <input name="translation_base_url" value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} placeholder={provider === "local" ? "http://127.0.0.1:1234" : "https://api.example.com/v1"} />
        </label>
        <label>
          翻译模型
          <input name="translation_model" value={model} onChange={(event) => setModel(event.target.value)} placeholder={provider === "local" ? "hy-mt2-1.8b" : "云端模型名称"} />
        </label>
        <label>
          云端 API Key
          <input
            name="translation_api_key"
            value={apiKey}
            onChange={(event) => setApiKey(event.target.value)}
            type="password"
            autoComplete="new-password"
            placeholder={keyConfigured ? "已配置；留空保持不变" : "仅云端翻译需要"}
          />
        </label>
        <div className="source-meta">云端密钥：{keyConfigured ? "已配置（不会回显）" : "未配置"}</div>
        <div className="source-meta">Translation Endpoint: {translationEndpoint(provider, baseUrl) || "未配置"}</div>
        {error ? <div className="error-line" role="alert">{error}</div> : null}
        {message ? <div className="status-line success-line" role="status">{message}</div> : null}
        <button type="submit" disabled={saving}>{saving ? "保存中..." : "保存翻译设置"}</button>
      </form>
      {keyConfigured ? (
        <div className="settings-action-row">
          <button type="button" disabled={saving} onClick={clearCloudKey}>切回本地并清除云端密钥</button>
        </div>
      ) : null}
      {actionDialog.dialog}
    </>
  );
}

function providerValue(value: string): Provider {
  return value === "openai_compatible" ? "openai_compatible" : "local";
}

function translationEndpoint(provider: Provider, baseUrl: string) {
  const base = baseUrl.trim().replace(/\/$/, "");
  if (!base) return "";
  if (provider === "local") return `${base}/api/v1/chat`;
  return base.endsWith("/v1") ? `${base}/chat/completions` : `${base}/v1/chat/completions`;
}
