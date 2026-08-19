"use client";

import { FormEvent, useState } from "react";

import { useActionDialog } from "./action-dialog";
import { userFacingErrorMessage } from "./lib/api";

type SynthesisSettings = {
  synthesis_provider: string;
  synthesis_remote_base_url: string;
  synthesis_remote_model: string;
  synthesis_remote_api_key_configured: boolean;
};

export default function SynthesisSettingsControl({ apiUrl, settings }: { apiUrl: string; settings: SynthesisSettings }) {
  const actionDialog = useActionDialog();
  const [provider, setProvider] = useState(settings.synthesis_provider);
  const [baseUrl, setBaseUrl] = useState(settings.synthesis_remote_base_url);
  const [model, setModel] = useState(settings.synthesis_remote_model);
  const [apiKey, setApiKey] = useState("");
  const [keyConfigured, setKeyConfigured] = useState(settings.synthesis_remote_api_key_configured);
  const [saving, setSaving] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await patchSettings({
      synthesis_provider: provider,
      synthesis_remote_base_url: baseUrl.trim(),
      synthesis_remote_model: model.trim(),
      synthesis_remote_api_key: apiKey.trim()
    });
  }

  async function clearKey() {
    if (!await actionDialog.confirm({
      title: "清除合成稿云端密钥",
      message: "这会永久清除已保存的远端合成 API Key；清除后无法恢复。",
      confirmLabel: "清除密钥",
      danger: true
    })) return;
    await patchSettings({ clear_synthesis_remote_api_key: true });
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
      let body: SynthesisSettings & { detail?: unknown };
      try {
        body = (await response.json()) as SynthesisSettings & { detail?: unknown };
      } catch {
        throw new Error("合成稿设置保存失败");
      }
      if (!response.ok) throw new Error(typeof body.detail === "string" ? body.detail : `保存失败（${response.status}）`);
      setBaseUrl(body.synthesis_remote_base_url);
      setProvider(body.synthesis_provider);
      setModel(body.synthesis_remote_model);
      setKeyConfigured(body.synthesis_remote_api_key_configured);
      setApiKey("");
      setMessage("合成稿设置已保存");
    } catch (reason) {
      setError(userFacingErrorMessage(reason, "合成稿设置保存失败"));
    } finally {
      setSaving(false);
    }
  }

  return (
    <>
      <form className="form-stack" onSubmit={save}>
        <label>
          合成稿提供方
          <select name="synthesis_provider" value={provider} onChange={(event) => setProvider(event.target.value)}>
            <option value="local">本地模型</option>
            <option value="openai_compatible">远端 OpenAI 兼容接口</option>
          </select>
        </label>
        <label>
          远端合成地址
          <input name="synthesis_base_url" value={baseUrl} onChange={(event) => setBaseUrl(event.target.value)} placeholder="https://api.example.com/v1" />
        </label>
        <label>
          远端合成模型
          <input name="synthesis_model" value={model} onChange={(event) => setModel(event.target.value)} placeholder="model-name" />
        </label>
        <label>
          远端合成 API Key
          <input
            name="synthesis_api_key"
            value={apiKey}
            onChange={(event) => setApiKey(event.target.value)}
            type="password"
            autoComplete="new-password"
            placeholder={keyConfigured ? "已保存；留空保持不变" : "仅写入，不会回显"}
          />
        </label>
        <div className="source-meta">云端密钥：{keyConfigured ? "已配置（不会回显）" : "未配置"}</div>
        {error ? <div className="error-line" role="alert">{error}</div> : null}
        {message ? <div className="status-line success-line" role="status">{message}</div> : null}
        <button type="submit" disabled={saving}>{saving ? "保存中..." : "保存合成稿设置"}</button>
      </form>
      {keyConfigured ? (
        <div className="settings-action-row">
          <button type="button" disabled={saving} onClick={clearKey}>清除云端合成密钥</button>
        </div>
      ) : null}
      {actionDialog.dialog}
    </>
  );
}
