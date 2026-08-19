"use client";

import { useRef, useState } from "react";
import { RefreshCw } from "lucide-react";

export default function FetchForm({ compact = false }: { compact?: boolean }) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const submittingRef = useRef(false);

  async function submitFetch(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (submittingRef.current) return;
    submittingRef.current = true;
    setSubmitting(true);
    setError("");
    try {
      const response = await fetch("/actions/fetch", { method: "POST" });
      if (response.redirected && response.url) {
        window.location.assign(response.url);
        return;
      }
      window.location.reload();
    } catch {
      submittingRef.current = false;
      setSubmitting(false);
      setError("刷新失败，请检查网络连接后重试");
    }
  }

  return (
    <form
      action="/actions/fetch"
      className={`fetch-form ${compact ? "compact" : ""}`}
      method="post"
      onSubmit={submitFetch}
    >
      <button className={compact ? "icon" : undefined} type="submit" title="刷新 RSS" aria-label="刷新 RSS" disabled={submitting}>
        <RefreshCw size={compact ? 17 : 15} /> {compact ? null : submitting ? "刷新中..." : "手动刷新 RSS"}
      </button>
      {error ? <span className="fetch-error" role="alert">{error}</span> : null}
    </form>
  );
}
