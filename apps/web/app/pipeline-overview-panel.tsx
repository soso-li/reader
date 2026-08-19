"use client";

import { useEffect, useMemo, useState } from "react";

export type QueueOverview = {
  available: boolean;
  queued: number;
  running: number;
  error: string;
};

export type PipelineOverview = {
  rss: {
    last_completed_at: string | null;
    interval_seconds: number;
    source_count: number;
    failed_source_count: number;
    queue: QueueOverview;
  };
  embedding: {
    pending_items: number;
    queue: QueueOverview;
  };
  translation: {
    cached_24h: number;
  };
  generation: {
    pending: number;
    running: number;
    failed: number;
    complete: number;
    latest_failed_error: string;
  };
};

export default function PipelineOverviewPanel({ apiUrl, initial, initialError }: { apiUrl: string; initial: PipelineOverview | null; initialError?: string }) {
  const baseUrl = useMemo(() => apiUrl.replace(/\/$/, ""), [apiUrl]);
  const [overview, setOverview] = useState<PipelineOverview | null>(initial);
  const [error, setError] = useState(initialError ?? "");
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);
  const [workingAction, setWorkingAction] = useState("");

  async function load() {
    setLoading(true);
    try {
      const response = await fetch(`${baseUrl}/pipeline/overview`, { cache: "no-store" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      setOverview((await response.json()) as PipelineOverview);
      setError("");
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : "后台任务状态加载失败");
    } finally {
      setLoading(false);
    }
  }

  async function trigger(path: string, label: string) {
    setWorkingAction(label);
    setMessage("");
    try {
      const response = await fetch(`${baseUrl}${path}`, { method: "POST" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      setMessage(`${label}已提交`);
      await load();
    } catch (actionError) {
      setError(actionError instanceof Error ? `${label}失败：${actionError.message}` : `${label}失败`);
    } finally {
      setWorkingAction("");
    }
  }

  useEffect(() => {
    if (!overview) void load();
    const timer = window.setInterval(() => void load(), 30_000);
    return () => window.clearInterval(timer);
  }, [baseUrl]);

  return (
    <section id="settings-pipeline" className="settings-block">
      <div className="settings-block-heading">
        <h3>后台任务</h3>
        <button type="button" onClick={() => void load()} disabled={loading}>
          {loading ? "刷新中" : "刷新"}
        </button>
      </div>
      {error ? <p className="error-line" role="alert">{error}</p> : null}
      {message ? <p className="status-line success-line" role="status">{message}</p> : null}
      {overview ? (
        <div className="pipeline-grid">
          <PipelineCard
            title="RSS 抓取"
            rows={[
              ["最近完成", formatPipelineDateTime(overview.rss.last_completed_at)],
              ["调度间隔", formatDuration(overview.rss.interval_seconds)],
              ["源数 / 失败", `${overview.rss.source_count} / ${overview.rss.failed_source_count}`],
              ["队列", queueText(overview.rss.queue)]
            ]}
            queue={overview.rss.queue}
          />
          <PipelineCard
            title="Embedding"
            rows={[
              ["待处理", `${overview.embedding.pending_items}`],
              ["队列", queueText(overview.embedding.queue)]
            ]}
            queue={overview.embedding.queue}
          />
          <PipelineCard title="翻译" rows={[["近 24h 缓存", `${overview.translation.cached_24h}`]]} />
          <PipelineCard
            title="生成任务"
            rows={[
              ["待处理 / 运行中", `${overview.generation.pending} / ${overview.generation.running}`],
              ["失败 / 完成", `${overview.generation.failed} / ${overview.generation.complete}`],
              ["最近失败", overview.generation.latest_failed_error || "—"]
            ]}
          />
        </div>
      ) : (
        <p className="source-meta">暂无后台任务状态。</p>
      )}
      <div className="settings-action-row">
        <button type="button" onClick={() => void trigger("/jobs/fetch", "RSS 抓取")} disabled={Boolean(workingAction)}>
          {workingAction === "RSS 抓取" ? "提交中" : "触发 RSS 抓取"}
        </button>
        <button type="button" onClick={() => void trigger("/jobs/embeddings", "Embedding")} disabled={Boolean(workingAction)}>
          {workingAction === "Embedding" ? "提交中" : "生成 Embedding / 重聚类"}
        </button>
      </div>
    </section>
  );
}

function PipelineCard({ title, rows, queue }: { title: string; rows: Array<[string, string]>; queue?: QueueOverview }) {
  return (
    <article className="pipeline-card">
      <div className="pipeline-card-title">
        <h4>{title}</h4>
        {queue ? <span className={`pipeline-status-dot ${queue.available ? "ok" : "bad"}`} role="img" aria-label={queue.available ? "队列可用" : "队列不可用"} /> : null}
      </div>
      <dl>
        {rows.map(([label, value]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd title={value}>{value}</dd>
          </div>
        ))}
      </dl>
      {queue && !queue.available ? <p className="source-meta">队列不可用：{queue.error || "Redis 未连接"}</p> : null}
    </article>
  );
}

function queueText(queue: QueueOverview) {
  return queue.available ? `${queue.queued} 排队 / ${queue.running} 运行` : "队列不可用";
}

const pipelineDateTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  day: "2-digit",
  hour: "2-digit",
  hour12: false,
  minute: "2-digit",
  month: "2-digit",
  timeZone: "Asia/Shanghai",
  year: "numeric"
});

export function formatPipelineDateTime(value: string | null) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  return pipelineDateTimeFormatter.format(date);
}

function formatDuration(seconds: number) {
  if (seconds >= 3600) return `${Math.round(seconds / 3600)} 小时`;
  if (seconds >= 60) return `${Math.round(seconds / 60)} 分钟`;
  return `${seconds} 秒`;
}
