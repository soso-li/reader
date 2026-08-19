import Link from "next/link";
import { notFound } from "next/navigation";

import ArticleContent from "../../../../article-content";
import { ApiFetchError, apiFetch, userFacingErrorMessage } from "../../../../lib/api";

type Evidence = {
  version_uid: string;
  source: { name: string };
  title: string;
  url: string;
  author: string;
  published_at: string | null;
  content: string;
  reading_html: string | null;
};

type HistoricalRevision = {
  revision_uid: string;
  revision_no: number;
  title: string;
  event_time: string | null;
  created_at: string;
  evidence: Evidence[];
};

export default async function HistoricalEventPage({
  params,
  searchParams
}: {
  params: Promise<{ event_uid: string; revision_uid: string }>;
  searchParams: Promise<{ period?: string; date?: string }>;
}) {
  const { event_uid: eventUid, revision_uid: revisionUid } = await params;
  const query = await searchParams;
  if (!isUid(eventUid) || !isUid(revisionUid)) notFound();
  let revision: HistoricalRevision;
  try {
    revision = await apiFetch<HistoricalRevision>(
      `/events/${encodeURIComponent(eventUid)}/revisions/${encodeURIComponent(revisionUid)}`
    );
  } catch (error) {
    if (error instanceof ApiFetchError && error.status === 404) notFound();
    return (
      <main id="reader-main" tabIndex={-1} className="historical-event-page">
        <Link className="text-button" href={reportBackHref(query)}>返回报告</Link>
        <p className="error-line">{userFacingErrorMessage(error, "历史证据加载失败")}</p>
      </main>
    );
  }
  return (
    <main id="reader-main" tabIndex={-1} className="historical-event-page">
      <Link className="text-button" href={reportBackHref(query)}>返回报告</Link>
      <header>
        <p className="item-meta">历史报告证据 · 版本 {revision.revision_no}</p>
        <h1>{revision.title}</h1>
        <p className="item-meta">事件时间：{formatDateTime(revision.event_time)} · 冻结时间：{formatDateTime(revision.created_at)}</p>
        <p className="source-meta">以下内容来自报告生成时引用的不可变事件版本，不随当前聚类变化。</p>
      </header>
      {revision.evidence.map((evidence) => (
        <article className="historical-evidence" key={evidence.version_uid}>
          <h2>{evidence.title || evidence.source.name}</h2>
          <p className="item-meta">
            {evidence.source.name} · {formatDateTime(evidence.published_at)}
          </p>
          {evidence.author ? <p className="item-meta">作者：{evidence.author}</p> : null}
          {evidence.url ? <a className="text-button" href={evidence.url} target="_blank" rel="noreferrer">打开原文</a> : null}
          <ArticleContent html={evidence.reading_html} text={evidence.content || "（该证据没有正文快照）"} />
        </article>
      ))}
    </main>
  );
}

function isUid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function reportBackHref(query: { period?: string; date?: string }) {
  const params = new URLSearchParams({ view: "reports", period: ["day", "week", "month"].includes(query.period ?? "") ? query.period! : "day" });
  if (/^\d{4}-\d{2}-\d{2}$/.test(query.date ?? "")) params.set("date", query.date!);
  return `/?${params}`;
}

function formatDateTime(value: string | null) {
  if (!value) return "未知";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai" });
}
