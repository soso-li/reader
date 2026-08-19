import { apiFetch, userFacingErrorMessage } from "../lib/api";
import { UninterestedList, type UninterestedReason, type UninterestedTarget } from "../uninterested-actions";
import { queryString } from "../url-state";

type SearchParams = Record<string, string | string[] | undefined>;
type Source = { id: number; name: string };
type Targets = { count: number; items: UninterestedTarget[] };

const REASONS: Array<[UninterestedReason, string]> = [
  ["promotion", "广告 / 推广"],
  ["repetitive", "没有新信息 / 重复炒作"],
  ["topic", "这个主题不感兴趣"],
  ["low_quality", "标题党 / 内容质量差"],
  ["other", "其他"]
];

export default async function UninterestedPage({ searchParams }: { searchParams: Promise<SearchParams> }) {
  const params = await searchParams;
  const q = one(params.q) ?? "";
  const reason = REASONS.some(([value]) => value === one(params.reason))
    ? one(params.reason) as UninterestedReason
    : "";
  const sourceId = positiveInteger(one(params.source_id));
  const query = queryString({
    q: q || undefined,
    reason: reason || undefined,
    source_id: sourceId
  });
  let targets: Targets = { count: 0, items: [] };
  let sources: Source[] = [];
  let error = "";
  try {
    [targets, sources] = await Promise.all([
      apiFetch<Targets>(`/uninterested-targets?${query}`),
      apiFetch<Source[]>("/sources/navigation")
    ]);
  } catch (cause) {
    error = userFacingErrorMessage(cause, "不感兴趣列表加载失败");
  }

  return (
    <main id="reader-main" tabIndex={-1} className="uninterested-page">
      <header className="uninterested-page-header">
        <a href="/">← 返回 Reader</a>
        <div>
          <h1>不感兴趣</h1>
          <p>这里是尚未被正式规则覆盖的样本；原因只做记录。</p>
        </div>
        <a href="/?view=browse&media=article&filtered=1">查看已过滤</a>
      </header>
      <form className="uninterested-filters" method="get">
        <input aria-label="搜索不感兴趣内容" name="q" defaultValue={q} placeholder="搜索标题、正文或来源" />
        <select aria-label="按原因筛选" name="reason" defaultValue={reason}>
          <option value="">全部原因</option>
          {REASONS.map(([value, label]) => <option key={value} value={value}>{label}</option>)}
        </select>
        <select aria-label="按来源筛选" name="source_id" defaultValue={sourceId ?? ""}>
          <option value="">全部来源</option>
          {sources.map((source) => <option key={source.id} value={source.id}>{source.name}</option>)}
        </select>
        <button type="submit">筛选</button>
      </form>
      {error ? <p className="error-line" role="alert">{error}</p> : null}
      <UninterestedList initialCount={targets.count} initialTargets={targets.items} />
    </main>
  );
}

function one(value: string | string[] | undefined) {
  return Array.isArray(value) ? value[0] : value;
}

function positiveInteger(value: string | undefined) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}
