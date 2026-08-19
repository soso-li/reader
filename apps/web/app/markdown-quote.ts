type MarkdownQuoteInput = {
  publishedAt: string | null;
  sourceName: string;
  summary: string;
  title: string;
  url: string;
};

export function markdownQuote({ publishedAt, sourceName, summary, title, url }: MarkdownQuoteInput) {
  const first = firstParagraph(summary);
  const sourceParts = [`[${escapeMarkdownLinkText(title || url)}](${url})`, sourceName, dateLabel(publishedAt)].filter(Boolean);
  const citation = `> — ${sourceParts.join(" · ")}`;
  return first ? `> ${first}\n>\n${citation}` : citation;
}

function firstParagraph(value: string) {
  const paragraph = value
    .split(/\n+/)
    .map((line) => line.trim())
    .find((line) => line && !/^!\[[^\]]*]\(/.test(line));
  if (!paragraph) return "";
  return paragraph.length > 220 ? `${paragraph.slice(0, 220).trim()}...` : paragraph;
}

function dateLabel(value: string | null) {
  if (!value) return "";
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return "";
  return new Date(time).toISOString().slice(0, 10);
}

function escapeMarkdownLinkText(value: string) {
  return value.replace(/([\\[\]])/g, "\\$1");
}
