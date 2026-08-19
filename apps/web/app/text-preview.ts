export function previewText(value: string) {
  const text = decodeEntities(value)
    .replace(/<[^>]*>/g, " ")
    .replace(/!\[[^\]]*]\([^)]*(?:\)|$)/g, " ")
    .replace(/\s+!$/, "")
    .replace(/\s+/g, " ")
    .trim();
  if (!text || /^(?:null|undefined|none)$/i.test(text)) return "无摘要";
  return text.length > 160 ? `${text.slice(0, 160)}…` : text;
}

function decodeEntities(value: string) {
  const named: Record<string, string> = {
    amp: "&",
    apos: "'",
    gt: ">",
    lt: "<",
    nbsp: " ",
    quot: '"'
  };
  return value.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (entity, key: string) => {
    if (key.startsWith("#x")) return codePoint(Number.parseInt(key.slice(2), 16), entity);
    if (key.startsWith("#")) return codePoint(Number.parseInt(key.slice(1), 10), entity);
    return named[key.toLowerCase()] ?? entity;
  });
}

function codePoint(value: number, fallback: string) {
  return Number.isInteger(value) && value >= 0 && value <= 0x10ffff
    ? String.fromCodePoint(value)
    : fallback;
}
