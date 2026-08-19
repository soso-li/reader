const exactTimeFormatter = new Intl.DateTimeFormat("zh-CN", {
  day: "2-digit",
  hour: "2-digit",
  hour12: false,
  minute: "2-digit",
  month: "2-digit",
  second: "2-digit",
  timeZone: "Asia/Shanghai",
  year: "numeric"
});

export function formatRelativeTime(value: string | null) {
  if (!value) return "未知时间";
  const then = new Date(value).getTime();
  if (!Number.isFinite(then)) return "未知时间";
  const diffSeconds = Math.max(0, Math.floor((Date.now() - then) / 1000));
  if (diffSeconds < 60) return "刚刚";
  const minutes = Math.floor(diffSeconds / 60);
  if (minutes < 60) return `${minutes} 分钟前`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} 小时前`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days} 天前`;
  const months = Math.floor(days / 30);
  if (months < 12) return `${months} 个月前`;
  return `${Math.floor(months / 12)} 年前`;
}

export function formatExactTime(value: string | null) {
  if (!value) return "未知时间";
  const time = new Date(value).getTime();
  if (!Number.isFinite(time)) return "未知时间";
  return exactTimeFormatter.format(new Date(time));
}
