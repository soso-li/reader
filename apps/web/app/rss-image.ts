export function rssImageSrc(src: string) {
  return `/images/rss?${new URLSearchParams({ src, v: "5" }).toString()}`;
}
