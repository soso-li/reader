export type ArticleFontSize = "small" | "standard" | "large";
export type ArticleLineHeight = "compact" | "standard" | "relaxed";
export type ArticleMaxWidth = "narrow" | "standard" | "wide";
export type ListDensity = "compact" | "comfortable";
export type ListThumbnails = "always" | "auto" | "never";
export type ClusterOrder = "desc" | "asc";

export type ReadingPreferences = {
  articleFontSize: ArticleFontSize;
  articleLineHeight: ArticleLineHeight;
  articleMaxWidth: ArticleMaxWidth;
  listDensity: ListDensity;
  listThumbnails: ListThumbnails;
  clusterOrder: ClusterOrder;
};

export const READING_PREFERENCE_COOKIE_NAMES = {
  articleFontSize: "reader-pref-article-font-size",
  articleLineHeight: "reader-pref-article-line-height",
  articleMaxWidth: "reader-pref-article-max-width",
  listDensity: "reader-pref-list-density",
  listThumbnails: "reader-pref-list-thumbnails",
  clusterOrder: "reader-pref-cluster-order"
} as const;

export const DEFAULT_READING_PREFERENCES: ReadingPreferences = {
  articleFontSize: "standard",
  articleLineHeight: "standard",
  articleMaxWidth: "standard",
  listDensity: "comfortable",
  listThumbnails: "never",
  clusterOrder: "desc"
};

export function readingPreferencesFromCookies(getCookie: (name: string) => string | undefined): ReadingPreferences {
  return {
    articleFontSize: option(getCookie(READING_PREFERENCE_COOKIE_NAMES.articleFontSize), ["small", "standard", "large"], DEFAULT_READING_PREFERENCES.articleFontSize),
    articleLineHeight: option(getCookie(READING_PREFERENCE_COOKIE_NAMES.articleLineHeight), ["compact", "standard", "relaxed"], DEFAULT_READING_PREFERENCES.articleLineHeight),
    articleMaxWidth: option(getCookie(READING_PREFERENCE_COOKIE_NAMES.articleMaxWidth), ["narrow", "standard", "wide"], DEFAULT_READING_PREFERENCES.articleMaxWidth),
    listDensity: option(getCookie(READING_PREFERENCE_COOKIE_NAMES.listDensity), ["compact", "comfortable"], DEFAULT_READING_PREFERENCES.listDensity),
    listThumbnails: option(getCookie(READING_PREFERENCE_COOKIE_NAMES.listThumbnails), ["always", "auto", "never"], DEFAULT_READING_PREFERENCES.listThumbnails),
    clusterOrder: option(getCookie(READING_PREFERENCE_COOKIE_NAMES.clusterOrder), ["desc", "asc"], DEFAULT_READING_PREFERENCES.clusterOrder)
  };
}

export function normalizeReadingPreference(name: string, value: string) {
  if (name === READING_PREFERENCE_COOKIE_NAMES.articleFontSize) return option(value, ["small", "standard", "large"], DEFAULT_READING_PREFERENCES.articleFontSize);
  if (name === READING_PREFERENCE_COOKIE_NAMES.articleLineHeight) return option(value, ["compact", "standard", "relaxed"], DEFAULT_READING_PREFERENCES.articleLineHeight);
  if (name === READING_PREFERENCE_COOKIE_NAMES.articleMaxWidth) return option(value, ["narrow", "standard", "wide"], DEFAULT_READING_PREFERENCES.articleMaxWidth);
  if (name === READING_PREFERENCE_COOKIE_NAMES.listDensity) return option(value, ["compact", "comfortable"], DEFAULT_READING_PREFERENCES.listDensity);
  if (name === READING_PREFERENCE_COOKIE_NAMES.listThumbnails) return option(value, ["always", "auto", "never"], DEFAULT_READING_PREFERENCES.listThumbnails);
  if (name === READING_PREFERENCE_COOKIE_NAMES.clusterOrder) return option(value, ["desc", "asc"], DEFAULT_READING_PREFERENCES.clusterOrder);
  return "";
}

function option<const T extends string>(value: string | undefined, allowed: readonly T[], fallback: T): T {
  return allowed.includes(value as T) ? (value as T) : fallback;
}
