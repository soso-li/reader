export const SOURCE_MEDIA_OPTIONS = [
  { value: "article", label: "文章" },
  { value: "social", label: "社交" },
  { value: "image", label: "图片" },
  { value: "video", label: "视频" },
  { value: "podcast", label: "播客" },
  { value: "notification", label: "通知" }
] as const;

export type SourceMediaType = (typeof SOURCE_MEDIA_OPTIONS)[number]["value"];
