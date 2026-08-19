import type { MetadataRoute } from "next";

export default function manifest(): MetadataRoute.Manifest {
  return {
    id: "/",
    name: "Reader",
    short_name: "Reader",
    description: "个人 RSS 信息阅读器",
    start_url: "/?filter=unread&pane=list",
    scope: "/",
    display: "standalone"
  };
}
