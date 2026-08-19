import type { Metadata, Viewport } from "next";
import { cookies } from "next/headers";
import InstalledReaderLaunch from "./installed-reader-launch";
import { readingPreferencesFromCookies } from "./reading-preferences";
import "./globals.css";

export const metadata: Metadata = {
  title: "Reader",
  description: "个人 RSS 信息阅读器"
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1
};

export default async function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  const cookieStore = await cookies();
  const savedTheme = cookieStore.get("reader-theme")?.value;
  const theme = savedTheme === "light" || savedTheme === "dark" ? savedTheme : undefined;
  const savedLayoutMode = cookieStore.get("reader-force-mode")?.value;
  const layoutMode = savedLayoutMode === "compact" || savedLayoutMode === "mobile" ? "compact" : "auto";
  const layoutModeClass = layoutMode === "compact" ? "reader-force-mobile" : undefined;
  const readingPreferences = readingPreferencesFromCookies((name) => cookieStore.get(name)?.value);

  return (
    <html
      lang="zh-CN"
      className={layoutModeClass}
      data-layout-mode={layoutMode}
      data-reader-article-size={readingPreferences.articleFontSize}
      data-reader-line-height={readingPreferences.articleLineHeight}
      data-reader-max-width={readingPreferences.articleMaxWidth}
      data-reader-list-density={readingPreferences.listDensity}
      data-reader-thumbnails={readingPreferences.listThumbnails}
      data-reader-cluster-order={readingPreferences.clusterOrder}
      data-theme={theme}
      suppressHydrationWarning
    >
      <body>
        <a className="skip-link" href="#reader-main">跳到主要内容</a>
        <InstalledReaderLaunch />
        {children}
      </body>
    </html>
  );
}
