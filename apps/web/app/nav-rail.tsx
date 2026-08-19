import { Bell, FileText, Film, ImageIcon, Mic, Newspaper, Settings, Tags, Users } from "lucide-react";
import Link from "next/link";
import type { ReactNode } from "react";

type View = "clusters" | "topics" | "reports" | "settings";
type BrowseRailItem = {
  media: "social" | "image" | "video" | "podcast" | "notification";
  href: string;
  enabled: boolean;
  unread_count: number;
};

export default function NavRail({ browseItems, clusterUnreadCount, currentMedia, currentView, links }: { browseItems: BrowseRailItem[]; clusterUnreadCount: number; currentMedia: string; currentView: View | "browse"; links: Record<View, string> }) {
  const social = browseItems.find((item) => item.media === "social");
  const image = browseItems.find((item) => item.media === "image");
  const video = browseItems.find((item) => item.media === "video");
  const podcast = browseItems.find((item) => item.media === "podcast");
  const notification = browseItems.find((item) => item.media === "notification");
  return (
    <aside className="rail" aria-label="主导航">
      <div className="rail-mark" aria-hidden="true">读</div>
      <nav className="rail-group" aria-label="阅读">
        <RailLink active={currentView === "clusters"} count={clusterUnreadCount} href={links.clusters} icon={<Newspaper size={20} />} label="聚类" />
        <RailLink active={currentView === "topics"} href={links.topics} icon={<Tags size={20} />} label="议题" />
        <RailLink active={currentView === "reports"} href={links.reports} icon={<FileText size={20} />} label="报告" />
      </nav>
      <nav className="rail-group" aria-label="浏览">
        {social?.enabled ? <RailLink active={currentView === "browse" && currentMedia === "social"} count={social.unread_count} href={social.href} icon={<Users size={19} />} label="社交" /> : <RailDisabled icon={<Users size={19} />} label="社交" reason="无订阅源" />}
        {image?.enabled ? <RailLink active={currentView === "browse" && currentMedia === "image"} count={image.unread_count} href={image.href} icon={<ImageIcon size={19} />} label="图片" /> : <RailDisabled icon={<ImageIcon size={19} />} label="图片" reason="无订阅源" />}
        {video?.enabled ? <RailLink active={currentView === "browse" && currentMedia === "video"} count={video.unread_count} href={video.href} icon={<Film size={19} />} label="视频" /> : <RailDisabled icon={<Film size={19} />} label="视频" reason="无订阅源" />}
        {podcast?.enabled ? <RailLink active={currentView === "browse" && currentMedia === "podcast"} count={podcast.unread_count} href={podcast.href} icon={<Mic size={19} />} label="音频" /> : <RailDisabled icon={<Mic size={19} />} label="音频" reason="无订阅源" />}
        {notification?.enabled ? <RailLink active={currentView === "browse" && currentMedia === "notification"} count={notification.unread_count} href={notification.href} icon={<Bell size={19} />} label="通知" /> : <RailDisabled icon={<Bell size={19} />} label="通知" reason="无订阅源" />}
      </nav>
      <nav className="rail-group rail-bottom" aria-label="设置">
        <RailLink active={currentView === "settings"} href={links.settings} icon={<Settings size={20} />} label="设置" />
      </nav>
    </aside>
  );
}

function RailLink({ active, count = 0, href, icon, label }: { active: boolean; count?: number; href: string; icon: ReactNode; label: string }) {
  const displayedCount = count > 99 ? "99+" : String(count);
  return (
    <Link className={`rail-button ${active ? "active" : ""}`} href={href} title={count > 99 ? `${label}，${count} 条未读` : label} aria-label={count > 0 ? `${label}，未读 ${displayedCount}` : label} aria-current={active ? "page" : undefined}>
      {icon}
      {count > 0 ? <span className="rail-badge" aria-hidden="true">{displayedCount}</span> : null}
      <span className="rail-label">{label}</span>
    </Link>
  );
}

function RailDisabled({ icon, label, reason = "后续阶段" }: { icon: ReactNode; label: string; reason?: string }) {
  return (
    <span className="rail-button disabled" title={`${label}（${reason}）`} aria-label={`${label}（${reason}）`}>
      {icon}
      <span className="rail-label">{label}</span>
    </span>
  );
}
