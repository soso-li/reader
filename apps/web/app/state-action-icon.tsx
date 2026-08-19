import { Bookmark, CheckCircle, Circle, FileText, Loader2, X } from "lucide-react";

export function stateActionIcon(label?: string, active?: boolean) {
  if (label === "标记看过") return <CheckCircle size={17} />;
  if (label === "标记未读") return <Circle size={17} />;
  if (label === "忽略") return <X size={17} />;
  if (label === "稍后阅读") return <Bookmark size={17} fill={active ? "currentColor" : "none"} />;
  if (label === "抓取中") return <Loader2 size={17} />;
  if (label === "RSS 正文" || label === "阅读模式") return <FileText size={17} />;
  if (label === "Bionic") return <span className="bionic-icon"><strong>B</strong>r</span>;
  return <span className="icon-text">{label}</span>;
}
