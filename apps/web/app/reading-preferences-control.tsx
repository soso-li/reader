import { cookies } from "next/headers";

import { READING_PREFERENCE_COOKIE_NAMES, type ReadingPreferences, readingPreferencesFromCookies } from "./reading-preferences";

const GROUPS: Array<{
  key: keyof ReadingPreferences;
  label: string;
  name: string;
  options: Array<[string, string]>;
}> = [
  { key: "articleFontSize", label: "正文字号", name: READING_PREFERENCE_COOKIE_NAMES.articleFontSize, options: [["small", "小"], ["standard", "标准"], ["large", "大"]] },
  { key: "articleLineHeight", label: "正文行高", name: READING_PREFERENCE_COOKIE_NAMES.articleLineHeight, options: [["compact", "紧凑"], ["standard", "标准"], ["relaxed", "宽松"]] },
  { key: "articleMaxWidth", label: "正文宽度", name: READING_PREFERENCE_COOKIE_NAMES.articleMaxWidth, options: [["narrow", "窄"], ["standard", "标准"], ["wide", "宽"]] },
  { key: "listDensity", label: "列表密度", name: READING_PREFERENCE_COOKIE_NAMES.listDensity, options: [["compact", "紧凑"], ["comfortable", "舒适"]] },
  { key: "listThumbnails", label: "列表缩略图", name: READING_PREFERENCE_COOKIE_NAMES.listThumbnails, options: [["always", "总是"], ["auto", "有图"], ["never", "从不"]] },
  { key: "clusterOrder", label: "聚类排序", name: READING_PREFERENCE_COOKIE_NAMES.clusterOrder, options: [["desc", "最新"], ["asc", "最旧"]] }
];

export default async function ReadingPreferencesControl() {
  const cookieStore = await cookies();
  const preferences = readingPreferencesFromCookies((name) => cookieStore.get(name)?.value);

  return (
    <div className="reading-preferences">
      {GROUPS.map((group) => (
        <div key={group.name} className="preference-row">
          <span>{group.label}</span>
          <div className="theme-control reader-pref-control" role="group" aria-label={group.label}>
            {group.options.map(([value, label]) => (
              <form key={value} action="/actions/reading-preferences" method="post">
                <input type="hidden" name="name" value={group.name} />
                <input type="hidden" name="value" value={value} />
                <button type="submit" className={preferences[group.key] === value ? "active" : ""}>
                  {label}
                </button>
              </form>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
