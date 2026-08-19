import { cookies } from "next/headers";

type Theme = "system" | "light" | "dark";

export default async function ThemeControl() {
  const cookieStore = await cookies();
  const savedTheme = cookieStore.get("reader-theme")?.value;
  const theme: Theme = savedTheme === "light" || savedTheme === "dark" ? savedTheme : "system";

  return (
    <div className="theme-control" role="group" aria-label="主题">
      {[
        ["system", "跟随系统"],
        ["light", "固定浅色"],
        ["dark", "固定深色"]
      ].map(([value, label]) => (
        <form key={value} action="/actions/theme" method="post">
          <input type="hidden" name="theme" value={value} />
          <button type="submit" className={theme === value ? "active" : ""}>
            {label}
          </button>
        </form>
      ))}
    </div>
  );
}
