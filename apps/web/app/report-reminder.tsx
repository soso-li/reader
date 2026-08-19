"use client";

import { FileText, X } from "lucide-react";
import { useEffect, useState } from "react";

import { REPORT_REMINDER_DISMISSED_COOKIE } from "./report-reminder-cookie";

type ReportReminderSnapshot = {
  status: string;
  title: string;
  updated_at?: string | null;
};

export default function ReportReminder({ date, error, initialDismissed = false, report }: { date: string; error: string; initialDismissed?: boolean; report: ReportReminderSnapshot | null }) {
  const [dismissed, setDismissed] = useState(initialDismissed);
  const ready = report?.status === "ready";
  const pending = report?.status === "pending";

  useEffect(() => {
    const legacyNames = document.cookie
      .split(";")
      .map((part) => part.trim().split("=", 1)[0])
      .filter((name) => name.startsWith("reader_report_reminder_") && name !== REPORT_REMINDER_DISMISSED_COOKIE);
    const dismissedByLegacyCookie = legacyNames.includes(`reader_report_reminder_${date}`);
    for (const name of legacyNames) document.cookie = `${name}=; Path=/; Max-Age=0; SameSite=Lax`;
    if (dismissedByLegacyCookie) {
      document.cookie = `${REPORT_REMINDER_DISMISSED_COOKIE}=${encodeURIComponent(date)}; Path=/; Max-Age=31536000; SameSite=Lax`;
    }
  }, [date]);

  function dismiss() {
    document.cookie = `${REPORT_REMINDER_DISMISSED_COOKIE}=${encodeURIComponent(date)}; Path=/; Max-Age=31536000; SameSite=Lax`;
    setDismissed(true);
  }

  if (dismissed) return null;

  return (
    <section className="report-reminder" aria-label="前日报告提醒">
      <div className="report-reminder-icon" aria-hidden="true">
        <FileText size={17} />
      </div>
      <div className="report-reminder-main">
        <div className="report-reminder-title">{ready ? "前日报告已生成" : pending ? "前日报告生成中" : "前日报告还没有生成"}</div>
        <div className="report-reminder-meta">{error || (ready || pending ? report.title || `${date} 日报` : `${date} 日报`)}</div>
      </div>
      <div className="report-reminder-actions">
        {ready || pending ? (
          <a className="text-button" href={`/?view=reports&period=day&date=${date}`}>
            查看
          </a>
        ) : (
          <form action="/actions/report" method="post">
            <input type="hidden" name="period" value="day" />
            <input type="hidden" name="date" value={date} />
            <button type="submit">生成</button>
          </form>
        )}
        <button className="icon" type="button" title="关闭提醒" aria-label="关闭提醒" onClick={dismiss}>
          <X size={16} />
        </button>
      </div>
    </section>
  );
}
