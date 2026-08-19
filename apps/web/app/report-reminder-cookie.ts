export const REPORT_REMINDER_DISMISSED_COOKIE = "reader_report_reminder_dismissed_date";

export function isReportReminderDismissed(cookieValue: string | undefined, date: string) {
  return cookieValue === date;
}
