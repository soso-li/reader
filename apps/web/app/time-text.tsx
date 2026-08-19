"use client";

import { useEffect, useState } from "react";

import { formatExactTime, formatRelativeTime } from "./time-format";

export function TimeText({ value, interactive = true }: { value: string | null; interactive?: boolean }) {
  const [exact, setExact] = useState(false);
  const [label, setLabel] = useState(() => stableTimeText(value));

  useEffect(() => {
    setLabel(exact ? formatExactTime(value) : formatRelativeTime(value));
    if (exact) return;
    const timer = window.setInterval(() => setLabel(formatRelativeTime(value)), 60_000);
    return () => window.clearInterval(timer);
  }, [exact, value]);

  if (!value) return <span title="未知时间">未知时间</span>;
  if (!interactive) {
    return (
      <time className="toggle-time is-static" dateTime={value} title={formatExactTime(value)}>
        {label}
      </time>
    );
  }
  const toggle = () => setExact((current) => !current);
  return (
    <time
      className="toggle-time"
      dateTime={value}
      role="button"
      tabIndex={0}
      title={exact ? "点击切换相对时间" : formatExactTime(value)}
      onClick={(event) => {
        event.preventDefault();
        event.stopPropagation();
        toggle();
      }}
      onKeyDown={(event) => {
        if (event.key !== "Enter" && event.key !== " ") return;
        event.preventDefault();
        event.stopPropagation();
        toggle();
      }}
    >
      {label}
    </time>
  );
}

function stableTimeText(value: string | null) {
  if (!value) return "未知时间";
  return formatExactTime(value);
}

export { formatExactTime, formatRelativeTime } from "./time-format";
