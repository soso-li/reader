"use client";

import { AlertCircle, Check, RefreshCw } from "lucide-react";
import type { CSSProperties } from "react";

import type { PullRefreshStatus } from "./use-pull-refresh";

export default function PullRefresh({
  distance,
  ready,
  status
}: {
  distance: number;
  ready: boolean;
  status: PullRefreshStatus;
}) {
  if (!distance && status === "idle") return null;
  const visualState = status === "idle" ? "pulling" : status;
  const rotation = Math.min(distance / 72, 1) * 180;
  const label = ready ? "松开即可刷新" : "继续下拉以刷新";

  return (
    <span
      className="pull-refresh"
      data-ready={ready ? "true" : "false"}
      data-state={visualState}
      style={{ "--pull-refresh-rotation": `${rotation}deg` } as CSSProperties}
    >
      {status === "error" ? (
        <>
          <AlertCircle aria-hidden="true" className="pull-refresh-icon" size={17} />
          <span>刷新失败，请重试</span>
        </>
      ) : status === "success" ? (
        <>
          <Check aria-hidden="true" className="pull-refresh-icon" size={17} />
          <span className="rail-label">刷新完成</span>
        </>
      ) : (
        <>
          <RefreshCw aria-hidden="true" className="pull-refresh-icon" size={17} />
          <span className="rail-label">
            {status === "refreshing" ? "正在刷新" : label}
          </span>
        </>
      )}
    </span>
  );
}
