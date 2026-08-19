"use client";

import { Star } from "lucide-react";

import { stateActionIcon } from "./state-action-icon";

export function StateButton({
  object,
  label,
  icon,
  active,
  disabled,
  onClick
}: {
  object?: { id: number; starred: boolean };
  label?: string;
  icon?: boolean;
  active?: boolean;
  disabled?: boolean;
  onClick: () => void;
}) {
  const title = icon ? "星标" : label || "操作";
  const isActive = Boolean(active || (icon && object?.starred));
  return (
    <button className={`icon ${isActive ? "active" : ""}`} type="button" title={title} aria-label={title} disabled={disabled} onClick={onClick}>
      {icon ? <Star size={17} fill={object?.starred ? "currentColor" : "none"} /> : stateActionIcon(label, active)}
    </button>
  );
}
