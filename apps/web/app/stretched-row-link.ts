import type { MouseEvent } from "react";

const INTERACTIVE_TARGETS =
  'a, button, input, select, textarea, audio, video, [role="button"], [role="link"], [contenteditable="true"]';

export function activateStretchedRowLink(event: MouseEvent<HTMLElement>) {
  if ((event.target as Element).closest(INTERACTIVE_TARGETS)) return;
  event.currentTarget
    .querySelector<HTMLAnchorElement>(".stretched-row-link")
    ?.click();
}
