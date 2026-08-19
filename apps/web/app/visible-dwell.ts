type VisibleDwellOptions = {
  deferMs: number;
  isVisible: () => boolean;
  markSeen: () => void;
  addVisibilityListener: (listener: () => void) => void;
  removeVisibilityListener: (listener: () => void) => void;
  setTimer: (callback: () => void, delayMs: number) => number;
  clearTimer: (timerId: number) => void;
};

export function shouldStartVisibleDwellAttempt({
  eligible,
  attemptKey,
  attemptedKey
}: {
  eligible: boolean;
  attemptKey: string;
  attemptedKey: string | null;
}): boolean {
  return eligible && attemptKey !== attemptedKey;
}

export function startVisibleDwell({
  deferMs,
  isVisible,
  markSeen,
  addVisibilityListener,
  removeVisibilityListener,
  setTimer,
  clearTimer
}: VisibleDwellOptions): () => void {
  let timerId: number | null = null;
  let completed = false;

  const cancelTimer = () => {
    if (timerId === null) return;
    clearTimer(timerId);
    timerId = null;
  };
  const complete = () => {
    timerId = null;
    if (completed || !isVisible()) return;
    completed = true;
    markSeen();
  };
  const schedule = () => {
    cancelTimer();
    if (completed || !isVisible()) return;
    if (deferMs <= 0) {
      complete();
      return;
    }
    timerId = setTimer(complete, deferMs);
  };
  const onVisibilityChange = () => {
    if (isVisible()) schedule();
    else cancelTimer();
  };

  addVisibilityListener(onVisibilityChange);
  schedule();
  return () => {
    cancelTimer();
    removeVisibilityListener(onVisibilityChange);
  };
}
