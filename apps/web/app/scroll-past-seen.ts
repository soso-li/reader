type VerticalBounds = {
  top: number;
  bottom: number;
};

export function scrollPastReadStatusTransition({
  previousReadStatus,
  nextReadStatus,
  previousRevisionUid,
  nextRevisionUid,
  marked,
  presented
}: {
  previousReadStatus: string | undefined;
  nextReadStatus: string;
  previousRevisionUid?: string | null;
  nextRevisionUid?: string | null;
  marked: boolean;
  presented: boolean;
}): { marked: boolean; presented: boolean } {
  if (
    (previousRevisionUid !== undefined &&
      previousRevisionUid !== nextRevisionUid) ||
    (previousReadStatus !== undefined &&
      previousReadStatus !== "unread" &&
      nextReadStatus === "unread")
  ) {
    return { marked: false, presented: false };
  }
  return { marked, presented };
}

export function scrollPastRowTransition({
  row,
  root,
  surfacePresented,
  wasPresented
}: {
  row: VerticalBounds;
  root: VerticalBounds;
  surfacePresented: boolean;
  wasPresented: boolean;
}): { presented: boolean; markSeen: boolean } {
  const visibleInsideRoot =
    surfacePresented && row.bottom > root.top + 1 && row.top < root.bottom - 1;
  const presented = wasPresented || visibleInsideRoot;
  return {
    presented,
    markSeen: surfacePresented && presented && row.bottom <= root.top + 1
  };
}
