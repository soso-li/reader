"use client";

import { useRef } from "react";

import { useActionDialog } from "./action-dialog";

export default function TopicDeleteForm({ topicId, topicName }: { topicId: number; topicName: string }) {
  const formRef = useRef<HTMLFormElement | null>(null);
  const actionDialog = useActionDialog();

  return (
    <>
      <form ref={formRef} action="/actions/topic" method="post">
        <input type="hidden" name="topic_id" value={topicId} />
        <input type="hidden" name="action" value="delete" />
        <button
          className="danger"
          type="button"
          onClick={async () => {
            if (await actionDialog.confirm({
              title: "删除议题组",
              message: `永久删除议题组“${topicName}”？此操作无法撤销。`,
              confirmLabel: "永久删除",
              danger: true
            })) formRef.current?.requestSubmit();
          }}
        >删除议题组</button>
      </form>
      {actionDialog.dialog}
    </>
  );
}
