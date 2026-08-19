import os


QUEUE_PREFIX = os.getenv("RQ_QUEUE_PREFIX", "reader").strip().strip("-") or "reader"


def reader_queue_names(queue_prefix: str) -> tuple[str, str]:
    return (f"{queue_prefix}-fetch", f"{queue_prefix}-llm")


def belongs_to_reader_queue_namespace(queue_name: str) -> bool:
    return queue_name.startswith("reader-")


FETCH_QUEUE_NAME, LLM_QUEUE_NAME = reader_queue_names(QUEUE_PREFIX)
