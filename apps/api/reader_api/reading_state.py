from __future__ import annotations


def next_read_status(current: str, requested: str) -> str:
    if requested in {"unread", "dismissed"}:
        return requested
    order = {
        "unread": 0,
        "dismissed": 0,
        "summary_seen": 1,
        "original_opened": 2,
    }
    return requested if order.get(requested, 0) >= order.get(current, 0) else current
