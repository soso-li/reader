from __future__ import annotations

from .deployment_validation import main as shared_main
from .isolated_stage_target import P04_DATABASE_TARGET_POLICY


def main() -> int:
    return shared_main(stage_target_policy=P04_DATABASE_TARGET_POLICY)


if __name__ == "__main__":
    raise SystemExit(main())
