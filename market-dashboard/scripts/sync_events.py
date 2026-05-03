#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from providers import events as event_provider
from providers.store import connect_db, ensure_schema


def main() -> int:
    conn = connect_db()
    try:
        ensure_schema(conn)
        event_provider.ensure_event_defaults(conn)
        result = event_provider.sync_enabled_sources(conn)
        print(json.dumps(result, ensure_ascii=False, default=str))
        failed = [run for run in result.get("runs", []) if run.get("status") != "success"]
        return 1 if failed and len(failed) == len(result.get("runs", [])) else 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
