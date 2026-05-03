#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from providers.store import connect_db, ensure_schema
from sync_portfolio_data import rebuild_all_summaries


def main() -> int:
    with connect_db() as conn:
        ensure_schema(conn)
        count = rebuild_all_summaries(conn)
        conn.commit()
    print(f"Rebuilt portfolio summaries for {count} successful batches.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
