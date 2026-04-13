"""CLI: python -m mtg_viewer [update] ['query']"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from mtg_viewer.http_client import DEFAULT_UA
from mtg_viewer.query.executor import ExecuteConfig, ExecutorError, execute_parse_result
from mtg_viewer.query.parser import parse_query

EXIT_OK = 0
EXIT_PARSE = 1
EXIT_NO_DB = 2
EXIT_OFFLINE_REMOTE = 3
EXIT_NETWORK = 4
EXIT_OTHER = 5


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    data_dir = Path(os.environ.get("MTG_VIEWER_DATA", "data"))

    if argv and argv[0] == "update":
        from mtg_viewer.updater import update_oracle_cards

        ua = os.environ.get("SCRYFALL_USER_AGENT") or DEFAULT_UA
        force = "--force" in argv
        for a in argv[1:]:
            if a.startswith("--data-dir="):
                data_dir = Path(a.split("=", 1)[1])
        did, msg = update_oracle_cards(data_dir, user_agent=ua, force=force)
        print(msg)
        return EXIT_OK if did or "up to date" in msg.lower() else EXIT_OTHER

    if argv and argv[0] == "refresh-search":
        from mtg_viewer.db import connect, refresh_search_text_from_raw

        rs_data = Path(os.environ.get("MTG_VIEWER_DATA", "data"))
        for a in argv[1:]:
            if a.startswith("--data-dir="):
                rs_data = Path(a.split("=", 1)[1])
        db_path = rs_data / "cards.db"
        if not db_path.exists():
            print(f"No database at {db_path}.", file=sys.stderr)
            return EXIT_NO_DB
        conn = connect(db_path, create=False)
        try:
            n = refresh_search_text_from_raw(conn)
        finally:
            conn.close()
        print(f"Updated derived columns (search text, color_bits, ci_bits, is_commander) for {n} cards.")
        return EXIT_OK

    p = argparse.ArgumentParser(description="Search local Scryfall oracle_cards database")
    p.add_argument("query", nargs="?", default="", help="Scryfall-style query")
    p.add_argument(
        "--data-dir",
        type=Path,
        default=data_dir,
        help="Directory containing cards.db",
    )
    p.add_argument(
        "--offline",
        action="store_true",
        help="Disallow network (tagger/remote predicates need cache)",
    )
    p.add_argument("--ids", action="store_true", help="Print card ids instead of names")
    args = p.parse_args(argv)

    from mtg_viewer.db import connect

    db_path = args.data_dir / "cards.db"
    if not db_path.exists():
        print(f"No database at {db_path}. Run: python -m mtg_viewer update", file=sys.stderr)
        return EXIT_NO_DB

    conn = connect(db_path, create=False)
    rows: list = []
    try:
        pr = parse_query(args.query)
        cfg = ExecuteConfig(
            allow_network=not args.offline,
            user_agent=os.environ.get("SCRYFALL_USER_AGENT") or DEFAULT_UA,
        )
        rows = execute_parse_result(conn, pr, cfg=cfg)
    except ExecutorError as e:
        print(str(e), file=sys.stderr)
        if e.code == "parse":
            return EXIT_PARSE
        if e.code == "offline_remote":
            return EXIT_OFFLINE_REMOTE
        if e.code == "network":
            return EXIT_NETWORK
        return EXIT_OTHER
    except Exception as e:
        print(str(e), file=sys.stderr)
        return EXIT_OTHER
    finally:
        conn.close()

    for r in rows:
        if args.ids:
            print(r["id"])
        else:
            print(r["name"])
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
