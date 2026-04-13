"""Download Scryfall bulk oracle_cards and refresh local SQLite atomically."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import requests

from mtg_viewer.db import connect, get_meta, import_oracle_cards_stream, init_schema, set_meta
from mtg_viewer.http_client import DEFAULT_UA, session as http_session

BULK_DATA_URL = "https://api.scryfall.com/bulk-data"
META_UPDATED_AT = "bulk_oracle_updated_at"
META_DOWNLOAD_URI = "bulk_oracle_download_uri"


def fetch_bulk_data_list(sess: requests.Session) -> list[dict[str, Any]]:
    r = sess.get(BULK_DATA_URL, timeout=60)
    r.raise_for_status()
    data = r.json()
    return list(data.get("data") or [])


def find_oracle_bulk_entry(entries: list[dict[str, Any]]) -> dict[str, Any] | None:
    for e in entries:
        if e.get("type") == "oracle_cards":
            return e
    return None


def needs_update(conn_path: Path, remote_updated_at: str, remote_uri: str) -> bool:
    if not conn_path.exists():
        return True
    conn = connect(conn_path, create=False)
    try:
        old = get_meta(conn, META_UPDATED_AT)
        old_uri = get_meta(conn, META_DOWNLOAD_URI)
        if old != remote_updated_at:
            return True
        if old_uri != remote_uri:
            return True
        return False
    finally:
        conn.close()


def download_oracle_json_to_path(download_uri: str, dest: Path, sess: requests.Session) -> None:
    with sess.get(download_uri, stream=True, timeout=300) as r:
        r.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def build_database_from_json_file(json_path: Path, db_out: Path) -> int:
    db_out.parent.mkdir(parents=True, exist_ok=True)
    if db_out.exists():
        db_out.unlink()
    conn = connect(db_out, create=True)
    try:
        init_schema(conn)
        with json_path.open("rb") as f:
            n = import_oracle_cards_stream(conn, f)
        return n
    finally:
        conn.close()


def update_oracle_cards(
    data_dir: Path,
    *,
    user_agent: str | None = None,
    force: bool = False,
) -> tuple[bool, str]:
    """
    If bulk oracle_cards is newer than local meta, download and rebuild cards.db.

    Returns (did_update, message).
    """
    data_dir = Path(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    db_path = data_dir / "cards.db"
    ua = user_agent or os.environ.get("SCRYFALL_USER_AGENT") or DEFAULT_UA
    sess = http_session(user_agent=ua)

    entries = fetch_bulk_data_list(sess)
    entry = find_oracle_bulk_entry(entries)
    if entry is None:
        return False, "No oracle_cards entry in bulk-data response"

    updated_at = str(entry.get("updated_at") or "")
    download_uri = str(entry.get("download_uri") or "")
    if not download_uri:
        return False, "oracle_cards missing download_uri"

    if not force and not needs_update(db_path, updated_at, download_uri):
        return False, "Local database is up to date"

    staging_db = data_dir / "cards.new.db"
    json_file = data_dir / ".oracle-cards-download.json"
    count = 0

    try:
        download_oracle_json_to_path(download_uri, json_file, sess)
        count = build_database_from_json_file(json_file, staging_db)

        conn = connect(staging_db, create=True)
        try:
            set_meta(conn, META_UPDATED_AT, updated_at)
            set_meta(conn, META_DOWNLOAD_URI, download_uri)
            conn.commit()
        finally:
            conn.close()

        final_path = data_dir / "cards.db"
        backup = data_dir / "cards.db.bak"
        if final_path.exists():
            if backup.exists():
                backup.unlink()
            final_path.rename(backup)
        try:
            staging_db.rename(final_path)
        except OSError:
            if not final_path.exists() and backup.exists():
                backup.rename(final_path)
            raise
        else:
            if backup.exists():
                backup.unlink()
    finally:
        if json_file.exists():
            try:
                json_file.unlink()
            except OSError:
                pass

    return True, f"Imported {count} oracle cards; bulk updated_at={updated_at}"
