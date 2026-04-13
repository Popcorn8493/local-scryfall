"""SQLite schema, connection helpers, and bulk import from Scryfall oracle_cards."""

from __future__ import annotations

import json
import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO, Iterator

import ijson  # type: ignore[import-untyped]

COLOR_W = 1
COLOR_U = 2
COLOR_B = 4
COLOR_R = 8
COLOR_G = 16

_COLOR_MAP = {"W": COLOR_W, "U": COLOR_U, "B": COLOR_B, "R": COLOR_R, "G": COLOR_G}


def colors_to_bits(colors: list[str] | None) -> int:
    if not colors:
        return 0
    bits = 0
    for c in colors:
        bits |= _COLOR_MAP.get(c, 0)
    return bits


_WUBRG_ORDER = ("W", "U", "B", "R", "G")


def _colors_union_from_faces(card: dict[str, Any]) -> list[str] | None:
    """Union of card_faces[].colors; None if no face lists colors."""
    got: set[str] = set()
    for face in card.get("card_faces") or []:
        for c in face.get("colors") or []:
            got.add(c)
    if not got:
        return None
    return [c for c in _WUBRG_ORDER if c in got]


def card_colors_list_for_bits(card: dict[str, Any]) -> list[str] | None:
    """
    Colors used for `c:` / `color=` search (matches Scryfall).

    Multiface layouts sometimes omit root `colors` (null); Scryfall search still
    uses the union of face colors — see transform DFCs like Soundwave.
    """
    root = card.get("colors")
    if isinstance(root, list) and root:
        return root
    if root is None:
        merged = _colors_union_from_faces(card)
        return merged
    # Explicit [] = colorless at card root
    return None


def _strip_parenthetical_segments(s: str) -> str:
    """
    Remove (...) blocks. Per Scryfall docs, `o:` / `oracle:` search rules text
    without reminder text; `fo:` / `fulloracle:` includes it. We approximate
    reminders as parenthetical segments (see https://scryfall.com/docs/syntax ).
    """
    depth = 0
    out: list[str] = []
    for ch in s:
        if ch == "(":
            depth += 1
            continue
        if ch == ")":
            if depth > 0:
                depth -= 1
            continue
        if depth == 0:
            out.append(ch)
    return "".join(out)


def _oracle_face_parts_raw(card: dict[str, Any]) -> list[str]:
    """Oracle text segments per face, in order (root then card_faces), not stripped."""
    parts: list[str] = []
    ot = card.get("oracle_text") or ""
    if ot:
        parts.append(ot)
    for face in card.get("card_faces") or []:
        fo = face.get("oracle_text") or ""
        if fo:
            parts.append(fo)
    return parts


def _face_texts(card: dict[str, Any]) -> tuple[str, str, str]:
    """Aggregate (o:/oracle: search text, fo:/fulloracle: text, type line search)."""
    o_raw = _oracle_face_parts_raw(card)
    sep = "\n"
    full_ot = sep.join(o_raw)
    stripped_ot = sep.join(_strip_parenthetical_segments(p) for p in o_raw)
    t_parts: list[str] = []
    tl = card.get("type_line") or ""
    if tl:
        t_parts.append(tl)
    for face in card.get("card_faces") or []:
        ft = face.get("type_line") or ""
        if ft:
            t_parts.append(ft)
    search_tl = sep.join(t_parts)
    return stripped_ot, full_ot, search_tl


def _aggregated_type_line_lower(card: dict[str, Any]) -> str:
    """Lowercased type lines from root and each face (MDFC / transform / adventure)."""
    parts: list[str] = []
    tl = card.get("type_line") or ""
    if tl:
        parts.append(tl.lower())
    for face in card.get("card_faces") or []:
        ft = face.get("type_line") or ""
        if ft:
            parts.append(ft.lower())
    return " ".join(parts)


def compute_is_commander(card: dict[str, Any]) -> bool:
    """Heuristic aligned with Scryfall `is:commander` for deck construction."""
    layout = card.get("layout") or ""
    if layout in ("token", "emblem", "art_series", "double_faced_token", "vanguard"):
        return False

    tl = _aggregated_type_line_lower(card)
    oracle_all = (card.get("oracle_text") or "").lower()
    for face in card.get("card_faces") or []:
        oracle_all += " " + (face.get("oracle_text") or "").lower()

    if "legendary" not in tl:
        return False

    if "creature" in tl:
        return True

    if "planeswalker" in tl:
        return "can be your commander" in oracle_all

    # Background (can pair with commander); Doctor Who commanders, etc.
    if "background" in tl and "enchantment" in tl:
        return True
    if "doctor" in tl and "creature" in tl:
        return True

    return False


def _parse_price(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _coerce_float(val: Any) -> float | None:
    """SQLite + JSON helpers: bulk data may use Decimal (e.g. cmc)."""
    if val is None:
        return None
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _json_default(o: Any) -> Any:
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def card_to_raw_json(card: dict[str, Any]) -> str:
    return json.dumps(
        card,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    )


def flatten_card(card: dict[str, Any]) -> dict[str, Any]:
    """Map one oracle_cards JSON object to table row dict.

    Oracle bulk uses card-level ``mana_cost`` and ``cmc`` (Scryfall model); ``m:`` / ``mv:``
    queries compile against those columns only — not a concatenation of per-face mana strings.
    Face oracle/type text is aggregated into ``search_*`` columns for ``o:`` / ``t:``.
    """
    stripped_ot, full_ot, search_tl = _face_texts(card)
    o_fb = card.get("oracle_text") or ""
    search_ot = stripped_ot or (_strip_parenthetical_segments(o_fb) if o_fb else "")
    search_fot = full_ot if full_ot else o_fb
    prices = card.get("prices") or {}
    legalities = card.get("legalities") or {}
    edh = card.get("edhrec_rank")
    try:
        edhrec_rank = int(edh) if edh is not None else None
    except (TypeError, ValueError):
        edhrec_rank = None

    keywords = card.get("keywords") or []
    row: dict[str, Any] = {
        "id": card.get("id"),
        "oracle_id": card.get("oracle_id"),
        "name": card.get("name") or "",
        "mana_cost": card.get("mana_cost"),
        "cmc": _coerce_float(card.get("cmc")),
        "type_line": card.get("type_line"),
        "oracle_text": card.get("oracle_text"),
        "search_oracle_text": search_ot,
        "search_full_oracle_text": search_fot,
        "search_type_line": search_tl or (card.get("type_line") or ""),
        "power": card.get("power"),
        "toughness": card.get("toughness"),
        "keywords": json.dumps(keywords),
        "rarity": card.get("rarity"),
        "layout": card.get("layout"),
        "color_bits": colors_to_bits(card_colors_list_for_bits(card)),
        "ci_bits": colors_to_bits(card.get("color_identity")),
        "edhrec_rank": edhrec_rank,
        "is_commander": 1 if compute_is_commander(card) else 0,
        "price_usd": _parse_price(prices.get("usd")),
        "price_usd_foil": _parse_price(prices.get("usd_foil")),
        "price_eur": _parse_price(prices.get("eur")),
        "price_tix": _parse_price(prices.get("tix")),
        "legalities_json": json.dumps(legalities, default=_json_default),
        "raw_json": card_to_raw_json(card),
    }
    return row


def refresh_search_text_from_raw(conn: sqlite3.Connection) -> int:
    """Recompute derived columns from raw_json (search text, color bits, is_commander)."""
    cur = conn.execute("SELECT id, raw_json FROM cards")
    batch: list[tuple[Any, ...]] = []
    n = 0
    for row in cur:
        try:
            card = json.loads(row["raw_json"])
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(card, dict):
            continue
        fc = flatten_card(card)
        batch.append(
            (
                fc["search_oracle_text"],
                fc["search_full_oracle_text"],
                fc["search_type_line"],
                fc["color_bits"],
                fc["ci_bits"],
                fc["is_commander"],
                row["id"],
            )
        )
        n += 1
        if len(batch) >= 5000:
            conn.executemany(
                "UPDATE cards SET search_oracle_text=?, search_full_oracle_text=?, "
                "search_type_line=?, color_bits=?, ci_bits=?, is_commander=? WHERE id=?",
                batch,
            )
            batch.clear()
    if batch:
        conn.executemany(
            "UPDATE cards SET search_oracle_text=?, search_full_oracle_text=?, "
            "search_type_line=?, color_bits=?, ci_bits=?, is_commander=? WHERE id=?",
            batch,
        )
    conn.commit()
    return n


def migrate_cards_schema(conn: sqlite3.Connection) -> None:
    """Add columns introduced after older DB versions."""
    tables = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cards'"
    )}
    if "cards" not in tables:
        return
    cols = {row[1] for row in conn.execute("PRAGMA table_info(cards)")}
    if "search_full_oracle_text" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN search_full_oracle_text TEXT")
        conn.commit()


CARD_COLUMNS = [
    "id",
    "oracle_id",
    "name",
    "mana_cost",
    "cmc",
    "type_line",
    "oracle_text",
    "search_oracle_text",
    "search_full_oracle_text",
    "search_type_line",
    "power",
    "toughness",
    "keywords",
    "rarity",
    "layout",
    "color_bits",
    "ci_bits",
    "edhrec_rank",
    "is_commander",
    "price_usd",
    "price_usd_foil",
    "price_eur",
    "price_tix",
    "legalities_json",
    "raw_json",
]

INSERT_SQL = """
INSERT INTO cards (
  id, oracle_id, name, mana_cost, cmc, type_line, oracle_text,
  search_oracle_text, search_full_oracle_text, search_type_line, power, toughness, keywords,
  rarity, layout, color_bits, ci_bits, edhrec_rank, is_commander,
  price_usd, price_usd_foil, price_eur, price_tix, legalities_json, raw_json
) VALUES (
  :id, :oracle_id, :name, :mana_cost, :cmc, :type_line, :oracle_text,
  :search_oracle_text, :search_full_oracle_text, :search_type_line, :power, :toughness, :keywords,
  :rarity, :layout, :color_bits, :ci_bits, :edhrec_rank, :is_commander,
  :price_usd, :price_usd_foil, :price_eur, :price_tix, :legalities_json, :raw_json
)
"""


def schema_ddl() -> list[str]:
    return [
        """
        CREATE TABLE IF NOT EXISTS cards (
            id TEXT PRIMARY KEY,
            oracle_id TEXT,
            name TEXT NOT NULL,
            mana_cost TEXT,
            cmc REAL,
            type_line TEXT,
            oracle_text TEXT,
            search_oracle_text TEXT,
            search_full_oracle_text TEXT,
            search_type_line TEXT,
            power TEXT,
            toughness TEXT,
            keywords TEXT,
            rarity TEXT,
            layout TEXT,
            color_bits INTEGER NOT NULL DEFAULT 0,
            ci_bits INTEGER NOT NULL DEFAULT 0,
            edhrec_rank INTEGER,
            is_commander INTEGER NOT NULL DEFAULT 0,
            price_usd REAL,
            price_usd_foil REAL,
            price_eur REAL,
            price_tix REAL,
            legalities_json TEXT NOT NULL DEFAULT '{}',
            raw_json TEXT NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_cards_name ON cards(name COLLATE NOCASE);",
        "CREATE INDEX IF NOT EXISTS idx_cards_cmc ON cards(cmc);",
        "CREATE INDEX IF NOT EXISTS idx_cards_layout ON cards(layout);",
        "CREATE INDEX IF NOT EXISTS idx_cards_rarity ON cards(rarity);",
        "CREATE INDEX IF NOT EXISTS idx_cards_edhrec ON cards(edhrec_rank);",
        "CREATE INDEX IF NOT EXISTS idx_cards_is_commander "
        "ON cards(id) WHERE is_commander = 1;",
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tag_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            tag_normalized TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            ttl_seconds INTEGER NOT NULL,
            UNIQUE(kind, tag_normalized)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS tag_cache_cards (
            parent_id INTEGER NOT NULL REFERENCES tag_cache(id) ON DELETE CASCADE,
            card_id TEXT NOT NULL,
            PRIMARY KEY (parent_id, card_id)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_tag_cache_cards_card ON tag_cache_cards(card_id);",
    ]


def connect(db_path: str | Path, *, create: bool = False) -> sqlite3.Connection:
    path = Path(db_path)
    if not create and not path.exists():
        raise FileNotFoundError(f"Database not found: {path}")
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    migrate_cards_schema(conn)
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    for stmt in schema_ddl():
        conn.execute(stmt)
    conn.commit()


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def import_oracle_cards_stream(
    conn: sqlite3.Connection,
    stream: BinaryIO,
    *,
    batch_size: int = 5000,
) -> int:
    """Import from a binary stream of JSON array (oracle_cards bulk). Returns row count."""
    init_schema(conn)
    conn.execute("DELETE FROM cards")
    batch: list[dict[str, Any]] = []
    count = 0
    parser = ijson.items(stream, "item")
    for card in parser:
        if not isinstance(card, dict):
            continue
        row = flatten_card(card)
        if not row.get("id"):
            continue
        batch.append(row)
        if len(batch) >= batch_size:
            conn.executemany(INSERT_SQL, batch)
            count += len(batch)
            batch.clear()
    if batch:
        conn.executemany(INSERT_SQL, batch)
        count += len(batch)
    conn.commit()
    return count


def import_oracle_cards_path(conn: sqlite3.Connection, path: Path) -> int:
    with path.open("rb") as f:
        return import_oracle_cards_stream(conn, f)
