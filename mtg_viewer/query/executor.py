"""Evaluate parsed queries against SQLite + optional Scryfall API (tagger)."""

from __future__ import annotations

import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator

import requests

from mtg_viewer.http_client import DEFAULT_UA, throttle_api
from mtg_viewer.query.compiler import RemoteAtom, compile_local_predicate, pred_is_remote
from mtg_viewer.query.parser import And, Expr, Not, Or, ParseResult, Pred, SortItem

CancelFn = Callable[[], bool]


def _expr_mentions_game(expr: Expr | None) -> bool:
    """True if the user set any ``game:`` predicate (including under NOT / AND / OR)."""
    if expr is None:
        return False
    if isinstance(expr, Pred):
        return expr.key == "game"
    if isinstance(expr, Not):
        return _expr_mentions_game(expr.child)
    if isinstance(expr, (And, Or)):
        return _expr_mentions_game(expr.left) or _expr_mentions_game(expr.right)
    return False


def _merge_default_paper_filter(pr: ParseResult) -> ParseResult:
    """
    AND every query with ``game:paper`` so digital-only printings are excluded by default.

    Opt out: set env ``MTG_VIEWER_ALL_GAMES=1`` (or ``true`` / ``yes``).
    Skipped when the query already contains a ``game:`` predicate.
    """
    if os.environ.get("MTG_VIEWER_ALL_GAMES", "").lower() in ("1", "true", "yes"):
        return pr
    if _expr_mentions_game(pr.expr):
        return pr
    paper = Pred(raw="game:paper", key="game", op=":", value="paper")
    if pr.expr is None:
        return ParseResult(expr=paper, sorts=list(pr.sorts), errors=list(pr.errors))
    return ParseResult(
        expr=And(left=paper, right=pr.expr),
        sorts=list(pr.sorts),
        errors=list(pr.errors),
    )


class ExecutorError(Exception):
    """Query execution failure (e.g. offline with uncached remote predicate)."""

    def __init__(self, message: str, code: str = "error") -> None:
        super().__init__(message)
        self.code = code


def _load_all_ids(conn: sqlite3.Connection) -> set[str]:
    cur = conn.execute("SELECT id FROM cards")
    return {r[0] for r in cur.fetchall()}


def _sql_ids_for_where(conn: sqlite3.Connection, where: str, params: tuple[Any, ...]) -> set[str]:
    sql = f"SELECT id FROM cards WHERE {where}"
    cur = conn.execute(sql, params)
    return {r[0] for r in cur.fetchall()}


def _eval_pred_local(conn: sqlite3.Connection, p: Pred) -> set[str]:
    if pred_is_remote(p):
        raise RuntimeError("remote pred in local eval")
    compiled = compile_local_predicate(p)
    if compiled is None:
        return set()
    frag, params = compiled
    return _sql_ids_for_where(conn, frag, params)


def eval_expr(
    conn: sqlite3.Connection,
    expr: Expr | None,
    all_ids: set[str],
    *,
    eval_remote: Callable[[RemoteAtom], set[str]],
) -> set[str]:
    if expr is None:
        return set(all_ids)

    if isinstance(expr, Pred):
        if pred_is_remote(expr):
            return eval_remote(
                RemoteAtom(key=expr.key, value=expr.value, raw=expr.raw)
            )
        return _eval_pred_local(conn, expr)

    if isinstance(expr, Not):
        inner = eval_expr(conn, expr.child, all_ids, eval_remote=eval_remote)
        return all_ids - inner

    if isinstance(expr, And):
        a = eval_expr(conn, expr.left, all_ids, eval_remote=eval_remote)
        b = eval_expr(conn, expr.right, all_ids, eval_remote=eval_remote)
        return a & b

    if isinstance(expr, Or):
        a = eval_expr(conn, expr.left, all_ids, eval_remote=eval_remote)
        b = eval_expr(conn, expr.right, all_ids, eval_remote=eval_remote)
        return a | b

    raise TypeError(type(expr))


def sort_order_clause(sorts: list[SortItem]) -> str:
    """SQLite-compatible ORDER BY (avoid NULLS LAST for older SQLite)."""
    if not sorts:
        return "name COLLATE NOCASE ASC"
    parts: list[str] = []
    for s in sorts:
        desc = s.descending
        o = "DESC" if desc else "ASC"
        if s.key == "edhrec":
            parts.append(f"(edhrec_rank IS NULL) ASC, edhrec_rank {o}")
        elif s.key == "cmc":
            parts.append(f"(cmc IS NULL) ASC, cmc {o}")
        elif s.key == "name":
            parts.append(f"name COLLATE NOCASE {o}")
        elif s.key in ("released", "set"):
            parts.append(f"id {o}")
        elif s.key == "usd":
            parts.append(f"(price_usd IS NULL) ASC, price_usd {o}")
        elif s.key == "eur":
            parts.append(f"(price_eur IS NULL) ASC, price_eur {o}")
        elif s.key == "tix":
            parts.append(f"(price_tix IS NULL) ASC, price_tix {o}")
        elif s.key == "color":
            parts.append(f"color_bits {o}")
        else:
            parts.append(f"name COLLATE NOCASE {o}")
    return ", ".join(parts)


def fetch_sorted_rows(
    conn: sqlite3.Connection,
    ids: set[str],
    sorts: list[SortItem],
    *,
    limit: int | None = None,
) -> list[sqlite3.Row]:
    if not ids:
        return []
    order = sort_order_clause(sorts)
    id_list = list(ids)
    # Temp table path for large sets
    if len(id_list) > 500:
        conn.execute("DROP TABLE IF EXISTS _tmp_ids")
        conn.execute("CREATE TEMP TABLE _tmp_ids (id TEXT PRIMARY KEY)")
        conn.executemany("INSERT INTO _tmp_ids(id) VALUES (?)", [(i,) for i in id_list])
        sql = f"""
            SELECT cards.* FROM cards
            INNER JOIN _tmp_ids ON cards.id = _tmp_ids.id
            ORDER BY {order}
        """
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cur = conn.execute(sql)
        rows = cur.fetchall()
        conn.execute("DROP TABLE IF EXISTS _tmp_ids")
        return rows

    # batched IN for medium sets
    maxv = 32000
    if len(id_list) <= maxv:
        placeholders = ",".join("?" * len(id_list))
        sql = f"SELECT * FROM cards WHERE id IN ({placeholders}) ORDER BY {order}"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        cur = conn.execute(sql, id_list)
        return cur.fetchall()

    # chunk merge — preserve global order: use temp table anyway
    conn.execute("DROP TABLE IF EXISTS _tmp_ids")
    conn.execute("CREATE TEMP TABLE _tmp_ids (id TEXT PRIMARY KEY)")
    conn.executemany("INSERT INTO _tmp_ids(id) VALUES (?)", [(i,) for i in id_list])
    sql = f"""
        SELECT cards.* FROM cards
        INNER JOIN _tmp_ids ON cards.id = _tmp_ids.id
        ORDER BY {order}
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur = conn.execute(sql)
    rows = cur.fetchall()
    conn.execute("DROP TABLE IF EXISTS _tmp_ids")
    return rows


def search_scryfall_cards(
    q: str,
    *,
    session: requests.Session | None = None,
    cancel: CancelFn | None = None,
) -> Iterator[dict[str, Any]]:
    """Paginate /cards/search for given q."""
    sess = session or requests.Session()
    url = "https://api.scryfall.com/cards/search"
    page_url: str | None = url
    params: dict[str, Any] = {"q": q}
    while page_url:
        if cancel and cancel():
            break
        throttle_api()
        if page_url == url:
            r = sess.get(url, params=params, timeout=60)
        else:
            r = sess.get(page_url, timeout=60)
        r.raise_for_status()
        data = r.json()
        for obj in data.get("data") or []:
            yield obj
        if data.get("has_more") and data.get("next_page"):
            page_url = data["next_page"]
            params = {}
        else:
            break


def tag_cache_lookup(
    conn: sqlite3.Connection, kind: str, tag: str
) -> tuple[set[str], bool]:
    """Return (ids, hit_nonexpired)."""
    row = conn.execute(
        "SELECT id, fetched_at, ttl_seconds FROM tag_cache WHERE kind=? AND tag_normalized=?",
        (kind, tag),
    ).fetchone()
    if not row:
        return set(), False
    parent_id = row[0]
    fetched = row[1]
    ttl = int(row[2])
    try:
        from datetime import datetime, timezone

        t0 = datetime.fromisoformat(fetched.replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - t0.astimezone(timezone.utc)).total_seconds()
    except Exception:
        age = ttl + 1
    if age > ttl:
        return set(), False
    cur = conn.execute(
        "SELECT card_id FROM tag_cache_cards WHERE parent_id=?", (parent_id,)
    )
    ids = {r[0] for r in cur.fetchall()}
    return ids, True


def tag_cache_store(
    conn: sqlite3.Connection,
    kind: str,
    tag: str,
    ids: set[str],
    *,
    ttl_seconds: int = 7 * 24 * 3600,
) -> None:
    conn.execute("DELETE FROM tag_cache WHERE kind=? AND tag_normalized=?", (kind, tag))
    conn.execute(
        "INSERT INTO tag_cache(kind, tag_normalized, fetched_at, ttl_seconds) VALUES(?,?,?,?)",
        (kind, tag, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), ttl_seconds),
    )
    row = conn.execute(
        "SELECT id FROM tag_cache WHERE kind=? AND tag_normalized=?",
        (kind, tag),
    ).fetchone()
    assert row
    pid = row[0]
    conn.executemany(
        "INSERT OR IGNORE INTO tag_cache_cards(parent_id, card_id) VALUES(?,?)",
        [(pid, i) for i in ids],
    )
    conn.commit()


def remote_atom_to_query(atom: RemoteAtom) -> str:
    if atom.key == "otag":
        return f"otag:{atom.value}"
    if atom.key == "art":
        return f"art:{atom.value}"
    return atom.raw


@dataclass
class ExecuteConfig:
    allow_network: bool = True
    user_agent: str = DEFAULT_UA
    cancel: CancelFn | None = None


def execute_parse_result(
    conn: sqlite3.Connection,
    pr: ParseResult,
    *,
    cfg: ExecuteConfig | None = None,
) -> list[sqlite3.Row]:
    cfg = cfg or ExecuteConfig()
    if pr.errors:
        raise ExecutorError("; ".join(pr.errors), code="parse")
    pr = _merge_default_paper_filter(pr)

    def make_remote_eval() -> Callable[[RemoteAtom], set[str]]:
        def ev(atom: RemoteAtom) -> set[str]:
            ids, ok = tag_cache_lookup(conn, atom.key, atom.value)
            if ok:
                return ids
            if not cfg.allow_network:
                raise ExecutorError(
                    f"Remote predicate {atom.key}:{atom.value} requires network or cache",
                    code="offline_remote",
                )
            q = remote_atom_to_query(atom)
            out: set[str] = set()
            sess = requests.Session()
            sess.headers.update({"User-Agent": cfg.user_agent, "Accept": "application/json"})
            try:
                for card in search_scryfall_cards(q, session=sess, cancel=cfg.cancel):
                    oid = card.get("oracle_id")
                    sid = card.get("id")
                    row = None
                    for x in (oid, sid):
                        if not x:
                            continue
                        row = conn.execute(
                            "SELECT id FROM cards WHERE oracle_id = ? OR id = ?",
                            (str(x), str(x)),
                        ).fetchone()
                        if row:
                            break
                    if row:
                        out.add(str(row[0]))
            except Exception as e:
                raise ExecutorError(str(e), code="network") from e
            tag_cache_store(conn, atom.key, atom.value, out)
            return out

        return ev

    all_ids = _load_all_ids(conn)
    result_ids = eval_expr(
        conn, pr.expr, all_ids, eval_remote=make_remote_eval()
    )

    rows = fetch_sorted_rows(conn, result_ids, pr.sorts)
    return rows
