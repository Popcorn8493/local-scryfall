"""Compile predicates to local SQL fragments; mark remote keys."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mtg_viewer.db import COLOR_B, COLOR_G, COLOR_R, COLOR_U, COLOR_W
from mtg_viewer.query.color_nicknames import expand_color_symbols
from mtg_viewer.query.parser import And, Expr, Not, Or, Pred

REMOTE_KEYS = frozenset({"otag", "art"})

# f: shorthand -> JSON key under legalities
FORMAT_KEYS = {
    "standard": "standard",
    "future": "future",
    "historic": "historic",
    "gladiator": "gladiator",
    "pioneer": "pioneer",
    "explorer": "explorer",
    "modern": "modern",
    "legacy": "legacy",
    "pauper": "pauper",
    "vintage": "vintage",
    "penny": "penny",
    "commander": "commander",
    "edh": "commander",
    "duel": "duel",
    "oldschool": "oldschool",
    "premodern": "premodern",
    "predh": "predh",
    "oathbreaker": "oathbreaker",
    "brawl": "standardbrawl",
    "standardbrawl": "standardbrawl",
    "historicbrawl": "historicbrawl",
    "alchemy": "alchemy",
    "paupercommander": "paupercommander",
    "timeless": "timeless",
}


def _color_bits_from_symbols(sym: str) -> int:
    bits = 0
    s = expand_color_symbols(sym).lower().strip()
    for ch in s:
        if ch == "w":
            bits |= COLOR_W
        elif ch == "u":
            bits |= COLOR_U
        elif ch == "b":
            bits |= COLOR_B
        elif ch == "r":
            bits |= COLOR_R
        elif ch == "g":
            bits |= COLOR_G
        elif ch == "c" or ch == "m":
            pass  # colorless - no colored bits
    return bits


def pred_is_remote(p: Pred) -> bool:
    return p.key in REMOTE_KEYS


_ORACLE_SEARCH_COL = "search_oracle_text"
_FULL_ORACLE_SEARCH_COL = "search_full_oracle_text"


def _text_match_value(v: str) -> str:
    """Unescape and strip outer quotes from o:/t:/kw: values (e.g. o:\\\"this card\\\")."""
    v = v.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        v = v[1:-1].replace('\\"', '"')
    return v.replace("\\", "")


def _oracle_text_predicate_sql(
    tv: str,
    *,
    oracle_column: str = _ORACLE_SEARCH_COL,
) -> tuple[str, tuple[Any, ...]]:
    """
    Scryfall treats ~ in oracle search as “this card’s name”.

    Per https://scryfall.com/docs/syntax : ``o:`` / ``oracle:`` match rules text **without**
    reminder text → ``search_oracle_text``. ``fo:`` / ``fulloracle:`` include reminder text
    → ``search_full_oracle_text``.
    """
    if oracle_column not in (_ORACLE_SEARCH_COL, _FULL_ORACLE_SEARCH_COL):
        raise ValueError(oracle_column)
    col = oracle_column
    tv = tv.strip()
    if not tv:
        return ("1=0", ())
    if "~" not in tv:
        return (
            f"(lower({col}) LIKE '%' || lower(?) || '%')",
            (tv,),
        )
    # Literal ~; ~ → name (no space); ~ → " " + name (common on printed cards, e.g. "Discard Sol Ring:")
    return (
        "("
        f"(lower({col}) LIKE '%' || lower(?) || '%') OR "
        f"(lower({col}) LIKE '%' || lower(replace(?, '~', name)) || '%') OR "
        f"(lower({col}) LIKE '%' || lower(replace(?, '~', (' ' || name))) || '%')"
        ")",
        (tv, tv, tv),
    )


def _bare_word_name_term(p: Pred) -> str | None:
    """Scryfall-style: a token with no key:value form is a name search (substring)."""
    if p.op != "" or p.value != "":
        return None
    raw = p.raw.strip()
    if not raw or ":" in raw:
        return None
    # Quoted phrase from tokenizer: "Lightning Bolt"
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        return raw[1:-1].replace('\\"', '"')
    return p.key.strip()


def compile_local_predicate(p: Pred) -> tuple[str, tuple[Any, ...]] | None:
    """Return (WHERE fragment without leading AND, params) or None if remote/unsupported."""
    if pred_is_remote(p):
        return None
    k = p.key
    op = p.op
    v = p.value

    bare = _bare_word_name_term(p)
    if bare is not None:
        return ("(name COLLATE NOCASE LIKE '%' || ? || '%')", (bare,))

    if k == "f" or k == "format":
        fk = FORMAT_KEYS.get(v.lower().strip(), v.lower().strip())
        jpath = f"$.{fk}"
        return ("json_extract(legalities_json, ?) = ?", (jpath, "legal"))

    if k in ("o", "oracle"):
        tv = _text_match_value(v)
        return _oracle_text_predicate_sql(tv, oracle_column=_ORACLE_SEARCH_COL)
    if k in ("fo", "fulloracle"):
        tv = _text_match_value(v)
        return _oracle_text_predicate_sql(tv, oracle_column=_FULL_ORACLE_SEARCH_COL)

    if k in ("t", "type"):
        tv = _text_match_value(v)
        return (
            "(lower(search_type_line) LIKE '%' || lower(?) || '%')",
            (tv,),
        )

    if k in ("name", "n"):
        return ("(name COLLATE NOCASE LIKE '%' || ? || '%')", (v,))

    # cmc / mv (mana value) — card-level column; not summed across faces (oracle_cards model)
    if k in ("cmc", "mv") and op in ("<", "<=", ">", ">=", "=", ":"):
        try:
            num = float(v)
        except ValueError:
            return ("1=0", ())
        cmp_op = "=" if op == ":" else op
        m = {"<": "<", "<=": "<=", ">": ">", ">=": ">=", "=": "="}[cmp_op]
        return (f"(cmc IS NOT NULL AND cmc {m} ?)", (num,))

    if k == "r" or k == "rarity":
        return ("(lower(rarity) = lower(?))", (v,))

    if k in ("usd", "eur", "tix") and op in ("<", "<=", ">", ">=", "=", ":"):
        return _price_predicate_sql(k, op, v)

    if k in ("s", "e", "set", "edition"):
        code = _text_match_value(v).strip().lower()
        if not code:
            return ("1=0", ())
        return (
            "(lower(json_extract(raw_json, '$.set')) = ?)",
            (code,),
        )

    if k in ("c", "color"):
        bits = _color_bits_from_symbols(v)
        if op in ("=", ":") or op == "":
            return ("(color_bits = ?)", (bits,))
        if op == ">=":
            return ("((color_bits & ?) = ?)", (bits, bits))
        if op == "<=":
            # subset of colors in v: color_bits & ~bits == 0
            return ("((color_bits & (~? & 31)) = 0)", (bits,))

    # id:/identity:/ci:/commander: — Scryfall: `id:esper` / `id<=esper` = identity subset (deckbuilding);
    # `id=esper` = exact color identity match. See https://scryfall.com/docs/syntax (Colors).
    if k in ("id", "identity", "ci", "commander"):
        bits = _color_bits_from_symbols(v)
        if op == "=":
            return ("(ci_bits = ?)", (bits,))
        if op in (":", "") or op == "<=":
            return ("((ci_bits & (~? & 31)) = 0)", (bits,))
        if op == ">=":
            return ("((ci_bits & ?) = ?)", (bits, bits))

    # game:paper|mtgo|arena — Scryfall $.games (per printing). Oracle bulk may use an MTGO-only
    # reprint while `game:paper` still matches the oracle (paper exists on another printing); allow
    # MTGO + multiverse_ids as a proxy for “paper printing exists on Gatherer.”
    if k == "game" and op in (":", "="):
        g = v.lower().strip()
        if g not in ("paper", "mtgo", "arena"):
            return ("1=0", ())
        if g == "paper":
            return (
                "("
                "EXISTS (SELECT 1 FROM json_each(json_extract(raw_json, '$.games')) WHERE value = 'paper') "
                "OR ("
                "EXISTS (SELECT 1 FROM json_each(json_extract(raw_json, '$.games')) WHERE value = 'mtgo') "
                "AND COALESCE(json_array_length(json_extract(raw_json, '$.multiverse_ids')), 0) > 0"
                ")"
                ")",
                (),
            )
        return (
            "(EXISTS (SELECT 1 FROM json_each(json_extract(raw_json, '$.games')) WHERE value = ?))",
            (g,),
        )

    # prefer:* — print/display preference on Scryfall; does not filter card set locally
    if k == "prefer" and op in (":", "="):
        return ("1=1", ())

    if k in ("m", "mana"):
        return ("(mana_cost IS NOT NULL AND mana_cost LIKE '%' || ? || '%')", (v,))

    if k == "pow" or k == "power":
        return _power_clause(op, v)

    if k == "tou" or k == "toughness":
        return _tou_clause(op, v)

    if k == "is":
        return _is_clause(v.lower().strip())

    if k in ("kw", "keyword"):
        tv = _text_match_value(v)
        return (
            "(lower(keywords) LIKE '%' || lower(?) || '%')",
            (tv,),
        )

    return ("1=0", ())


def _price_predicate_sql(
    currency: str, op: str, v: str
) -> tuple[str, tuple[Any, ...]]:
    col = {"usd": "price_usd", "eur": "price_eur", "tix": "price_tix"}[currency]
    try:
        num = float(v.strip())
    except ValueError:
        return ("1=0", ())
    cmp_op = "=" if op == ":" else op
    m = {"<": "<", "<=": "<=", ">": ">", ">=": ">=", "=": "="}[cmp_op]
    return (f"({col} IS NOT NULL AND {col} {m} ?)", (num,))


def _power_clause(op: str, v: str) -> tuple[str, tuple[Any, ...]]:
    if v in ("*", "x"):
        return ("1=1", ())
    try:
        n = int(float(v))
    except ValueError:
        return ("(power = ?)", (v,))
    m = op if op in ("<", "<=", ">", ">=", "=", ":") else "="
    if m == ":":
        m = "="
    if m == "=":
        return ("(CAST(power AS REAL) = ?)", (float(n),))
    return (f"(CAST(power AS REAL) {m} ?)", (float(n),))


def _tou_clause(op: str, v: str) -> tuple[str, tuple[Any, ...]]:
    if v in ("*", "x"):
        return ("1=1", ())
    try:
        n = int(float(v))
    except ValueError:
        return ("(toughness = ?)", (v,))
    m = op if op in ("<", "<=", ">", ">=", "=", ":") else "="
    if m == ":":
        m = "="
    if m == "=":
        return ("(CAST(toughness AS REAL) = ?)", (float(n),))
    return (f"(CAST(toughness AS REAL) {m} ?)", (float(n),))


def _is_clause(kind: str) -> tuple[str, tuple[Any, ...]]:
    if kind == "commander":
        return ("(is_commander = 1)", ())
    # is:etb — ETB-style triggers (Scryfall-like): ~ / this … / printed name, shorthand
    # “enters”, and “whenever this creature … enters” with text between.
    if kind == "etb":
        col = _ORACLE_SEARCH_COL
        return (
            "("
            f"lower({col}) LIKE '%when ~ enters the battlefield%' "
            f"OR lower({col}) LIKE '%whenever ~ enters the battlefield%' "
            f"OR lower({col}) LIKE '%as ~ enters the battlefield%' "
            f"OR lower({col}) LIKE '%when ~ enters tapped%' "
            f"OR lower({col}) LIKE '%whenever ~ enters tapped%' "
            f"OR lower({col}) LIKE '%when ~ enters the battlefield tapped%' "
            f"OR lower({col}) LIKE '%when ~ enters play%' "
            f"OR lower({col}) LIKE '%whenever ~ enters play%' "
            f"OR lower({col}) LIKE '%when this creature enters the battlefield%' "
            f"OR lower({col}) LIKE '%whenever this creature enters the battlefield%' "
            f"OR lower({col}) LIKE '%when this planeswalker enters the battlefield%' "
            f"OR lower({col}) LIKE '%when this land enters the battlefield%' "
            f"OR lower({col}) LIKE '%when this enchantment enters the battlefield%' "
            f"OR lower({col}) LIKE '%when this artifact enters the battlefield%' "
            f"OR lower({col}) LIKE '%when this creature enters%' "
            f"OR lower({col}) LIKE '%whenever this creature enters%' "
            f"OR (lower({col}) LIKE '%enters the battlefield%' "
            f"AND lower({col}) LIKE '%~%') "
            f"OR lower({col}) LIKE ('%when ' || lower(name) || ' enters the battlefield%') "
            f"OR lower({col}) LIKE "
            "('%whenever ' || lower(name) || ' enters the battlefield%') "
            f"OR lower({col}) LIKE ('%when ' || lower(name) || ' enters%') "
            f"OR lower({col}) LIKE ('%whenever ' || lower(name) || ' enters%') "
            f"OR lower({col}) LIKE '%whenever this creature%enters%' "
            f"OR lower({col}) LIKE '%when this creature%enters%'"
            ")",
            (),
        )
    if kind in ("split",):
        return ("(layout = 'split')", ())
    if kind in ("dfc", "mdfc", "meld"):
        return (
            "(layout IN ('transform','modal_dfc','double_faced_token','art_series'))",
            (),
        )
    if kind == "funny":
        return ("(lower(raw_json) LIKE '%\"funny\"%')", ())  # weak; prefer set
    if kind in ("permanent", "spell", "historic", "reserved"):
        # Minimal stubs — expand with type_line / set_type from raw_json
        return ("1=1", ())
    return ("1=0", ())


@dataclass
class RemoteAtom:
    key: str
    value: str
    raw: str


def collect_remote(expr: Expr | None) -> list[RemoteAtom]:
    out: list[RemoteAtom] = []

    def walk(e: Expr) -> None:
        if isinstance(e, Pred):
            if pred_is_remote(e):
                out.append(RemoteAtom(key=e.key, value=e.value, raw=e.raw))
        elif isinstance(e, Not):
            walk(e.child)
        elif isinstance(e, And):
            walk(e.left)
            walk(e.right)
        elif isinstance(e, Or):
            walk(e.left)
            walk(e.right)

    if expr:
        walk(expr)
    return out
