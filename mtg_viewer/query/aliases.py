"""Canonical predicate keys and aliases (single source of truth)."""

from __future__ import annotations

# Predicate key aliases -> canonical key (syntax + Tagger; see Scryfall docs)
PREDICATE_ALIASES: dict[str, str] = {
    # Tagger / oracle & art tags
    "function": "otag",
    "oracletag": "otag",
    "oracle_tag": "otag",
    "atag": "art",
    "arttag": "art",
    "art_tag": "art",
    # Mana value (same column as cmc for oracle cards)
    "manavalue": "mv",
    # Format shorthand (value side — handled in compiler)
    "f": "f",
    "format": "f",
}

# sort: aliases
SORT_ALIASES: dict[str, str] = {
    "edhrec": "edhrec",
    "cmc": "cmc",
    "mv": "cmc",
    "name": "name",
    "released": "released",
    "release": "released",
    "set": "set",
    "usd": "usd",
    "eur": "eur",
    "euro": "eur",
    "tix": "tix",
}


def canonical_predicate_key(key: str) -> str:
    k = key.lower().strip()
    return PREDICATE_ALIASES.get(k, k)


def canonical_sort_key(key: str) -> str:
    k = key.lower().strip()
    return SORT_ALIASES.get(k, k)
