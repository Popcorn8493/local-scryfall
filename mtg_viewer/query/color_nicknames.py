"""Expand Scryfall color identity nicknames (guilds, shards, wedges, four-color) to WUBRG letters."""

from __future__ import annotations

# Aligned with common Scryfall syntax (https://scryfall.com/docs/syntax).
_NICK: dict[str, str] = {
    # Guilds
    "azorius": "wu",
    "boros": "rw",
    "dimir": "ub",
    "golgari": "bg",
    "gruul": "rg",
    "izzet": "ur",
    "orzhov": "wb",
    "rakdos": "br",
    "selesnya": "gw",
    "simic": "gu",
    # Shards
    "bant": "gwu",
    "esper": "wub",
    "grixis": "ubr",
    "jund": "brg",
    "naya": "rgw",
    # Wedges
    "abzan": "wbg",
    "jeskai": "urw",
    "mardu": "rwb",
    "sultai": "bgu",
    "temur": "rug",
    # Four-color (Scryfall “missing one color” names)
    "chaos": "ubrg",  # no W
    "aggression": "wbrg",  # no U
    "altruism": "wurg",  # no B
    "growth": "wubg",  # no R
    "artifice": "wubr",  # no G
}


def expand_color_symbols(sym: str) -> str:
    """If sym is a known nickname, return WUBRG letters; else return sym unchanged."""
    s = sym.strip().lower()
    return _NICK.get(s, s)
