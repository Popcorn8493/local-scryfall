# Offline MTG Viewer

Local SQLite + Scryfall `oracle_cards` bulk data with a Scryfall-style query bar and optional tagger predicates (hybrid API + cache).

## Setup

```bash
pip install -e ".[dev]"
```

## Refresh card data

```bash
python -m mtg_viewer update
```

Data is stored under `data/cards.db` (override with `MTG_VIEWER_DATA`).

## CLI search

```bash
python -m mtg_viewer "f:edh cmc<3"
```

## GUI

```bash
python -m mtg_viewer.main
```
