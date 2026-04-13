"""Lazy-download card art to disk (keyed by Scryfall card id)."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

from mtg_viewer.http_client import DEFAULT_UA, throttle_api

# Shared with API throttling so bursts of image loads stay polite.
IMAGE_TIMEOUT_S = 60


def pick_image_url(raw: dict[str, Any]) -> str | None:
    """Prefer normal-sized URI; handle top-level and card_faces (DFC/MDFC)."""
    iu = raw.get("image_uris")
    if isinstance(iu, dict):
        url = iu.get("normal") or iu.get("large") or iu.get("png") or iu.get("small")
        if url:
            return str(url)
    for face in raw.get("card_faces") or []:
        fiu = face.get("image_uris") if isinstance(face, dict) else None
        if isinstance(fiu, dict):
            url = fiu.get("normal") or fiu.get("large") or fiu.get("png") or fiu.get("small")
            if url:
                return str(url)
    return None


def _suffix_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    if path.endswith(".png"):
        return ".png"
    if path.endswith(".jpg") or path.endswith(".jpeg"):
        return ".jpg"
    return ".jpg"


def cache_dir(data_dir: Path) -> Path:
    d = Path(data_dir) / "images"
    d.mkdir(parents=True, exist_ok=True)
    return d


def cache_path_for_card(data_dir: Path, card_id: str, url: str) -> Path:
    """Stable path on disk; extension follows Scryfall CDN URL when possible."""
    safe = re.sub(r"[^\w\-.]", "_", card_id)
    return cache_dir(data_dir) / f"{safe}{_suffix_from_url(url)}"


def ensure_image_on_disk(
    card_id: str,
    raw_json: str,
    data_dir: Path,
    *,
    user_agent: str = DEFAULT_UA,
    allow_network: bool = True,
) -> Path | None:
    """
    If a cached file exists, return it.
    Otherwise download from image_uris using *allow_network*.
    Returns None when there is no image URI, network disabled and missing, or HTTP error.
    """
    try:
        raw = json.loads(raw_json or "{}")
    except json.JSONDecodeError:
        return None
    url = pick_image_url(raw)
    if not url:
        return None

    dest = cache_path_for_card(data_dir, card_id, url)
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    if not allow_network:
        return None

    throttle_api()
    sess = requests.Session()
    sess.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "image/*,*/*;q=0.8",
        }
    )
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    try:
        r = sess.get(url, stream=True, timeout=IMAGE_TIMEOUT_S)
        r.raise_for_status()
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)
        return dest
    except (OSError, requests.RequestException):
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        return None
