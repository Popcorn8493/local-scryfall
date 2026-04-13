"""Shared HTTP settings for Scryfall API compliance."""

import time
from typing import Any, Mapping, MutableMapping

import requests

# Set SCRYFALL_CONTACT_EMAIL in env or edit for your project.
DEFAULT_UA = (
    "mtg-viewer/0.1 (offline Oracle card viewer; "
    "contact: https://github.com/your/repo)"
)
DEFAULT_ACCEPT = "application/json;q=0.9,*/*;q=0.8"
MIN_API_INTERVAL_S = 0.1

_last_api_call = 0.0


def throttle_api() -> None:
    global _last_api_call
    now = time.monotonic()
    elapsed = now - _last_api_call
    if elapsed < MIN_API_INTERVAL_S:
        time.sleep(MIN_API_INTERVAL_S - elapsed)
    _last_api_call = time.monotonic()


def session(
    user_agent: str = DEFAULT_UA,
    accept: str = DEFAULT_ACCEPT,
) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": accept,
        }
    )
    return s


def api_get(
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    session: requests.Session | None = None,
    stream: bool = False,
) -> requests.Response:
    throttle_api()
    sess = session or requests.Session()
    if session is None:
        sess.headers.update({"User-Agent": DEFAULT_UA, "Accept": DEFAULT_ACCEPT})
    return sess.get(url, params=params, stream=stream, timeout=120)
