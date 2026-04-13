"""Card detail: oracle text + lazy-loaded image from disk cache."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from PySide6.QtCore import QObject, QRunnable, QThreadPool, Qt, QTimer, Signal, Slot
from PySide6.QtGui import QPixmap
from PySide6.QtWidgets import QLabel, QPlainTextEdit, QSizePolicy, QVBoxLayout, QWidget

from mtg_viewer.http_client import DEFAULT_UA
from mtg_viewer.image_cache import ensure_image_on_disk

# Typical MTG card frame is ~63mm × 88mm (width : height).
_CARD_W, _CARD_H = 63, 88
_MAX_BOX_W = 400
_MAX_BOX_H = 560


class _ImageSignals(QObject):
    loaded = Signal(int, object)  # seq, Path | None


class _ImageRunnable(QRunnable):
    def __init__(
        self,
        seq: int,
        card_id: str,
        raw_json: str,
        data_dir: Path,
        user_agent: str,
        allow_network: bool,
        signals: _ImageSignals,
    ) -> None:
        super().__init__()
        self.seq = seq
        self.card_id = card_id
        self.raw_json = raw_json
        self.data_dir = data_dir
        self.user_agent = user_agent
        self.allow_network = allow_network
        self.signals = signals

    def run(self) -> None:
        path = ensure_image_on_disk(
            self.card_id,
            self.raw_json,
            self.data_dir,
            user_agent=self.user_agent,
            allow_network=self.allow_network,
        )
        self.signals.loaded.emit(self.seq, path)


class CardDetail(QWidget):
    def __init__(
        self,
        conn: sqlite3.Connection,
        data_dir: Path,
        *,
        allow_network: bool = True,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._conn = conn
        self._data_dir = Path(data_dir)
        self._allow_network = allow_network
        self._ua = os.environ.get("SCRYFALL_USER_AGENT") or DEFAULT_UA
        self._load_seq = 0
        self._img_signals = _ImageSignals(self)
        self._img_signals.loaded.connect(self._on_image_loaded, Qt.ConnectionType.QueuedConnection)
        self._pool = QThreadPool(self)
        self._pool.setMaxThreadCount(4)

        # Full-resolution pixmap for re-scale on resize (avoid blurry stretch).
        self._source_pixmap: QPixmap | None = None

        self._image = QLabel(self)
        self._image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._image.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self._image.setObjectName("CardArtPanel")
        self._image.setText("")
        self._apply_viewport_size()

        self._text = QPlainTextEdit(self)
        self._text.setReadOnly(True)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(8, 4, 4, 4)
        lay.setSpacing(10)
        lay.addWidget(self._image, 0, Qt.AlignmentFlag.AlignHCenter)
        lay.addWidget(self._text)

    def set_allow_network(self, allow: bool) -> None:
        self._allow_network = allow

    def resizeEvent(self, event) -> None:  # noqa: ANN001
        super().resizeEvent(event)
        self._apply_viewport_size()
        self._refit_pixmap()

    def _apply_viewport_size(self) -> None:
        """Fixed card-shaped box so layout does not jump when pixmap/text changes."""
        w = min(_MAX_BOX_W, max(260, self.width() - 16))
        # Keep MTG aspect ratio for the *viewport* (letterboxed pixmap inside).
        h = int(w * _CARD_H / _CARD_W)
        h = min(h, _MAX_BOX_H)
        if self._image.width() == w and self._image.height() == h:
            return
        self._image.setFixedSize(w, h)

    def _refit_pixmap(self) -> None:
        if self._source_pixmap is None or self._source_pixmap.isNull():
            return
        self._set_pixmap_smoothed(self._source_pixmap)

    def _set_pixmap_smoothed(self, full: QPixmap) -> None:
        """Scale into the label rect, preserve aspect, smooth; center via QLabel."""
        tw = max(1, self._image.width())
        th = max(1, self._image.height())
        scaled = full.scaled(
            tw,
            th,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        self._image.setPixmap(scaled)

    def show_card(self, card_id: str) -> None:
        self._load_seq += 1
        seq = self._load_seq

        # Stable viewport: clear text overlay only; keep dark panel (no "Loading…" flash).
        self._source_pixmap = None
        self._image.clear()
        self._image.setText("")

        if not card_id:
            self._text.clear()
            return

        row = self._conn.execute("SELECT * FROM cards WHERE id=?", (card_id,)).fetchone()
        if not row:
            self._text.setPlainText("(not found)")
            return

        d = dict(row)
        raw = json.loads(d.get("raw_json") or "{}")
        lines = [
            d.get("name") or "",
            d.get("mana_cost") or "",
            d.get("type_line") or "",
            "",
            d.get("oracle_text") or raw.get("oracle_text") or "",
            "",
            f"rarity: {d.get('rarity')}",
            f"layout: {d.get('layout')}",
        ]
        self.setUpdatesEnabled(False)
        try:
            self._text.setPlainText("\n".join(lines))
        finally:
            self.setUpdatesEnabled(True)

        raw_json = d.get("raw_json") or "{}"
        task = _ImageRunnable(
            seq,
            card_id,
            raw_json,
            self._data_dir,
            self._ua,
            self._allow_network,
            self._img_signals,
        )
        self._pool.start(task)

        # After layout knows final width, refit (first paint width can be 0).
        QTimer.singleShot(0, self._apply_viewport_size)
        QTimer.singleShot(0, self._refit_pixmap)

    @Slot(int, object)
    def _on_image_loaded(self, seq: int, path: object) -> None:
        if seq != self._load_seq:
            return
        self.setUpdatesEnabled(False)
        try:
            if path is None:
                self._source_pixmap = None
                self._image.clear()
                self._image.setText("No art")
                return
            loaded = QPixmap(str(path))
            if loaded.isNull():
                self._source_pixmap = None
                self._image.clear()
                self._image.setText("Bad image")
                return
            self._image.setText("")
            self._source_pixmap = loaded
            self._apply_viewport_size()
            self._set_pixmap_smoothed(loaded)
        finally:
            self.setUpdatesEnabled(True)
