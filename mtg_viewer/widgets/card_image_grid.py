"""Scrollable card-art grid with viewport-based lazy image loads."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from PySide6.QtCore import QEvent, QObject, QRect, QRunnable, QSize, QThreadPool, QTimer, Qt, Signal, Slot
from PySide6.QtGui import QMouseEvent, QPixmap
from PySide6.QtWidgets import (
    QFrame,
    QGridLayout,
    QLabel,
    QScrollArea,
    QVBoxLayout,
    QWidget,
)

from mtg_viewer.http_client import DEFAULT_UA
from mtg_viewer.image_cache import ensure_image_on_disk

# ~MTG card aspect (63×88 mm)
_THUMB_W = 120
_THUMB_H = int(_THUMB_W * 88 / 63)
_GAP = 8
_MAX_CELLS = 600


class _ThumbSignals(QObject):
    loaded = Signal(int, int, str, object)  # version, index, card_id, Path | None


class _ThumbRunnable(QRunnable):
    def __init__(
        self,
        version: int,
        index: int,
        card_id: str,
        raw_json: str,
        data_dir: Path,
        user_agent: str,
        allow_network: bool,
        signals: _ThumbSignals,
    ) -> None:
        super().__init__()
        self.version = version
        self.index = index
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
        self.signals.loaded.emit(self.version, self.index, self.card_id, path)


class _ThumbLabel(QLabel):
    """Fixed-size thumbnail; click selects card."""

    clicked = Signal(str)

    def __init__(self, card_id: str, parent=None) -> None:
        super().__init__(parent)
        self._cid = card_id
        self.setFixedSize(_THUMB_W, _THUMB_H)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setScaledContents(False)
        self.setObjectName("CardThumb")

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit(self._cid)
        super().mouseReleaseEvent(event)


class CardImageGrid(QWidget):
    """
    Displays search results as a grid of card images.
    Loads art only for thumbnails near the viewport (plus a small buffer).
    """

    cardSelected = Signal(str)

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

        self._ids: list[str] = []
        self._labels: list[_ThumbLabel] = []
        self._ncol = 1
        self._load_version = 0
        self._scroll_ratio = 0.0
        self._inflight: set[int] = set()
        self._done: set[str] = set()

        self._signals = _ThumbSignals(self)
        self._signals.loaded.connect(self._on_thumb_loaded, Qt.ConnectionType.QueuedConnection)
        self._pool = QThreadPool(self)
        self._pool.setMaxThreadCount(6)

        self._scroll = QScrollArea(self)
        self._scroll.setWidgetResizable(True)
        self._scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._scroll.setFrameShape(QFrame.Shape.NoFrame)
        self._scroll.viewport().installEventFilter(self)

        self._inner = QWidget()
        self._grid = QGridLayout(self._inner)
        self._grid.setSpacing(_GAP)
        self._grid.setContentsMargins(8, 8, 8, 8)
        self._scroll.setWidget(self._inner)

        self._banner = QLabel(self)
        self._banner.setObjectName("GridBanner")
        self._banner.setWordWrap(True)

        lay = QVBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.addWidget(self._banner)
        lay.addWidget(self._scroll, 1)

        self._scroll.verticalScrollBar().valueChanged.connect(self._schedule_viewport_loads)
        self._resize_timer = QTimer(self)
        self._resize_timer.setSingleShot(True)
        self._resize_timer.setInterval(120)
        self._resize_timer.timeout.connect(self._on_resize_debounced)
        self._scroll.verticalScrollBar().rangeChanged.connect(self._clamp_scroll_after_layout)
        self._defer_build = False

    def set_allow_network(self, allow: bool) -> None:
        self._allow_network = allow

    def schedule_thumbnail_loads(self) -> None:
        """Call when the grid becomes visible (e.g. tab switch) to fill the viewport."""
        self._schedule_viewport_loads()

    def ensure_cells_built(self) -> None:
        """Materialize thumbnail widgets after a deferred search (List tab was active)."""
        if not self._defer_build or not self._ids:
            return
        self._defer_build = False
        self.setUpdatesEnabled(False)
        try:
            self._fill_cells()
        finally:
            self.setUpdatesEnabled(True)

    def eventFilter(self, obj, event) -> bool:  # noqa: ANN001
        if obj is self._scroll.viewport() and event.type() == QEvent.Type.Resize:
            self._resize_timer.start()
        return super().eventFilter(obj, event)

    def _on_resize_debounced(self) -> None:
        if not self._ids or self._defer_build:
            return
        ncol = self._compute_ncol()
        if ncol != self._ncol:
            self._save_scroll_state()
            self._rebuild_cells()
            QTimer.singleShot(0, self._after_ncol_change)
        else:
            QTimer.singleShot(0, self._schedule_viewport_loads)

    def _after_ncol_change(self) -> None:
        self._restore_scroll_state()
        self._schedule_viewport_loads()

    def _save_scroll_state(self) -> None:
        v = self._scroll.verticalScrollBar()
        m = max(1, v.maximum())
        self._scroll_ratio = float(v.value()) / float(m)

    def _restore_scroll_state(self) -> None:
        v = self._scroll.verticalScrollBar()
        m = v.maximum()
        target = int(self._scroll_ratio * m) if m > 0 else 0
        v.setValue(min(max(0, target), m))

    def _clamp_scroll_after_layout(self, _min: int, maxv: int) -> None:
        """Keep scroll value valid when content height changes (resize / relayout)."""
        v = self._scroll.verticalScrollBar()
        v.setValue(min(v.value(), maxv))

    def _compute_ncol(self) -> int:
        w = self._scroll.viewport().width()
        if w < 40:
            w = 400
        w = max(120, w - 16)
        return max(1, (w + _GAP) // (_THUMB_W + _GAP))

    def set_card_ids(self, ids: list[str], *, defer_cell_build: bool = False) -> None:
        truncated = len(ids) > _MAX_CELLS
        self._ids = ids[:_MAX_CELLS]
        if truncated:
            self._banner.setText(
                f"Showing first {_MAX_CELLS} of {len(ids)} results (grid limit)."
            )
        elif ids:
            self._banner.setText(f"{len(ids)} cards")
        else:
            self._banner.setText("")
        self._scroll_ratio = 0.0
        self._purge_cell_widgets()
        if not self._ids:
            self._ncol = 1
            self._inner.setMinimumWidth(0)
            self._defer_build = False
            return
        if defer_cell_build:
            self._defer_build = True
            self._ncol = 1
            self._inner.setMinimumWidth(0)
            return
        self._defer_build = False
        self.setUpdatesEnabled(False)
        try:
            self._fill_cells()
        finally:
            self.setUpdatesEnabled(True)
        QTimer.singleShot(0, self._schedule_viewport_loads)

    def _purge_cell_widgets(self) -> None:
        # Invalidate in-flight workers; drop pixmaps so resize/tab changes cannot
        # apply stale loads to recycled indices.
        self._load_version += 1
        self._inflight.clear()
        self._done.clear()

        while self._grid.count():
            item = self._grid.takeAt(0)
            if item is None:
                break
            w = item.widget()
            if w is not None:
                w.deleteLater()
        self._labels.clear()

    def _fill_cells(self) -> None:
        self._ncol = self._compute_ncol()
        for i, cid in enumerate(self._ids):
            r, c = divmod(i, self._ncol)
            lab = _ThumbLabel(cid)
            lab.clicked.connect(self.cardSelected.emit)
            self._grid.addWidget(lab, r, c)
            self._labels.append(lab)

        self._inner.setMinimumWidth(
            self._ncol * (_THUMB_W + _GAP)
            + self._grid.contentsMargins().left()
            + self._grid.contentsMargins().right()
        )

    def _rebuild_cells(self) -> None:
        """Relayout thumbnails after viewport resize (column count may change)."""
        if self._defer_build:
            return
        self.setUpdatesEnabled(False)
        try:
            self._purge_cell_widgets()
            if not self._ids:
                self._ncol = 1
                self._inner.setMinimumWidth(0)
                return
            self._fill_cells()
        finally:
            self.setUpdatesEnabled(True)

    def _schedule_viewport_loads(self) -> None:
        if not self._labels or not self._ids:
            return
        ver = self._load_version

        vp = self._scroll.viewport()
        vp_h = max(1, vp.height())
        scroll_y = self._scroll.verticalScrollBar().value()
        m = self._grid.contentsMargins()
        min_inner_w = (
            self._ncol * (_THUMB_W + _GAP) + m.left() + m.right()
            if self._ncol >= 1
            else 1
        )
        inner_w = max(self._inner.width(), min_inner_w, 1)
        # Visible band in _inner coordinates (content scrolls under the viewport).
        expand = _THUMB_H + _GAP * 2
        band = QRect(
            0,
            max(0, scroll_y - expand),
            inner_w,
            vp_h + 2 * expand,
        )

        for idx, lab in enumerate(self._labels):
            if idx >= len(self._ids):
                break
            geo = lab.geometry()
            if not band.intersects(geo):
                continue
            cid = self._ids[idx]
            if cid in self._done:
                continue
            if idx in self._inflight:
                continue
            row = self._conn.execute("SELECT raw_json FROM cards WHERE id=?", (cid,)).fetchone()
            raw_json = row[0] if row else "{}"
            self._inflight.add(idx)
            task = _ThumbRunnable(
                ver,
                idx,
                cid,
                raw_json,
                self._data_dir,
                self._ua,
                self._allow_network,
                self._signals,
            )
            self._pool.start(task)

    @Slot(int, int, str, object)
    def _on_thumb_loaded(self, version: int, index: int, card_id: str, path: object) -> None:
        if version != self._load_version:
            return
        self._inflight.discard(index)
        if index >= len(self._labels) or index >= len(self._ids):
            return
        if self._ids[index] != card_id:
            return
        self._done.add(card_id)
        lab = self._labels[index]
        if path is None:
            lab.setText("No art")
            lab.setPixmap(QPixmap())
            return
        pm = QPixmap(str(path))
        if pm.isNull():
            lab.setText("Bad img")
            lab.setPixmap(QPixmap())
            return
        lab.setText("")
        tw, th = lab.width(), lab.height()
        if tw < 8 or th < 8:
            tw, th = _THUMB_W, _THUMB_H
        scaled = pm.scaled(
            tw,
            th,
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        lab.setPixmap(scaled)

    def sizeHint(self) -> QSize:
        return QSize(400, 400)
