"""List view for card names."""

from __future__ import annotations

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QAbstractItemView, QListView


class CardList(QListView):
    currentCardChanged = Signal(str)

    def __init__(self, parent=None) -> None:
        super().__init__(parent)
        self.setAlternatingRowColors(True)
        self.setUniformItemSizes(True)
        self.setSpacing(2)
        self.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self._selection_connected = False

    def setModel(self, model):  # noqa: N802
        sm = self.selectionModel()
        if sm is not None and self._selection_connected:
            sm.currentChanged.disconnect(self._on_current)
            self._selection_connected = False
        super().setModel(model)
        sm = self.selectionModel()
        if sm is not None:
            sm.currentChanged.connect(self._on_current)
            self._selection_connected = True

    def _on_current(self, current, _previous) -> None:  # noqa: ANN001
        # Qt6: currentChanged(QModelIndex, QModelIndex) — not QItemSelection
        idx = current
        if not idx.isValid():
            self.currentCardChanged.emit("")
            return
        cid = idx.data(Qt.ItemDataRole.UserRole)
        self.currentCardChanged.emit(str(cid) if cid else "")
