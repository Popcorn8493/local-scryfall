"""Debounced search line edit."""

from __future__ import annotations

from PySide6.QtCore import QTimer, Signal
from PySide6.QtWidgets import QLineEdit


class SearchBar(QLineEdit):
    textDebounced = Signal(str)

    def __init__(self, parent=None, debounce_ms: int = 230) -> None:
        super().__init__(parent)
        self.setPlaceholderText('Try: t:creature c:r  or  o:"draw a card"')
        self.setClearButtonEnabled(True)
        self.setMinimumHeight(34)
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(debounce_ms)
        self._timer.timeout.connect(self._emit_debounced)
        self.textChanged.connect(self._on_text_changed)

    def _on_text_changed(self, _: str) -> None:
        self._timer.start()

    def _emit_debounced(self) -> None:
        self.textDebounced.emit(self.text())
