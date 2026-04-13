"""Qt list model over card ids."""

from __future__ import annotations

import sqlite3
from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QPersistentModelIndex, Qt


class CardIdListModel(QAbstractListModel):
    def __init__(self, conn: sqlite3.Connection, parent=None) -> None:
        super().__init__(parent)
        self._conn = conn
        self._ids: list[str] = []
        self._names: dict[str, str] = {}
        self._prefetched: list[str] | None = None

    def set_ids(self, ids: list[str], names: list[str] | None = None) -> None:
        self.beginResetModel()
        self._ids = list(ids)
        if names is not None and len(names) == len(self._ids):
            self._prefetched = list(names)
        else:
            self._prefetched = None
        self._names.clear()
        self.endResetModel()

    def rowCount(
        self, /, parent: QModelIndex | QPersistentModelIndex = QModelIndex()
    ) -> int:  # noqa: N802
        if parent.isValid():
            return 0
        return len(self._ids)

    def data(
        self,
        index: QModelIndex | QPersistentModelIndex,
        /,
        role: int = int(Qt.ItemDataRole.DisplayRole),
    ) -> Any:  # noqa: N802
        if not index.isValid() or index.row() >= len(self._ids):
            return None
        cid = self._ids[index.row()]
        if role == int(Qt.ItemDataRole.DisplayRole):
            if self._prefetched is not None:
                return self._prefetched[index.row()]
            if cid in self._names:
                return self._names[cid]
            row = self._conn.execute("SELECT name FROM cards WHERE id=?", (cid,)).fetchone()
            name = row[0] if row else cid
            self._names[cid] = name
            return name
        if role == int(Qt.ItemDataRole.UserRole):
            return cid
        return None

    def card_id_at(self, row: int) -> str | None:
        if 0 <= row < len(self._ids):
            return self._ids[row]
        return None
