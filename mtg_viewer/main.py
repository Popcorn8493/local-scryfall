"""PySide6 GUI entry.

Search uses a debounced ``SearchBar`` (~230 ms), ``QThreadPool`` workers, and passes a
threading ``Event`` to ``ExecuteConfig.cancel`` so paginated tagger/API work stops when
the user types again (see ``SearchRunnable``).
"""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

from PySide6.QtCore import QModelIndex, QObject, QRunnable, QThreadPool, QTimer, Signal, Slot
from PySide6.QtWidgets import (
    QApplication,
    QLabel,
    QMainWindow,
    QSplitter,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from mtg_viewer.db import connect
from mtg_viewer.http_client import DEFAULT_UA
from mtg_viewer.models import CardIdListModel
from mtg_viewer.query.executor import ExecuteConfig, ExecutorError, execute_parse_result
from mtg_viewer.query.parser import parse_query
from mtg_viewer.theme import apply_theme
from mtg_viewer.widgets.card_detail import CardDetail
from mtg_viewer.widgets.card_image_grid import CardImageGrid
from mtg_viewer.widgets.card_list import CardList
from mtg_viewer.widgets.search_bar import SearchBar


class SearchSignals(QObject):
    finished = Signal(list, list, str)  # ids, display names (parallel), error message
    failed = Signal(str)


class SearchRunnable(QRunnable):
    def __init__(
        self,
        db_path: Path,
        query: str,
        cancel: threading.Event,
        allow_network: bool,
    ) -> None:
        super().__init__()
        self.db_path = db_path
        self.query = query
        self.cancel = cancel
        self.allow_network = allow_network
        self.signals = SearchSignals()

    def run(self) -> None:  # noqa: D102
        conn = connect(self.db_path, create=False)
        try:
            pr = parse_query(self.query)
            cfg = ExecuteConfig(
                allow_network=self.allow_network,
                user_agent=os.environ.get("SCRYFALL_USER_AGENT") or DEFAULT_UA,
                cancel=lambda: self.cancel.is_set(),
            )
            rows = execute_parse_result(conn, pr, cfg=cfg)
            ids = [str(r["id"]) for r in rows]
            names = [str(r["name"] or "") for r in rows]
            self.signals.finished.emit(ids, names, "")
        except ExecutorError as e:
            self.signals.finished.emit([], [], str(e))
        except Exception as e:
            self.signals.failed.emit(str(e))
        finally:
            conn.close()


class MainWindow(QMainWindow):
    def __init__(self, db_path: Path, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Offline MTG Viewer — Scryfall-style search")
        self._db_path = db_path
        self._data_dir = db_path.parent
        self._conn = connect(db_path, create=False)
        self._pool = QThreadPool.globalInstance()
        self._cancel = threading.Event()
        self._allow_network = True

        root = QWidget()
        self.setCentralWidget(root)
        lay = QVBoxLayout(root)
        lay.setContentsMargins(14, 14, 14, 12)
        lay.setSpacing(8)
        self._search = SearchBar()
        query_lbl = QLabel("Search")
        query_lbl.setObjectName("QueryLabel")
        lay.addWidget(query_lbl)
        lay.addWidget(self._search)

        split = QSplitter()
        self._list = CardList()
        self._model = CardIdListModel(self._conn)
        self._list.setModel(self._model)
        img_offline = os.environ.get("MTG_VIEWER_IMAGE_OFFLINE", "").lower() in (
            "1",
            "true",
            "yes",
        )
        allow_img = not img_offline
        self._detail = CardDetail(
            self._conn,
            self._data_dir,
            allow_network=allow_img,
        )
        self._grid = CardImageGrid(
            self._conn,
            self._data_dir,
            allow_network=allow_img,
        )

        self._result_tabs = QTabWidget()
        self._result_tabs.addTab(self._list, "List")
        self._result_tabs.addTab(self._grid, "Images")
        split.addWidget(self._result_tabs)
        split.addWidget(self._detail)
        split.setStretchFactor(0, 1)
        split.setStretchFactor(1, 1)
        split.setHandleWidth(3)
        lay.addWidget(split, 1)

        self._search.textDebounced.connect(self._on_search)
        self._list.currentCardChanged.connect(self._detail.show_card)
        self._grid.cardSelected.connect(self._on_grid_card)
        self._result_tabs.currentChanged.connect(self._on_result_tab_changed)

    def closeEvent(self, event) -> None:  # noqa: ANN001
        self._cancel.set()
        self._conn.close()
        super().closeEvent(event)

    @Slot(str)
    def _on_search(self, text: str) -> None:
        self._cancel.set()
        self._cancel = threading.Event()
        task = SearchRunnable(
            self._db_path,
            text,
            self._cancel,
            self._allow_network,
        )
        task.signals.finished.connect(self._apply_results)
        task.signals.failed.connect(self._on_fail)
        self._pool.start(task)

    @Slot(list, list, str)
    def _apply_results(self, ids: list, names: list, err: str) -> None:
        if err:
            self.statusBar().showMessage(err, 8000)
            self._model.set_ids([])
            self._grid.set_card_ids([])
            return
        n = len(ids)
        self.statusBar().showMessage(f"{n} card{'s' if n != 1 else ''} found")
        self._model.set_ids(ids, names)
        defer_grid = self._result_tabs.currentWidget() is not self._grid
        self._result_tabs.setUpdatesEnabled(False)
        try:
            self._grid.set_card_ids(ids, defer_cell_build=defer_grid)
        finally:
            self._result_tabs.setUpdatesEnabled(True)

    @Slot(int)
    def _on_result_tab_changed(self, index: int) -> None:
        if self._result_tabs.widget(index) is self._grid:
            self._grid.ensure_cells_built()
            QTimer.singleShot(0, self._grid.schedule_thumbnail_loads)

    @Slot(str)
    def _on_grid_card(self, card_id: str) -> None:
        self._detail.show_card(card_id)
        if not card_id:
            return
        for row in range(self._model.rowCount()):
            if self._model.card_id_at(row) == card_id:
                self._list.setCurrentIndex(self._model.index(row, 0, QModelIndex()))
                break

    @Slot(str)
    def _on_fail(self, msg: str) -> None:
        self.statusBar().showMessage(msg, 8000)


def run_gui() -> None:
    data_dir = Path(os.environ.get("MTG_VIEWER_DATA", "data"))
    db_path = data_dir / "cards.db"
    if not db_path.exists():
        raise SystemExit(f"No database at {db_path}. Run: python -m mtg_viewer update")
    app = QApplication([])
    apply_theme(app)
    w = MainWindow(db_path)
    w.setMinimumSize(760, 520)
    w.resize(1100, 720)
    w.show()
    app.exec()


if __name__ == "__main__":
    run_gui()
