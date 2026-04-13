"""Application-wide Fusion styling and QSS for the MTG viewer."""

from __future__ import annotations

from PySide6.QtGui import QColor, QFont, QPalette
from PySide6.QtWidgets import QApplication


# Cohesive dark palette (readable contrast, subtle surfaces).
_BG = "#1a1b20"
_SURFACE = "#22242c"
_SURFACE_ELEVATED = "#2a2d38"
_BORDER = "#3d4150"
_TEXT = "#e6e4df"
_TEXT_MUTED = "#9896a3"
_ACCENT = "#c4a035"
_ACCENT_DIM = "#8f7a28"
_FOCUS = "#6b9bd4"


def apply_theme(app: QApplication) -> None:
    app.setStyle("Fusion")

    base = QFont()
    base.setPointSizeF(10.0)
    app.setFont(base)

    pal = QPalette()
    pal.setColor(QPalette.ColorRole.Window, QColor(_BG))
    pal.setColor(QPalette.ColorRole.WindowText, QColor(_TEXT))
    pal.setColor(QPalette.ColorRole.Base, QColor(_SURFACE))
    pal.setColor(QPalette.ColorRole.AlternateBase, QColor(_SURFACE_ELEVATED))
    pal.setColor(QPalette.ColorRole.Text, QColor(_TEXT))
    pal.setColor(QPalette.ColorRole.Button, QColor(_SURFACE_ELEVATED))
    pal.setColor(QPalette.ColorRole.ButtonText, QColor(_TEXT))
    pal.setColor(QPalette.ColorRole.Highlight, QColor(_ACCENT))
    pal.setColor(QPalette.ColorRole.HighlightedText, QColor("#1a1b20"))
    pal.setColor(QPalette.ColorRole.PlaceholderText, QColor(_TEXT_MUTED))
    app.setPalette(pal)

    app.setStyleSheet(
        f"""
        QMainWindow {{
            background-color: {_BG};
        }}
        QWidget {{
            color: {_TEXT};
        }}
        QLabel#QueryLabel {{
            font-weight: 600;
            color: {_TEXT};
            padding-bottom: 2px;
        }}
        QLabel#GridBanner {{
            color: {_TEXT_MUTED};
            font-size: 11px;
            padding: 4px 2px 8px 2px;
        }}
        QLineEdit {{
            background-color: {_SURFACE};
            border: 1px solid {_BORDER};
            border-radius: 6px;
            padding: 6px 10px;
            min-height: 20px;
            selection-background-color: {_ACCENT_DIM};
            selection-color: {_TEXT};
        }}
        QLineEdit:focus {{
            border: 1px solid {_FOCUS};
        }}
        QLineEdit:hover:!focus {{
            border: 1px solid #4a5062;
        }}
        QListView {{
            background-color: {_SURFACE};
            border: 1px solid {_BORDER};
            border-radius: 6px;
            padding: 4px;
            outline: none;
        }}
        QListView::item {{
            padding: 6px 8px;
            border-radius: 4px;
        }}
        QListView::item:selected {{
            background-color: {_ACCENT_DIM};
            color: {_TEXT};
        }}
        QListView::item:hover:!selected {{
            background-color: {_SURFACE_ELEVATED};
        }}
        QPlainTextEdit {{
            background-color: {_SURFACE};
            color: {_TEXT};
            border: 1px solid {_BORDER};
            border-radius: 6px;
            padding: 10px;
            font-family: "Segoe UI", "SF Pro Text", "Roboto", sans-serif;
        }}
        QTabWidget::pane {{
            border: 1px solid {_BORDER};
            border-radius: 8px;
            top: -1px;
            background-color: {_BG};
            padding: 4px;
        }}
        QTabBar::tab {{
            background-color: {_SURFACE};
            color: {_TEXT_MUTED};
            border: 1px solid {_BORDER};
            border-bottom: none;
            border-top-left-radius: 6px;
            border-top-right-radius: 6px;
            padding: 8px 16px;
            margin-right: 2px;
        }}
        QTabBar::tab:selected {{
            background-color: {_SURFACE_ELEVATED};
            color: {_TEXT};
            font-weight: 600;
            border-bottom: 2px solid {_ACCENT};
        }}
        QTabBar::tab:hover:!selected {{
            color: {_TEXT};
            background-color: {_SURFACE_ELEVATED};
        }}
        QScrollArea {{
            border: none;
            background-color: transparent;
        }}
        QScrollBar:vertical {{
            background-color: {_SURFACE};
            width: 12px;
            border-radius: 6px;
            margin: 0;
        }}
        QScrollBar::handle:vertical {{
            background-color: #4a5068;
            border-radius: 5px;
            min-height: 28px;
        }}
        QScrollBar::handle:vertical:hover {{
            background-color: #5a6078;
        }}
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {{
            height: 0;
        }}
        QScrollBar:horizontal {{
            background-color: {_SURFACE};
            height: 12px;
            border-radius: 6px;
        }}
        QScrollBar::handle:horizontal {{
            background-color: #4a5068;
            border-radius: 5px;
            min-width: 28px;
        }}
        QStatusBar {{
            background-color: {_SURFACE};
            border-top: 1px solid {_BORDER};
            color: {_TEXT_MUTED};
            padding: 4px 8px;
        }}
        QSplitter::handle {{
            background-color: {_BORDER};
            width: 1px;
        }}
        QLabel#CardArtPanel {{
            background-color: #252830;
            border: 1px solid {_BORDER};
            border-radius: 8px;
            color: {_TEXT_MUTED};
        }}
        QLabel#CardThumb {{
            background-color: #252830;
            border: 1px solid {_BORDER};
            border-radius: 6px;
            color: {_TEXT_MUTED};
        }}
        QLabel#CardThumb:hover {{
            border: 1px solid {_ACCENT_DIM};
            background-color: #2d3140;
        }}
        """
    )
