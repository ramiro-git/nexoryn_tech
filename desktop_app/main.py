from __future__ import annotations

import os
from pathlib import Path
import sys

import flet as ft


def _get_target():
    ui = (os.getenv("NEXORYN_UI") or "basic").strip().lower()
    if ui == "advanced":
        try:
            from desktop_app.ui_advanced import main as target
        except ModuleNotFoundError:
            from ui_advanced import main as target  # type: ignore

        return target

    try:
        from desktop_app.ui_basic import main as target
    except ModuleNotFoundError:
        from ui_basic import main as target  # type: ignore

    return target


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    ft.app(target=_get_target())
