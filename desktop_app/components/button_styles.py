from __future__ import annotations

from typing import Any, Callable, Optional

import flet as ft


def cancel_button(
    label: str,
    on_click: Optional[Callable],
    icon: Optional[Any] = ft.Icons.CLOSE_ROUNDED,
    *,
    text_color: str = "#1E293B",
    bgcolor: str = "#F1F5F9",
    radius: int = 8,
) -> ft.ElevatedButton:
    style_kwargs = dict(
        shape=ft.RoundedRectangleBorder(radius=radius),
        color=text_color,
        bgcolor=bgcolor,
    )
    try:
        style = ft.ButtonStyle(elevation=0, shadow_color="#00000000", **style_kwargs)
    except TypeError:
        try:
            style = ft.ButtonStyle(elevation=0, **style_kwargs)
        except TypeError:
            style = ft.ButtonStyle(**style_kwargs)
    btn = ft.ElevatedButton(label, icon=icon, on_click=on_click, style=style)
    if hasattr(btn, "elevation"):
        try:
            btn.elevation = 0
        except Exception:
            pass
    return btn
