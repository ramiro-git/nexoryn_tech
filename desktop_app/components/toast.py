
import threading
import time
from typing import Optional

import flet as ft


class ToastNotification(ft.Container):
    def __init__(
        self,
        message: str,
        kind: str = "info",  # info, success, warning, error
        duration: int = 4000,
        on_dismiss: Optional[callable] = None,
    ):
        super().__init__()
        self.message = message
        self.kind = kind
        self.duration = duration
        self.on_dismiss = on_dismiss
        
        # Style configuration based on kind
        self.colors = {
            "info": {"bg": ft.Colors.BLUE_50, "border": ft.Colors.BLUE_200, "icon": ft.Colors.BLUE_500, "text": ft.Colors.BLUE_900, "icon_name": ft.icons.INFO_OUTLINE},
            "success": {"bg": ft.Colors.GREEN_50, "border": ft.Colors.GREEN_200, "icon": ft.Colors.GREEN_500, "text": ft.Colors.GREEN_900, "icon_name": ft.icons.CHECK_CIRCLE_OUTLINE},
            "warning": {"bg": ft.Colors.AMBER_50, "border": ft.Colors.AMBER_200, "icon": ft.Colors.AMBER_500, "text": ft.Colors.AMBER_900, "icon_name": ft.icons.WARNING_AMBER_ROUNDED},
            "error": {"bg": ft.Colors.RED_50, "border": ft.Colors.RED_200, "icon": ft.Colors.RED_500, "text": ft.Colors.RED_900, "icon_name": ft.icons.ERROR_OUTLINE},
        }
        
        style = self.colors.get(kind, self.colors["info"])
        
        self.content = ft.Row(
            controls=[
                ft.Icon(name=style["icon_name"], color=style["icon"], size=24),
                ft.Text(message, color=style["text"], size=14, weight=ft.FontWeight.W_500, expand=True),
                ft.IconButton(
                    icon=ft.icons.CLOSE,
                    icon_size=18,
                    icon_color=style["text"],
                    tooltip="Cerrar",
                    on_click=lambda e: self.dismiss(),
                    style=ft.ButtonStyle(shape=ft.CircleBorder(), padding=0),
                )
            ],
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            spacing=12,
        )
        
        self.bgcolor = style["bg"]
        self.border = ft.border.all(1, style["border"])
        self.border_radius = 8
        self.padding = ft.padding.symmetric(horizontal=12, vertical=8)
        self.margin = ft.margin.only(bottom=10, right=10)
        self.shadow = ft.BoxShadow(
            spread_radius=1,
            blur_radius=10,
            color=ft.Colors.with_opacity(0.1, ft.Colors.BLACK),
            offset=ft.Offset(0, 4),
        )
        self.width = 350
        # Start hidden/transparent for animation
        self.opacity = 0
        self.offset = ft.Offset(0.5, 0)
        self.animate_opacity = 300
        self.animate_offset = ft.Animation(300, ft.AnimationCurve.EASE_OUT_CUBIC)
        # In Flet 0.25.2, z_index must be set after initialization if supported or 
        # managed via overlay ordering
        try:
            self.z_index = 99999
        except:
            pass

    def did_mount(self):
        # Trigger entry animation
        self.opacity = 1
        self.offset = ft.Offset(0, 0)
        self.update()
        
        # Schedule auto-dismiss
        if self.duration > 0:
            self.timer = threading.Timer(self.duration / 1000, self.dismiss)
            self.timer.start()

    def dismiss(self):
        if hasattr(self, "timer") and self.timer:
            self.timer.cancel()
            
        self.opacity = 0
        self.offset = ft.Offset(0.5, 0)
        try:
            self.update()
            # Wait for animation to finish then remove
            threading.Timer(0.3, self._remove_from_view).start()
        except:
            pass

    def _remove_from_view(self):
        if self.on_dismiss:
            self.on_dismiss(self)


class ToastManager:
    def __init__(self, page: ft.Page):
        self.page = page
        self.container = ft.Column(
            controls=[],
            bottom=20,
            right=20,
            spacing=10,
            alignment=ft.MainAxisAlignment.END,
            horizontal_alignment=ft.CrossAxisAlignment.END,
        )
        try:
            self.container.z_index = 99999
        except:
            pass
        self.page.overlay.append(self.container)
        self.page.update()

    def show(self, message: str, kind: str = "info", duration: int = 4000):
        # Ensure container is still in overlay and at the front
        if self.container not in self.page.overlay:
            self.page.overlay.append(self.container)
        else:
            # Move to the end of overlay to be on top of other overlay elements 
            # (z_index already handles this but this is extra insurance)
            self.page.overlay.remove(self.container)
            self.page.overlay.append(self.container)

        def on_dismiss(toast):
            try:
                if toast in self.container.controls:
                    self.container.controls.remove(toast)
                    try:
                        self.page.update()
                    except (AssertionError, Exception):
                        pass
            except:
                pass

        toast = ToastNotification(message, kind, duration, on_dismiss)
        self.container.controls.append(toast)
        try:
            self.page.update()
        except (AssertionError, Exception):
            pass
        
        # Manually trigger animations if did_mount doesn't likely fire immediately
        # or rely on Flet's behavior. For overlay, explicit update is often needed.
        # But controls in overlay update independently sometimes.
        # Let's ensure update is called on the container to show the new child.
