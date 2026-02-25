"""
AsyncSelect - Componente de select con búsqueda y carga lazy para Flet.

Características:
- Búsqueda con debounce
- Lazy loading con infinite scroll
- Cache simple por (query, offset)
- Manejo de errores con retry
- Selección con callback
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import flet as ft

Option = Dict[str, Any]
logger = logging.getLogger(__name__)


class AsyncSelect(ft.Column):
    _default_page: Optional[ft.Page] = None

    @classmethod
    def set_default_page(cls, page: ft.Page) -> None:
        cls._default_page = page

    def __init__(
        self,
        loader: Callable[[str, int, int], Any],
        value: Any = None,
        placeholder: str = "Seleccionar...",
        on_change: Optional[Callable[[Any], None]] = None,
        width: Optional[int] = None,
        disabled: bool = False,
        page_size: int = 50,
        debounce_ms: int = 400,
        label: Optional[str] = None,
        bgcolor: Optional[str] = "#F1F5F9",
        border_color: str = "#475569",  # Slate 600
        focused_border_color: str = "#6366F1",  # Indigo 500
        expand: bool = False,
        initial_items: Optional[List[Dict[str, Any]]] = None,
        page_ref: Optional[Any] = None,  # Explicit page reference
        show_label: bool = False,
        border_width: int = 2,
        border_radius: int = 12,
        placeholder_color: Optional[str] = None,
        text_color: str = "#1E293B",
        text_weight: Optional[ft.FontWeight] = None,
        placeholder_weight: Optional[ft.FontWeight] = None,
        label_color: str = "#1E293B",
        label_size: int = 13,
        label_weight: ft.FontWeight = ft.FontWeight.BOLD,
        horizontal_alignment: ft.CrossAxisAlignment = ft.CrossAxisAlignment.STRETCH,
        keyboard_accessible: bool = False,
    ):
        # Flet Control.__init__ may call property setters (e.g. disabled) before
        # this constructor body continues. Pre-initialize attributes used there.
        self._trigger: Optional[ft.Control] = None
        self._keyboard_trigger: Optional[ft.Control] = None
        self._keyboard_trigger_container: Optional[ft.Container] = None
        self._trigger_label: Optional[ft.Text] = None
        self._trigger_icon: Optional[ft.Icon] = None
        self._keyboard_text_only = False
        self._keyboard_focused = False
        self._disabled = bool(disabled)

        super().__init__(spacing=2, expand=expand, width=width, disabled=disabled, horizontal_alignment=horizontal_alignment)
        self.loader = loader
        if bgcolor is None:
            bgcolor = "#F1F5F9"
        self._value = value
        
        # Determine descriptive placeholder
        if placeholder == "Seleccionar..." and label:
            # Clean label (remove "Filtrar ", "Seleccionar ", asterisks, etc if they exist)
            clean_label = label.replace("Filtrar ", "").replace("Seleccionar ", "").replace("*", "").strip()
            self.placeholder = f"Seleccionar {clean_label.lower()}..." + (" *" if "*" in label else "")
        else:
            self.placeholder = placeholder or "Seleccionar..."

        self._on_change_callback = on_change
        self.debounce_ms = debounce_ms
        self.label = label
        self.bgcolor = bgcolor
        self.border_color = border_color
        self.focused_border_color = focused_border_color
        self.page_size = page_size
        self.show_label = show_label
        self.border_width = border_width
        self.border_radius = border_radius
        if placeholder_color is None:
            placeholder_color = "#1E293B" if not show_label else "#94A3B8"
        self.placeholder_color = placeholder_color
        self.text_color = text_color
        self.text_weight = text_weight
        if placeholder_weight is None:
            placeholder_weight = ft.FontWeight.BOLD if not show_label else None
        self.placeholder_weight = placeholder_weight
        self.label_color = label_color
        self.label_size = label_size
        self.label_weight = label_weight
        self.keyboard_accessible = bool(keyboard_accessible)

        # State
        self._query = ""
        self._offset = 0
        self._items: List[Dict[str, Any]] = initial_items or []
        self._has_more = True
        self._loading = False
        self._loading_more = False
        self._error: Optional[str] = None
        self._selected_label = ""
        self._selected_tooltip = ""
        self._debounce_task: Optional[asyncio.Task] = None
        self._cache: Dict[str, Tuple[List[Dict[str, Any]], bool]] = {}
        self._disabled = bool(disabled)
        self._tab_index: Optional[int] = None
        self._active_index = -1
        self._previous_keyboard_handler: Optional[Callable[[Any], None]] = None
        self._keyboard_handler_page: Optional[ft.Page] = None
        self._last_nav_ts = 0.0
        self._nav_min_interval_ms = 22
        self._pending_scroll_task: Optional[asyncio.Task] = None

        # Controls placeholders
        self._search_field: Optional[ft.TextField] = None
        self._options_list: Optional[ft.ListView] = None
        self._dialog: Optional[ft.AlertDialog] = None
        self._loading_indicator: Optional[ft.Control] = None
        self._loading_more_indicator: Optional[ft.Control] = None
        self._empty_text: Optional[ft.Text] = None
        self._error_row: Optional[ft.Row] = None
        self._dialog_card_container: Optional[ft.Container] = None
        self._page_ref = page_ref

        # Build UI
        self._update_selected_label()
        self._trigger = self._build_trigger()
        
        if self.label and self.show_label:
            self.controls.append(
                ft.Text(self.label, size=self.label_size, color=self.label_color, weight=self.label_weight)
            )
        self.controls.append(self._trigger)

    def _safe_set(self, obj: Any, name: str, value: Any) -> None:
        if obj is None:
            return
        if not hasattr(obj, name):
            return
        try:
            setattr(obj, name, value)
        except Exception:
            pass

    def _build_trigger_content(self, text_color: str, text_weight: Optional[ft.FontWeight]) -> ft.Row:
        trigger_text = self._selected_label or self.placeholder
        trigger_tooltip = self._selected_tooltip or trigger_text
        self._trigger_label = ft.Text(
            trigger_text,
            color=text_color,
            size=14,
            weight=text_weight,
            no_wrap=True,
            max_lines=1,
            overflow=ft.TextOverflow.ELLIPSIS,
            tooltip=trigger_tooltip,
            expand=True,
        )
        self._trigger_icon = ft.Icon(
            ft.icons.ARROW_DROP_DOWN,
            color="#475569",
            size=24,
        )
        return ft.Row(
            [
                self._trigger_label,
                self._trigger_icon,
            ],
            alignment=ft.MainAxisAlignment.SPACE_BETWEEN,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
        )

    def _build_keyboard_button_style(self) -> ft.ButtonStyle:
        style_kwargs: Dict[str, Any] = {
            "shape": ft.RoundedRectangleBorder(radius=self.border_radius),
            "padding": ft.padding.only(left=12, right=8),
            "bgcolor": self.bgcolor,
        }
        try:
            # Keep side neutral: visible border is managed by the outer container
            # to avoid size/style drift across Flet versions.
            style_kwargs["side"] = ft.BorderSide(width=0, color=self.bgcolor or "transparent")
        except Exception:
            pass
        try:
            return ft.ButtonStyle(**style_kwargs)
        except TypeError:
            style_kwargs.pop("side", None)
            try:
                return ft.ButtonStyle(**style_kwargs)
            except Exception:
                return ft.ButtonStyle(shape=ft.RoundedRectangleBorder(radius=self.border_radius))

    def _build_trigger(self):
        is_selected = bool(self._selected_label)
        text_color = self.text_color if is_selected else self.placeholder_color
        text_weight = self.text_weight if is_selected else self.placeholder_weight
        trigger_content = self._build_trigger_content(text_color, text_weight)

        if self.keyboard_accessible:
            try:
                trigger_button = ft.TextButton(
                    content=trigger_content,
                    on_click=self._on_trigger_click,
                    disabled=self._disabled,
                    style=self._build_keyboard_button_style(),
                )
                self._keyboard_text_only = False
            except TypeError:
                self._keyboard_text_only = True
                self._trigger_label = None
                self._trigger_icon = None
                trigger_button = ft.TextButton(
                    text=self._selected_label or self.placeholder,
                    on_click=self._on_trigger_click,
                    disabled=self._disabled,
                    style=self._build_keyboard_button_style(),
                )
            self._keyboard_trigger = trigger_button
            self._safe_set(trigger_button, "expand", True)
            self._safe_set(trigger_button, "on_focus", self._on_keyboard_focus)
            self._safe_set(trigger_button, "on_blur", self._on_keyboard_blur)
            self._safe_set(trigger_button, "tab_index", self._tab_index)
            self._safe_set(trigger_button, "tooltip", self._selected_tooltip or self._selected_label or self.placeholder)
            trigger_container = ft.Container(
                content=trigger_button,
                width=self.width,
                height=50,
                border=ft.border.all(self._keyboard_border_width(), self._keyboard_border_color()),
                border_radius=self.border_radius,
                bgcolor=self.bgcolor,
                alignment=ft.alignment.center_left,
            )
            self._keyboard_trigger_container = trigger_container
            return trigger_container

        trigger_container = ft.Container(
            on_click=self._on_trigger_click,
            disabled=self._disabled,
            content=trigger_content,
            padding=ft.padding.only(left=12, right=8),
            border=ft.border.all(self.border_width, self.border_color),
            border_radius=self.border_radius,
            bgcolor=self.bgcolor,
            width=self.width,
            height=50,
            alignment=ft.alignment.center_left,
            tooltip=self._selected_tooltip or self._selected_label or self.placeholder,
        )
        self._safe_set(trigger_container, "tab_index", self._tab_index)
        return trigger_container

    @property
    def on_change(self):
        return self._on_change_callback

    @on_change.setter
    def on_change(self, v):
        self._on_change_callback = v

    @property
    def disabled(self):
        return self._disabled if hasattr(self, '_disabled') else False

    @disabled.setter
    def disabled(self, v):
        self._disabled = bool(v)
        keyboard_trigger = getattr(self, "_keyboard_trigger", None)
        keyboard_trigger_container = getattr(self, "_keyboard_trigger_container", None)
        trigger = getattr(self, "_trigger", None)
        if keyboard_trigger is not None:
            self._safe_set(keyboard_trigger, "disabled", self._disabled)
            self._safe_update(keyboard_trigger, "disabled_keyboard_trigger")
        if keyboard_trigger_container is not None:
            self._safe_set(keyboard_trigger_container, "disabled", self._disabled)
            self._refresh_keyboard_trigger_visual()
        if trigger is not None:
            self._safe_set(trigger, "disabled", self._disabled)
            self._safe_update(trigger, "disabled_trigger")

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        self._value = v
        self._update_selected_label()
        trigger_text = self._selected_label or self.placeholder
        trigger_tooltip = self._selected_tooltip or trigger_text
        if self._trigger_label is not None:
            self._trigger_label.value = trigger_text
            self._trigger_label.color = self.text_color if self._selected_label else self.placeholder_color
            self._trigger_label.weight = self.text_weight if self._selected_label else self.placeholder_weight
            self._safe_set(self._trigger_label, "tooltip", trigger_tooltip)
        elif self._keyboard_text_only and self._keyboard_trigger is not None and hasattr(self._keyboard_trigger, "text"):
            self._safe_set(self._keyboard_trigger, "text", trigger_text)
        self._safe_set(self._keyboard_trigger, "tooltip", trigger_tooltip)
        self._safe_set(self._trigger, "tooltip", trigger_tooltip)
        self._safe_update(self, "value_update")

    def set_tab_index(self, tab_index: Optional[int]) -> None:
        self._tab_index = tab_index
        self._safe_set(self._keyboard_trigger, "tab_index", tab_index)
        self._safe_set(self._trigger, "tab_index", tab_index)
        self._safe_update(self, "tab_index_update")

    def focus(self) -> None:
        target = self._keyboard_trigger or self._trigger
        focus_fn = getattr(target, "focus", None)
        if callable(focus_fn):
            try:
                focus_fn()
                return
            except Exception:
                pass
        page = self._get_page()
        if page and hasattr(page, "set_focus") and target is not None:
            try:
                page.set_focus(target)
            except Exception:
                pass

    def focus_trigger(self) -> None:
        self.focus()

    def update(self):
        try:
            return super().update()
        except AssertionError:
            return None

    def clear_cache(self) -> None:
        self._cache.clear()
        self._items = []
        self._offset = 0
        self._has_more = True

    def _safe_update(self, control: Optional[ft.Control], context: str) -> None:
        if not control:
            return
        try:
            control.update()
        except AssertionError as exc:
            logger.debug("AsyncSelect update skipped (%s): %s", context, exc)
        except Exception:
            logger.exception("AsyncSelect update failed (%s)", context)

    def _safe_callback(self, callback: Callable[[], None], context: str) -> None:
        try:
            callback()
        except Exception:
            logger.exception("AsyncSelect callback failed (%s)", context)

    def set_busy(self, loading: bool) -> None:
        self._update_trigger_icon(loading)
        if loading:
            self.disabled = True

    def prefetch(self, query: str = "", on_done: Optional[Callable[[], None]] = None) -> None:
        page = self._get_page()
        if not page:
            if on_done:
                self._safe_callback(on_done, "prefetch_on_done_no_page")
            return

        async def _do():
            try:
                await self._load_items(query, 0, is_search=True)
            finally:
                if on_done:
                    self._safe_callback(on_done, "prefetch_on_done")

        page.run_task(_do)

    @property
    def options(self):
        return []

    @options.setter
    def options(self, v):
        items = []
        for opt in (v or []):
            if isinstance(opt, dict):
                value = opt.get("value", opt.get("key"))
                if value is None:
                    continue
                label = str(opt.get("label", opt.get("text", "")))
                selected_label = str(opt.get("selected_label", "")).strip() or (label.splitlines()[0] if label else "")
                tooltip = str(opt.get("tooltip", "")).strip() or label or selected_label
                items.append(
                    {
                        "value": value,
                        "label": label,
                        "selected_label": selected_label,
                        "tooltip": tooltip,
                    }
                )
            elif hasattr(opt, 'key') and hasattr(opt, 'text'):
                label = str(opt.text or "")
                selected_label = label.splitlines()[0] if label else ""
                items.append(
                    {
                        "value": opt.key,
                        "label": label,
                        "selected_label": selected_label,
                        "tooltip": label or selected_label,
                    }
                )
        self._items = items
        self._cache.clear()
        self._update_selected_label()
        self._safe_update(self, "options_update")

    def _update_selected_label(self):
        if self._value is None:
            self._selected_label = ""
            self._selected_tooltip = ""
            return
        
        # Try to find in current items
        for opt in self._items:
            if str(opt.get("value")) == str(self._value):
                label = str(opt.get("label", ""))
                first_line = label.splitlines()[0] if label else ""
                self._selected_label = str(opt.get("selected_label", "")).strip() or first_line or str(self._value)
                self._selected_tooltip = str(opt.get("tooltip", "")).strip() or label or self._selected_label
                return
        
        # NOTE: Initial label resolution needs to be handled by the parent 
        # or by a separate loader call if not in current view items.
        self._selected_label = ""
        self._selected_tooltip = ""

    def _keyboard_border_color(self) -> str:
        if self._disabled:
            return "#94A3B8"
        if self._keyboard_focused:
            return self.focused_border_color
        return self.border_color

    def _keyboard_border_width(self) -> int:
        return int(getattr(self, "border_width", 2) or 2)

    def _refresh_keyboard_trigger_visual(self) -> None:
        trigger_container = getattr(self, "_keyboard_trigger_container", None)
        if trigger_container is None:
            return
        self._safe_set(
            trigger_container,
            "border",
            ft.border.all(self._keyboard_border_width(), self._keyboard_border_color()),
        )
        self._safe_set(trigger_container, "bgcolor", self.bgcolor)
        if self._keyboard_focused and not self._disabled:
            try:
                self._safe_set(
                    trigger_container,
                    "shadow",
                    ft.BoxShadow(blur_radius=0, spread_radius=1, color=self.focused_border_color),
                )
            except Exception:
                self._safe_set(trigger_container, "shadow", None)
        else:
            self._safe_set(trigger_container, "shadow", None)
        self._safe_update(trigger_container, "keyboard_trigger_visual")

    def _on_keyboard_focus(self, _e: Any) -> None:
        self._keyboard_focused = True
        self._refresh_keyboard_trigger_visual()

    def _on_keyboard_blur(self, _e: Any) -> None:
        self._keyboard_focused = False
        self._refresh_keyboard_trigger_visual()

    def _get_cache_key(self, query, offset):
        return f"{query}|{offset}"

    async def _load_items(self, query, offset, is_search=False):
        if is_search:
            self._error = None
            self._update_trigger_icon(True)

        # Cache check
        key = self._get_cache_key(query, offset)
        if key in self._cache:
            items, has_more = self._cache[key]
            if is_search:
                self._items = items
                self._has_more = has_more
                self._loading = False
                self._update_dialog_ui()
            else: # load more
                self._items.extend(items)
                self._has_more = has_more
                self._loading_more = False
                self._update_dialog_ui()
            return

        if offset == 0:
            self._loading = True
            self._error = None
        else:
            self._loading_more = True

        self._update_dialog_ui()

        try:
            result = self.loader(query, offset, self.page_size)
            if asyncio.iscoroutine(result):
                items, has_more = await result
            else:
                items, has_more = result

            self._cache[key] = (items, has_more)

            if is_search:
                self._items = items
                self._offset = 0
            else:
                self._items.extend(items)
                self._offset = offset

            self._has_more = has_more
            self._error = None
        except Exception as exc:
            self._error = str(exc)
        finally:
            self._loading = False
            self._loading_more = False
            self._update_trigger_icon(False)
            self._update_dialog_ui()

    def _update_trigger_icon(self, loading):
        if self._trigger_icon is not None:
            self._trigger_icon.size = 24
            self._trigger_icon.color = "#475569"
            self._trigger_icon.icon = ft.icons.HOURGLASS_EMPTY_ROUNDED if loading else ft.icons.ARROW_DROP_DOWN
            self._safe_update(self, "trigger_icon")

    def _on_search_change(self, e):
        new_query = e.control.value

        if self._debounce_task:
            self._debounce_task.cancel()

        async def debounced_search():
            await asyncio.sleep(self.debounce_ms / 1000)
            self._query = new_query
            self._cache.clear()
            await self._load_items(new_query, 0, is_search=True)

        page = self._get_page()
        if not page:
            return
        self._debounce_task = page.run_task(debounced_search)

    def _on_scroll(self, e):
        if self._loading_more or not self._has_more or self._error:
            return

        if e.pixels >= e.max_scroll_extent - 100:
            page = self._get_page()
            if not page:
                return
            page.run_task(self._load_items, self._query, len(self._items))

    def _on_option_click(self, option):
        self._value = option.get("value")
        selected_label = str(option.get("selected_label", "")).strip()
        option_label = str(option.get("label", ""))
        self._selected_label = selected_label or (option_label.splitlines()[0] if option_label else "")
        self._selected_tooltip = str(option.get("tooltip", "")).strip() or option_label or self._selected_label
        self._close_dialog()

        if self._on_change_callback:
            callback = self._on_change_callback
            value = self._value
            page = self._get_page()
            if page:
                async def _deferred_on_change():
                    await asyncio.sleep(0)
                    callback(value)
                page.run_task(_deferred_on_change)
            else:
                callback(value)

        # Update trigger manualy before general update
        if self._trigger_label is not None:
            self._trigger_label.value = self._selected_label
            self._trigger_label.color = self.text_color
            self._trigger_label.weight = self.text_weight
            self._safe_set(self._trigger_label, "tooltip", self._selected_tooltip or self._selected_label)
        elif self._keyboard_text_only and self._keyboard_trigger is not None and hasattr(self._keyboard_trigger, "text"):
            self._safe_set(self._keyboard_trigger, "text", self._selected_label)
        self._safe_set(self._keyboard_trigger, "tooltip", self._selected_tooltip or self._selected_label)
        self._safe_set(self._trigger, "tooltip", self._selected_tooltip or self._selected_label)

        self._safe_update(self, "option_click")

    def _normalize_active_index(self) -> None:
        if not self._items:
            self._active_index = -1
            return
        if self._active_index < 0 or self._active_index >= len(self._items):
            self._active_index = 0

    def _select_active_option(self) -> None:
        if self._loading:
            return
        self._normalize_active_index()
        if self._active_index < 0 or self._active_index >= len(self._items):
            return
        self._on_option_click(self._items[self._active_index])

    def _on_search_submit(self, _e: Any) -> None:
        # If a debounced search is still pending, the current items are stale.
        # Cancel the debounce, run the search immediately, then select.
        if self._debounce_task and not self._debounce_task.done():
            pending_query = self._search_field.value if self._search_field else ""
            self._debounce_task.cancel()
            self._debounce_task = None

            page = self._get_page()
            if not page:
                return

            async def _immediate_search_then_select():
                self._query = pending_query
                self._cache.clear()
                await self._load_items(pending_query, 0, is_search=True)
                # After fresh results loaded, select the first item
                self._active_index = 0
                self._select_active_option()

            page.run_task(_immediate_search_then_select)
            return

        # No pending search — items are fresh, select normally
        self._select_active_option()

    def _is_keydown_event(self, event: Any) -> bool:
        event_type = str(getattr(event, "event_type", "") or getattr(event, "type", "")).strip().lower()
        if event_type in {"keyup", "up", "key_up"}:
            return False
        if event_type in {"keydown", "down", "key_down"}:
            return True
        # Unknown/empty event types are treated as keydown to avoid losing
        # keyboard navigation events on some Flet runtimes/platforms.
        return True

    def _forward_previous_keyboard_handler(self, event: Any) -> None:
        if callable(self._previous_keyboard_handler):
            try:
                self._previous_keyboard_handler(event)
            except Exception:
                logger.debug("AsyncSelect keyboard handler previo falló", exc_info=True)

    def _is_own_keyboard_handler(self, handler: Any) -> bool:
        if handler is None:
            return False
        if handler is self._on_page_keyboard_event:
            return True
        return (
            getattr(handler, "__self__", None) is self
            and getattr(handler, "__func__", None) is AsyncSelect._on_page_keyboard_event
        )

    def _is_selected_index(self, index: int) -> bool:
        if index < 0 or index >= len(self._items):
            return False
        return str(self._items[index].get("value")) == str(self._value)

    def _active_bgcolor_for_index(self, index: int) -> Optional[str]:
        if index == self._active_index:
            return "#DBEAFE"
        if self._is_selected_index(index):
            return "#EEF2FF"
        return None

    def _option_key(self, index: int) -> str:
        return f"async-select-opt-{index}"

    def _ensure_active_option_visible(self) -> None:
        if self._active_index < 0 or self._options_list is None:
            return
        scroll_fn = getattr(self._options_list, "scroll_to", None)
        if not callable(scroll_fn):
            return

        option_key = self._option_key(self._active_index)
        try:
            scroll_fn(key=option_key, duration=0)
            return
        except Exception:
            pass

        offset = max(0, int(self._active_index * 56))
        try:
            scroll_fn(offset=offset, duration=0)
            return
        except Exception:
            pass
        try:
            scroll_fn(offset=offset)
        except Exception:
            pass

    def _cancel_pending_scroll_task(self) -> None:
        task = self._pending_scroll_task
        if task is None:
            return
        cancel_fn = getattr(task, "cancel", None)
        if callable(cancel_fn):
            try:
                cancel_fn()
            except Exception:
                pass
        self._pending_scroll_task = None

    def _schedule_active_option_scroll(self) -> None:
        page = self._get_page()
        if page is None:
            self._ensure_active_option_visible()
            return
        self._cancel_pending_scroll_task()

        async def _do_scroll() -> None:
            try:
                await asyncio.sleep(0)
            except asyncio.CancelledError:
                return
            if not self._dialog or not self._dialog.visible:
                return
            self._ensure_active_option_visible()

        self._pending_scroll_task = page.run_task(_do_scroll)

    def _apply_active_visual_state(self, previous_index: Optional[int], ensure_visible: bool = False) -> None:
        controls = getattr(self._options_list, "controls", None)
        if not isinstance(controls, list):
            return

        if len(controls) != len(self._items):
            self._update_dialog_ui()
            if ensure_visible:
                self._schedule_active_option_scroll()
            return

        dirty_indexes: List[int] = []
        for idx in (previous_index, self._active_index):
            if isinstance(idx, int) and 0 <= idx < len(controls) and idx not in dirty_indexes:
                dirty_indexes.append(idx)

        if dirty_indexes:
            for idx in dirty_indexes:
                self._safe_set(controls[idx], "bgcolor", self._active_bgcolor_for_index(idx))
            self._safe_update(self._options_list, "active_visual_state")

        if ensure_visible:
            self._schedule_active_option_scroll()

    def _move_active_index(self, step: int) -> bool:
        if not self._items:
            return False
        self._normalize_active_index()
        previous_index = self._active_index
        if self._active_index < 0:
            self._active_index = 0
            self._apply_active_visual_state(previous_index, ensure_visible=True)
            return True

        max_index = len(self._items) - 1
        if step > 0 and self._active_index >= max_index:
            if self._has_more and not self._loading_more and not self._loading:
                page = self._get_page()
                if page:
                    page.run_task(self._load_more_from_keyboard, self._active_index)
                    return True
            if previous_index != max_index:
                self._active_index = max_index
                self._apply_active_visual_state(previous_index, ensure_visible=True)
            return True

        if step < 0 and self._active_index <= 0:
            if previous_index != 0:
                self._active_index = 0
                self._apply_active_visual_state(previous_index, ensure_visible=True)
            return True

        self._active_index = max(0, min(self._active_index + step, max_index))
        if self._active_index == previous_index:
            return True
        self._apply_active_visual_state(previous_index, ensure_visible=True)
        return True

    async def _load_more_from_keyboard(self, current_index: int) -> None:
        if self._loading_more or self._loading or not self._has_more:
            return
        old_len = len(self._items)
        await self._load_items(self._query, old_len, False)
        new_len = len(self._items)
        previous_index = current_index
        if new_len <= 0:
            self._active_index = -1
        elif new_len > old_len:
            self._active_index = min(max(current_index + 1, 0), new_len - 1)
        else:
            self._active_index = min(max(current_index, 0), new_len - 1)
        if self._active_index != previous_index:
            self._apply_active_visual_state(previous_index, ensure_visible=True)
        else:
            self._schedule_active_option_scroll()

    def _on_page_keyboard_event(self, event: Any) -> None:
        if not self._dialog or not self._dialog.visible:
            self._forward_previous_keyboard_handler(event)
            return
        if not self._is_keydown_event(event):
            self._forward_previous_keyboard_handler(event)
            return

        key = str(getattr(event, "key", "") or "").strip().lower().replace("_", " ").replace("-", " ")
        consumed = False
        arrow_keys = {"arrow down", "arrowdown", "down", "arrow up", "arrowup", "up"}
        if key in arrow_keys:
            now = time.monotonic()
            min_interval_s = max(0, int(getattr(self, "_nav_min_interval_ms", 22) or 0)) / 1000.0
            if (now - self._last_nav_ts) < min_interval_s:
                consumed = True
            else:
                self._last_nav_ts = now
                if key in {"arrow down", "arrowdown", "down"}:
                    consumed = self._move_active_index(1)
                else:
                    consumed = self._move_active_index(-1)

        if not consumed:
            self._forward_previous_keyboard_handler(event)

    def _install_keyboard_handler(self, page: Optional[ft.Page]) -> None:
        if page is None:
            return
        current_handler = getattr(page, "on_keyboard_event", None)
        if not self._is_own_keyboard_handler(current_handler):
            self._previous_keyboard_handler = current_handler
        self._keyboard_handler_page = page
        try:
            self._safe_set(page, "on_keyboard_event", self._on_page_keyboard_event)
        except Exception:
            logger.debug("AsyncSelect no pudo instalar keyboard handler", exc_info=True)

    def _restore_keyboard_handler(self) -> None:
        page = self._keyboard_handler_page
        if page is None:
            self._previous_keyboard_handler = None
            return
        try:
            if self._is_own_keyboard_handler(getattr(page, "on_keyboard_event", None)):
                self._safe_set(page, "on_keyboard_event", self._previous_keyboard_handler)
        except Exception:
            logger.debug("AsyncSelect no pudo restaurar keyboard handler", exc_info=True)
        finally:
            self._keyboard_handler_page = None
            self._previous_keyboard_handler = None

    def _focus_search_field(self, page: Optional[ft.Page], retries: int = 0, delay: float = 0.04) -> None:
        if self._search_field is None:
            return
        focus_fn = getattr(self._search_field, "focus", None)
        try:
            if callable(focus_fn):
                focus_fn()
            elif page and hasattr(page, "set_focus"):
                page.set_focus(self._search_field)
        except Exception:
            pass

        if page is None or retries <= 0:
            return

        async def _refocus() -> None:
            for _ in range(retries):
                await asyncio.sleep(delay)
                if not self._dialog or not self._dialog.visible:
                    return
                try:
                    focus_fn_local = getattr(self._search_field, "focus", None)
                    if callable(focus_fn_local):
                        focus_fn_local()
                    elif hasattr(page, "set_focus"):
                        page.set_focus(self._search_field)
                except Exception:
                    continue

        page.run_task(_refocus)

    def _on_retry(self, _e):
        self._cache.clear()
        page = self._get_page()
        if not page:
            return
        page.run_task(self._load_items, self._query, 0, True)

    def _on_trigger_click(self, _e):
        if self.disabled: return
        self._open_dialog()

    def _open_dialog(self):
        # build once or update existing
        if not self._dialog:
            self._build_dialog()
        
        # Initialize loading state
        self._query = ""
        self._offset = 0
        self._items = []
        self._has_more = True
        self._error = None
        self._loading = False
        self._loading_more = False
        self._active_index = -1
        self._last_nav_ts = 0.0
        self._cancel_pending_scroll_task()

        if self._search_field is not None:
            self._search_field.value = ""

        cache_key = self._get_cache_key("", 0)
        if cache_key in self._cache:
            cached_items, cached_has_more = self._cache[cache_key]
            self._items = list(cached_items)
            self._has_more = cached_has_more
        else:
            self._loading = True
            self._update_trigger_icon(True)
        
        # Get page reference - try self.page first, then traverse parent hierarchy
        page = self._get_page()
        if self._dialog_card_container is not None:
            self._dialog_card_container.width = self._resolve_dialog_width()
        
        if page:
            self._install_keyboard_handler(page)
            # IMPORTANT: Clear Flet's internal dialog to avoid conflicts
            # Setting it to None and ensuring open=False prevents Flet from closing other things
            if hasattr(page, 'dialog') and page.dialog:
                try:
                    page.dialog.open = False
                    page.dialog = None
                except Exception as exc:
                    logger.debug("AsyncSelect no pudo limpiar dialog previo: %s", exc)

            # Ensure it's in overlay and ALWAYS at the end (to be on top of other modals)
            if self._dialog in page.overlay:
                page.overlay.remove(self._dialog)
            page.overlay.append(self._dialog)
            
            # Show the manual modal
            self._dialog.visible = True
            self._update_dialog_ui()
            self._focus_search_field(page, retries=4, delay=0.05)
            
            # Start loading (only if cache is empty)
            if self._loading:
                page.run_task(self._load_items, "", 0, True)

    def _get_page(self):
        """Get page reference, traversing parent hierarchy if needed."""
        # First check stored reference from did_mount
        if hasattr(self, '_page_ref') and self._page_ref:
            return self._page_ref
        if self.page:
            return self.page
        if AsyncSelect._default_page:
            self._page_ref = AsyncSelect._default_page
            return AsyncSelect._default_page
        ctrl = self
        depth = 0
        while ctrl and depth < 50:
            if hasattr(ctrl, 'page') and ctrl.page:
                print(f"[AsyncSelect] Found page at depth {depth}")
                self._page_ref = ctrl.page
                return ctrl.page
            ctrl = getattr(ctrl, 'parent', None)
            depth += 1
        print(f"[AsyncSelect] Could not find page after {depth} levels")
        return None

    def _resolve_dialog_width(self) -> int:
        base_width = 450
        min_width = 420
        max_width = 760

        preferred = base_width
        control_width = getattr(self, "width", None)
        if isinstance(control_width, (int, float)):
            preferred = max(preferred, int(control_width) + 140)

        page = self._get_page()
        viewport_cap: Optional[int] = None
        if page is not None:
            page_width = getattr(page, "width", None)
            if isinstance(page_width, (int, float)):
                viewport_cap = max(320, int(page_width) - 24)

        max_allowed = min(max_width, viewport_cap) if viewport_cap is not None else max_width
        min_allowed = min(min_width, max_allowed)
        return max(min_allowed, min(preferred, max_allowed))

    def _close_dialog(self, _e=None):
        if self._dialog:
            self._dialog.visible = False
            self._cancel_pending_scroll_task()
            self._restore_keyboard_handler()
            try:
                page = self._get_page()
                if page:
                    self._safe_update(page, "close_dialog")
            except Exception as exc:
                logger.debug("AsyncSelect no pudo actualizar página al cerrar dialog: %s", exc)

    def did_mount(self):
        # Store page reference for later use
        if self.page:
            self._page_ref = self.page
        
        # When component mounts, pre-build to have it ready
        if not self._dialog:
            self._build_dialog()
        
        self._update_selected_label()
        self._safe_update(self, "did_mount")

    def _build_dialog(self):
        self._search_field = ft.TextField(
            hint_text="Buscar...",
            on_change=self._on_search_change,
            on_submit=self._on_search_submit,
            border_color="#E2E8F0",
            focused_border_color=self.focused_border_color,
            border_radius=8,
            height=44,
            text_size=14,
            prefix_icon=ft.icons.SEARCH_ROUNDED,
            autofocus=True,
        )

        self._loading_indicator = ft.Container(
            content=ft.Row([
                ft.ProgressRing(width=16, height=16, stroke_width=2, color=self.focused_border_color),
                ft.Text("Buscando...", size=13, color="#64748B")
            ], alignment=ft.MainAxisAlignment.CENTER),
            padding=20,
            visible=False
        )

        self._loading_more_indicator = ft.Container(
            content=ft.Row([
                ft.ProgressRing(width=14, height=14, stroke_width=2, color=self.focused_border_color),
                ft.Text("Más...", size=12, color="#64748B")
            ], alignment=ft.MainAxisAlignment.CENTER),
            padding=10,
            visible=False
        )

        self._error_row = ft.Row(
            [
                ft.Icon(ft.icons.ERROR_OUTLINE, color="#EF4444", size=16),
                ft.Text("Error al cargar", size=13, color="#EF4444", expand=True),
                ft.TextButton("Reintentar", on_click=self._on_retry),
            ],
            visible=False,
        )

        self._empty_text = ft.Container(
            content=ft.Text("Sin resultados", size=14, color="#94A3B8"),
            alignment=ft.Alignment(0, 0),
            padding=40,
            visible=False
        )

        self._options_list = ft.ListView(
            expand=True,
            spacing=2,
            padding=ft.padding.all(4),
            on_scroll=self._on_scroll,
        )

        # Usamos un Container manual en el overlay que se comporta como un modal
        # pero SIN ser un ft.AlertDialog, para que no cierre otros diálogos.
        self._dialog_card_container = ft.Container(
            width=self._resolve_dialog_width(),
            height=550,
            bgcolor="#FFFFFF",
            padding=ft.padding.all(16),
            content=ft.Column([
                ft.Row([
                    ft.Text((f"Seleccionar {self.label.replace('Filtrar ', '').replace('*', '').strip().lower()}..." + (" *" if "*" in self.label else "")) if self.label else "Seleccionar...", size=18, weight=ft.FontWeight.BOLD, color="#1E293B"),
                    ft.IconButton(ft.icons.CLOSE_ROUNDED, icon_size=24, on_click=self._close_dialog)
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN),
                self._search_field,
                ft.Divider(height=1, color="#F1F5F9"),
                self._loading_indicator,
                self._error_row,
                ft.Container(
                    content=self._options_list,
                    expand=True,
                ),
                self._empty_text,
                self._loading_more_indicator,
            ], spacing=10)
        )
        self._dialog = ft.Container(
            content=ft.Card(
                elevation=30, # Higher elevation for search
                shape=ft.RoundedRectangleBorder(radius=16),
                content=self._dialog_card_container
            ),
            bgcolor="#40000000", # Slightly lighter dimming for search
            alignment=ft.Alignment(0, 0),
            visible=False,
            # Force full screen in overlay
            left=0, top=0, right=0, bottom=0,
            on_click=lambda _: None 
        )

    def _update_dialog_ui(self):
        if not self._dialog or not self._dialog.visible: return

        self._loading_indicator.visible = self._loading and not self._items
        self._loading_more_indicator.visible = self._loading_more
        self._error_row.visible = self._error is not None
        if self._error:
            self._error_row.controls[1].value = f"Error: {self._error[:30]}..."
            
        self._empty_text.visible = not self._loading and not self._error and not self._items
        self._normalize_active_index()

        self._options_list.controls = [
            self._build_option_item(opt, index) for index, opt in enumerate(self._items)
        ]
        
        page = self._get_page()
        if page:
            self._safe_update(page, "dialog_update")

    def _build_option_item(self, option, index: int):
        is_selected = str(option.get("value")) == str(self._value)
        option_label = str(option.get("label", ""))
        option_tooltip = str(option.get("tooltip", "")).strip() or option_label
        base_bgcolor = self._active_bgcolor_for_index(index)

        def on_item_hover(e):
            hovered_control = getattr(e, "control", None)
            if hovered_control is None:
                return
            hovered_data = getattr(hovered_control, "data", None) or {}
            hovered_index_raw = hovered_data.get("index", index) if isinstance(hovered_data, dict) else index
            try:
                hovered_index = int(hovered_index_raw)
            except Exception:
                hovered_index = index
            if hovered_index == self._active_index:
                hovered_control.bgcolor = "#DBEAFE"
            elif e.data == "true":
                hovered_control.bgcolor = "#F1F5F9"
            else:
                hovered_control.bgcolor = "#EEF2FF" if self._is_selected_index(hovered_index) else None
            # Hover events can arrive after the option row was unmounted.
            # Use safe update to avoid "Control must be added to the page first".
            self._safe_update(hovered_control, "option_hover")

        return ft.Container(
            content=ft.Row([
                ft.Text(
                    option_label,
                    size=14,
                    color="#1E293B",
                    expand=True,
                    max_lines=2,
                    overflow=ft.TextOverflow.ELLIPSIS,
                    tooltip=option_tooltip,
                ),
                ft.Icon(ft.icons.CHECK_ROUNDED, size=16, color=self.focused_border_color, visible=is_selected)
            ]),
            padding=ft.padding.symmetric(horizontal=12, vertical=10),
            border_radius=8,
            on_click=lambda _: self._on_option_click(option),
            on_hover=on_item_hover,
            bgcolor=base_bgcolor,
            key=self._option_key(index),
            data={"index": index},
        )
