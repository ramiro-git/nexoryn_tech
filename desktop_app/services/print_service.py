"""
Print service for generating professional PDF documents (invoices, quotes, remitos).
Redesigned with improved layout, company data from config, and proper pagination.
"""
import base64
import json
import logging
import os
import platform
import subprocess
import tempfile
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from fpdf import FPDF

from desktop_app.enums import DocumentoEstado, RemotoEstado
from desktop_app.services.number_locale import format_currency, format_percent

logger = logging.getLogger(__name__)

# Color constants for consistent styling
COLOR_PRIMARY = (15, 23, 42)      # Dark blue for headers
COLOR_WHITE = (255, 255, 255)
COLOR_LIGHT_GRAY = (241, 245, 249)  # Light row background
COLOR_BORDER = (226, 232, 240)
COLOR_TEXT = (15, 23, 42)
COLOR_TEXT_MUTED = (100, 116, 139)
AFIP_IVA_RATES = (27.0, 21.0, 10.5, 5.0, 2.5, 0.0)


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            cleaned = str(value).strip()
            if not cleaned:
                return default
            if cleaned.lstrip("-").isdigit():
                return int(cleaned)
            digits = "".join(ch for ch in cleaned if ch.isdigit())
            if not digits:
                return default
            return int(digits)
        return int(value)
    except (TypeError, ValueError):
        return default


def _format_numeric_code(value: Any, width: int) -> str:
    parsed = _safe_int(value, None)
    if parsed is None:
        text = str(value or "").strip()
        return text or "-"
    if width <= 0:
        return str(parsed)
    return f"{parsed:0{width}d}"


def _format_money(value: Any) -> str:
    number = _safe_float(value, None)
    if number is None:
        return "-"
    return format_currency(number)


def _format_date(value: Any) -> str:
    """Format date value to DD/MM/YYYY."""
    if not value:
        return "-"
    # If it's already a string, try to parse it
    if isinstance(value, str):
        # Handle ISO format with timezone
        value = value.split("T")[0].split(" ")[0]  # Get just the date part
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return dt.strftime("%d/%m/%Y")
        except ValueError:
            return value
    # If it's a datetime object
    if hasattr(value, "strftime"):
        return value.strftime("%d/%m/%Y")
    return str(value)


def _distribute_width(total: float, ratios: List[float], min_width: float = 10.0) -> List[float]:
    if not ratios:
        return []
    ratio_sum = sum(ratios)
    if ratio_sum <= 0:
        ratio_sum = len(ratios)
        ratios = [1.0] * len(ratios)

    widths: List[float] = []
    for ratio in ratios:
        width = (ratio / ratio_sum) * total
        widths.append(max(min_width, width))

    current_total = sum(widths)
    diff = total - current_total
    if widths and abs(diff) > 1e-3:
        widths[-1] = max(min_width, widths[-1] + diff)
        current_total = sum(widths)
        if current_total > total and len(widths) > 1:
            overflow = current_total - total
            widths[-1] = max(min_width, widths[-1] - overflow)
    return widths


def _wrap_text_to_width(pdf: FPDF, text: str, max_width: float) -> str:
    if not text:
        return ""
    if max_width <= 0:
        return str(text)

    lines: List[str] = []
    for paragraph in str(text or "").splitlines() or [""]:
        line = ""
        for ch in paragraph:
            if ch == "\r":
                continue
            candidate = f"{line}{ch}"
            if line and pdf.get_string_width(candidate) > max_width:
                lines.append(line)
                line = ch
            else:
                line = candidate
        lines.append(line)
    return "\n".join(lines)


def _truncate_text_to_width(pdf: FPDF, text: str, max_width: float, suffix: str = "...") -> str:
    if not text:
        return ""
    if max_width <= 0:
        return str(text)
    text = str(text or "")
    if pdf.get_string_width(text) <= max_width:
        return text
    ellipsis_width = pdf.get_string_width(suffix)
    if ellipsis_width >= max_width:
        return ""
    low = 0
    high = len(text)
    while low < high:
        mid = (low + high) // 2
        candidate = text[:mid]
        if pdf.get_string_width(candidate) + ellipsis_width <= max_width:
            low = mid + 1
        else:
            high = mid
    cut = max(low - 1, 0)
    return text[:cut].rstrip() + suffix


def _safe_multicell(pdf: FPDF, width: float, height: float, text: str, **kwargs: Any) -> None:
    c_margin = getattr(pdf, "c_margin", 0.5)
    min_char_width = max(pdf.get_string_width("W"), pdf.get_string_width("M"), pdf.get_string_width("0"), 1.0)
    min_cell_width = (c_margin * 2) + min_char_width + 0.1

    if width <= 0:
        remaining = pdf.w - pdf.r_margin - pdf.get_x()
        if remaining <= min_cell_width:
            pdf.set_x(pdf.l_margin)
            remaining = pdf.w - pdf.l_margin - pdf.r_margin
        width = remaining

    if width < min_cell_width:
        width = min_cell_width

    content_width = max(width - (c_margin * 2), 0.1)
    safe_text = _wrap_text_to_width(pdf, text, content_width)
    pdf.multi_cell(width, height, safe_text, **kwargs)


def _create_qr_png(data: str) -> Optional[str]:
    try:
        import qrcode
    except ImportError:
        return None

    try:
        qr = qrcode.QRCode(box_size=4, border=1)
        qr.add_data(data)
        qr.make(fit=True)
        image = qr.make_image(fill_color="black", back_color="white")
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
        image.save(temp_file.name, format="PNG")
        temp_file.close()
        return temp_file.name
    except Exception as exc:
        logger.debug("No se pudo generar QR: %s", exc)
        return None


def _extract_afip_qr_payload(qr_data: Any) -> Dict[str, Any]:
    if not qr_data:
        return {}
    try:
        text = str(qr_data).strip()
        if not text:
            return {}
        parsed = urlparse(text)
        encoded_param = parse_qs(parsed.query).get("p", [None])[0]
        if not encoded_param:
            return {}
        encoded = unquote(str(encoded_param))
        padding = "=" * ((4 - (len(encoded) % 4)) % 4)
        payload_raw = base64.b64decode(encoded + padding)
        payload_obj = json.loads(payload_raw.decode("utf-8"))
        if isinstance(payload_obj, dict):
            return payload_obj
    except Exception:
        logger.debug("No se pudo parsear payload QR AFIP.", exc_info=True)
    return {}


def _open_pdf(path: str) -> None:
    try:
        os.startfile(path)
    except AttributeError:
        runner = "open" if platform.system() == "Darwin" else "xdg-open"
        try:
            subprocess.run([runner, path], check=False)
        except Exception:
            logger.debug("No se pudo abrir el PDF automáticamente.", exc_info=True)
    except Exception:
        logger.debug("Abrir PDF falló.", exc_info=True)


class BaseDocumentPDF(FPDF):
    """Base class for all document PDFs with common header/footer and company data."""
    
    def __init__(
        self,
        doc_data: Dict[str, Any],
        entity_data: Dict[str, Any],
        items_data: List[Dict[str, Any]],
        company_config: Optional[Dict[str, Any]] = None,
        show_prices: bool = True,
    ):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.doc = doc_data or {}
        self.entity = entity_data or {}
        self.items = items_data or []
        self.company = company_config or {}
        self.show_prices = bool(show_prices)
        self.set_auto_page_break(True, margin=25)
        self.set_margins(12, 10, 12)
        self._temp_images: List[str] = []
        self._table_header_active: bool = False
        self._table_header_drawer: Optional[Callable[[], None]] = None
        self._table_header_last_page: int = 0
        
        # Document type info for header
        self._doc_type_label = ""
        self._doc_letter = ""

    def _get_company_name(self) -> str:
        return self.company.get("nombre_sistema") or "NEXORYN TECH"
    
    def _get_company_slogan(self) -> str:
        return self.company.get("slogan") or "Soluciones tecnológicas y logísticas"
    
    def _get_company_cuit(self) -> str:
        return self.company.get("cuit_empresa") or "-"
    
    def _get_company_address(self) -> str:
        return self.company.get("domicilio_empresa") or "-"
    
    def _get_company_razon_social(self) -> str:
        return self.company.get("razon_social") or self._get_company_name()

    def header(self) -> None:
        """Draw document header with company info and document type."""
        # Dark blue header bar
        self.set_fill_color(*COLOR_PRIMARY)
        self.set_draw_color(*COLOR_PRIMARY)
        self.rect(0, 0, self.w, 28, "F")
        
        # Company name (left side)
        self.set_font("helvetica", "B", 20)
        self.set_text_color(*COLOR_WHITE)
        self.set_xy(12, 7)
        self.cell(100, 10, self._get_company_name(), border=0)
        
        # Slogan
        self.set_font("helvetica", "", 9)
        self.set_xy(12, 17)
        self.cell(100, 5, self._get_company_slogan(), border=0)
        
        # Document type (right side)
        if self._doc_type_label:
            # Calculate positions - letter box first if present
            if self._doc_letter:
                letter_box_size = 18
                letter_x = self.w - letter_box_size - 12
                letter_y = 5
                # White box for letter
                self.set_fill_color(*COLOR_WHITE)
                self.set_draw_color(*COLOR_WHITE)
                self.rect(letter_x, letter_y, letter_box_size, letter_box_size, "FD")
                # Letter text
                self.set_text_color(*COLOR_PRIMARY)
                self.set_font("helvetica", "B", 14)
                self.set_xy(letter_x, letter_y + 4)
                self.cell(letter_box_size, 10, self._doc_letter, border=0, align="C")
                
                # Document type text to the left of the box
                self.set_text_color(*COLOR_WHITE)
                self.set_font("helvetica", "B", 14)
                doc_type_width = self.get_string_width(self._doc_type_label) + 5
                self.set_xy(letter_x - doc_type_width - 5, 9)
                self.cell(doc_type_width, 8, self._doc_type_label, border=0, align="R")
            else:
                # No letter, just document type
                self.set_text_color(*COLOR_WHITE)
                self.set_font("helvetica", "B", 16)
                doc_type_width = self.get_string_width(self._doc_type_label) + 10
                self.set_xy(self.w - doc_type_width - 12, 9)
                self.cell(doc_type_width, 8, self._doc_type_label, border=0, align="R")
        
        # Reset text color
        self.set_text_color(*COLOR_TEXT)
        
        # Position for content
        self.set_y(32)
        
        # Re-draw table header on new pages if active
        if self._table_header_active and self._table_header_drawer:
            current_page = self.page_no()
            if current_page > self._table_header_last_page:
                self._table_header_drawer()
                self._table_header_last_page = current_page

    def footer(self) -> None:
        """Draw page number in footer."""
        self.set_y(-15)
        self.set_font("helvetica", "", 8)
        self.set_text_color(*COLOR_TEXT_MUTED)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", 0, 0, "C")

    def _register_temp_image(self, path: Optional[str]) -> None:
        if path:
            self._temp_images.append(path)

    def build(self) -> None:
        raise NotImplementedError

    def generate(self) -> str:
        self.alias_nb_pages()
        self.add_page()
        self.build()
        return self._finalize()

    def _finalize(self) -> str:
        fd, path = tempfile.mkstemp(suffix=".pdf")
        os.close(fd)
        self.output(path)
        for temp_file in self._temp_images:
            try:
                os.remove(temp_file)
            except Exception:
                pass
        return path


class InvoicePDF(BaseDocumentPDF):
    """PDF generator for invoices and similar documents (quotes, etc.)."""
    
    def __init__(
        self,
        doc_data: Dict[str, Any],
        entity_data: Dict[str, Any],
        items_data: List[Dict[str, Any]],
        company_config: Optional[Dict[str, Any]] = None,
        show_prices: bool = True,
    ):
        super().__init__(doc_data, entity_data, items_data, company_config, show_prices=show_prices)
        self.is_presupuesto = self._is_presupuesto_document()
        self.is_invoice = self._is_invoice_document()
        self.subtotal_bruto = self._resolve_subtotal_bruto()
        self.descuento_lineas = self._resolve_line_discount_total()
        self.descuento_global = self._resolve_global_discount_total()
        self.tax_summary = self._build_tax_summary()
        self.neto = self._resolve_neto()
        self.iva_total = self._resolve_iva_total()
        self.total = self._resolve_total()
        
        # Set document type for header
        self._setup_doc_type()

    def _setup_doc_type(self) -> None:
        """Configure document type label and letter for header."""
        doc_type = str(self.doc.get("tipo_documento") or "COMPROBANTE").strip().upper()
        letter = str(self.doc.get("letra") or "").strip().upper()
        
        # Clean up doc_type - remove letter if already included
        if letter and doc_type.endswith(f" {letter}"):
            doc_type = doc_type[:-len(f" {letter}")]
        
        self._doc_type_label = doc_type
        self._doc_letter = letter

    def header(self) -> None:
        if self.is_presupuesto:
            if self.page_no() <= 1:
                self._draw_presupuesto_page_header()
            else:
                self._draw_presupuesto_continuation_header()
            return
        super().header()

    def build(self) -> None:
        if self.is_presupuesto:
            self._build_presupuesto()
            return

        self._draw_document_info()
        
        if self.is_invoice:
            self._draw_afip_block()
        
        self._draw_client_block()
        self.ln(4)
        self._draw_items_table()
        self.ln(4)
        self._draw_totals_block()
        self._draw_footer_remarks()

    def _build_presupuesto(self) -> None:
        self._draw_presupuesto_items_table()
        self._draw_presupuesto_totals_note_block()

    def _resolve_presupuesto_client_name(self) -> str:
        razon_social = str(self.entity.get("razon_social") or "").strip()
        if razon_social:
            return razon_social

        nombre = str(self.entity.get("nombre") or "").strip()
        apellido = str(self.entity.get("apellido") or "").strip()
        nombre_apellido = f"{nombre} {apellido}".strip()
        if nombre_apellido:
            return nombre_apellido

        nombre_completo = str(self.entity.get("nombre_completo") or "").strip()
        return nombre_completo or "Consumidor Final"

    def _presupuesto_table_headers(self) -> List[str]:
        return ["Código", "Cant.", "Artículos", "Costo/Uni", "Importe"]

    def _presupuesto_table_widths(self) -> List[float]:
        table_width = max(self.w - self.l_margin - self.r_margin, 20)
        return _distribute_width(table_width, [0.14, 0.10, 0.42, 0.17, 0.17])

    def _draw_presupuesto_table_header(self) -> None:
        headers = self._presupuesto_table_headers()
        widths = self._presupuesto_table_widths()

        self.set_x(self.l_margin)
        self.set_font("helvetica", "B", 9)
        self.set_fill_color(*COLOR_LIGHT_GRAY)
        self.set_text_color(*COLOR_TEXT)
        self.set_draw_color(*COLOR_BORDER)
        for width, title in zip(widths, headers):
            self.cell(width, 7, title, border=1, align="C", fill=True)
        self.ln()
        self.set_font("helvetica", "", 9)

    def _draw_presupuesto_page_header(self) -> None:
        number = self.doc.get("numero_serie") or "-"
        date = self.doc.get("fecha") or datetime.now().strftime("%Y-%m-%d")

        client_name = self._resolve_presupuesto_client_name()
        domicilio = self.entity.get("domicilio") or self.doc.get("direccion_entrega") or "-"
        provincia = self.entity.get("provincia") or "-"
        telefono = self.entity.get("telefono") or "-"
        condicion_iva = self.entity.get("condicion_iva") or "-"
        cuit = self.entity.get("cuit") or "-"

        content_width = self.w - self.l_margin - self.r_margin
        left_x = self.l_margin

        self.set_draw_color(*COLOR_TEXT)
        self.set_text_color(*COLOR_TEXT)
        self.set_y(10)
        self.set_x(left_x)

        self.set_font("helvetica", "B", 12)
        self.cell(content_width * 0.62, 6, f"PRESUPUESTO N°: {number}", border=0, align="L")
        self.set_font("helvetica", "B", 10)
        self.cell(content_width * 0.38, 6, f"FECHA: {_format_date(date)}", border=0, align="R")
        self.ln(5)

        self.set_x(left_x)
        self.set_font("helvetica", "B", 10)
        self.cell(0, 5, "X", border=0, ln=1, align="C")

        self.set_x(left_x)
        self.set_font("helvetica", "B", 9)
        self.cell(0, 5, "DOCUMENTO NO VÁLIDO COMO FACTURA", border=0, ln=1, align="C")
        self.ln(1)

        left_width = content_width * 0.62
        right_width = content_width - left_width
        self.set_font("helvetica", "", 9)
        self.set_x(left_x)
        client_text = _truncate_text_to_width(
            self,
            f"Cliente: {client_name}",
            left_width - (getattr(self, "c_margin", 0.5) * 2),
        )
        dom_text = _truncate_text_to_width(
            self,
            f"Dom: {domicilio}",
            right_width - (getattr(self, "c_margin", 0.5) * 2),
        )
        self.cell(left_width, 5, client_text, border=0, align="L")
        self.cell(right_width, 5, dom_text, border=0, align="R")
        self.ln()

        details_widths = _distribute_width(content_width, [0.27, 0.23, 0.18, 0.32])
        details = [
            f"Provincia: {provincia}",
            f"Teléfono: {telefono}",
            f"IVA: {condicion_iva}",
            f"C.U.I.T.: {cuit}",
        ]
        self.set_x(left_x)
        for idx, (width, text) in enumerate(zip(details_widths, details)):
            clipped = _truncate_text_to_width(
                self,
                text,
                width - (getattr(self, "c_margin", 0.5) * 2),
            )
            self.cell(width, 5, clipped, border=0, align="R" if idx == 3 else "L")
        self.ln(5)

        line_y = self.get_y()
        self.set_line_width(0.3)
        self.line(self.l_margin, line_y, self.w - self.r_margin, line_y)
        self.set_y(line_y + 1.5)

        self._draw_presupuesto_table_header()

    def _draw_presupuesto_continuation_header(self) -> None:
        self.set_draw_color(*COLOR_TEXT)
        self.set_text_color(*COLOR_TEXT)
        self.set_y(10)
        line_y = self.get_y() + 1
        self.set_line_width(0.3)
        self.line(self.l_margin, line_y, self.w - self.r_margin, line_y)
        self.set_y(line_y + 1.5)
        self._draw_presupuesto_table_header()

    def _format_qty(self, qty: float) -> str:
        if qty == int(qty):
            return str(int(qty))
        return f"{qty:.2f}"

    def _presupuesto_footer_height(self) -> float:
        line_height = 6
        return 3 + line_height + line_height

    def _draw_presupuesto_items_table(self) -> None:
        widths = self._presupuesto_table_widths()
        row_height = 7
        footer_height = self._presupuesto_footer_height()

        if not self.items:
            self.set_font("helvetica", "", 9)
            self.set_text_color(*COLOR_TEXT_MUTED)
            self.set_x(self.l_margin)
            self.cell(sum(widths), 8, "No hay ítems para este comprobante.", border=1, align="C")
            self.set_text_color(*COLOR_TEXT)
            self.ln()
            return

        for idx, item in enumerate(self.items):
            qty = _safe_float(item.get("cantidad"), 0.0)
            unit = _safe_float(item.get("precio_unitario"), 0.0)
            desc_imp = _safe_float(item.get("descuento_importe"), 0.0)
            total = _safe_float(item.get("total_linea"), None)
            if total is None:
                base = qty * unit
                sign = 1 if base >= 0 else -1
                total = base - (sign * max(0.0, desc_imp))

            code = str(item.get("articulo_codigo") or item.get("id_articulo") or "-")
            article = (
                item.get("descripcion_historica")
                or item.get("articulo_nombre")
                or item.get("descripcion")
                or f"Artículo {item.get('id_articulo', '-')}"
            )

            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            is_last_row = idx == (len(self.items) - 1)
            required_height = row_height + (footer_height if is_last_row else 0.0)
            if self.get_y() + required_height > page_break_at:
                self.add_page()

            if idx % 2 == 0:
                self.set_fill_color(*COLOR_LIGHT_GRAY)
            else:
                self.set_fill_color(*COLOR_WHITE)

            self.set_x(self.l_margin)
            self.set_draw_color(*COLOR_BORDER)

            code_text = _truncate_text_to_width(
                self,
                code,
                widths[0] - (getattr(self, "c_margin", 0.5) * 2),
            )
            article_text = _truncate_text_to_width(
                self,
                str(article),
                widths[2] - (getattr(self, "c_margin", 0.5) * 2),
            )

            self.cell(widths[0], row_height, code_text, border=1, align="C", fill=True)
            self.cell(widths[1], row_height, self._format_qty(qty), border=1, align="C", fill=True)
            self.cell(widths[2], row_height, f" {article_text}", border=1, align="L", fill=True)
            self.cell(widths[3], row_height, _format_money(unit) if self.show_prices else "---", border=1, align="R", fill=True)
            self.cell(widths[4], row_height, _format_money(total) if self.show_prices else "---", border=1, align="R", fill=True)
            self.ln()

    def _draw_presupuesto_totals_note_block(self) -> None:
        content_width = self.w - self.l_margin - self.r_margin
        line_height = 6
        required_height = self._presupuesto_footer_height()
        page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
        footer_start_y = page_break_at - required_height
        if self.get_y() > footer_start_y:
            self.add_page()
            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            footer_start_y = page_break_at - required_height
        self.set_y(max(self.get_y(), footer_start_y))

        line_count = len(self.items)
        qty_total = sum(_safe_float(item.get("cantidad"), 0.0) for item in self.items)
        desc_total = max(0.0, self.descuento_lineas) + max(0.0, self.descuento_global)
        desc_pct = max(0.0, _safe_float(self.doc.get("descuento_porcentaje"), 0.0))
        note = str(self.doc.get("observacion") or "-").strip() or "-"

        sep_y = self.get_y() + 1
        self.set_draw_color(*COLOR_TEXT)
        self.set_line_width(0.3)
        self.line(self.l_margin, sep_y, self.w - self.r_margin, sep_y)
        self.set_y(sep_y + 2)

        values = [
            f"Cant/Líneas: {line_count}",
            f"Cant/Prod: {self._format_qty(qty_total)}",
            f"Neto: {_format_money(self.neto)}" if self.show_prices else "Neto: ---",
            f"Desc: {_format_money(desc_total)}" if self.show_prices else "Desc: ---",
            (
                f"%Desc: {format_percent(desc_pct, decimals=2)} {_format_money(self.descuento_global)}"
                if self.show_prices
                else "%Desc: ---"
            ),
            f"Total: {_format_money(self.total)}" if self.show_prices else "Total: ---",
        ]
        widths = _distribute_width(content_width, [0.13, 0.14, 0.16, 0.14, 0.22, 0.21])

        self.set_font("helvetica", "B", 9)
        self.set_text_color(*COLOR_TEXT)
        self.set_x(self.l_margin)
        for idx, (width, text) in enumerate(zip(widths, values)):
            max_width = width - (getattr(self, "c_margin", 0.5) * 2)
            clipped = _truncate_text_to_width(
                self,
                text,
                max_width,
                suffix="",
            )
            if clipped != text:
                clipped = clipped.rstrip(" ,.;:")
                if not clipped:
                    clipped = "-"
            self.cell(width, line_height, clipped, border=0, align="R" if idx == 5 else "L")
        self.ln()

        self.set_font("helvetica", "", 9)
        self.set_x(self.l_margin)
        note_text = _truncate_text_to_width(
            self,
            f"Nota: {note}",
            content_width - (getattr(self, "c_margin", 0.5) * 2),
        )
        self.cell(content_width, line_height, note_text, border=0, ln=1, align="L")

    def _resolve_neto(self) -> float:
        neto = _safe_float(self.doc.get("neto"), None)
        if neto is not None:
            return neto
        total = sum(_safe_float(item.get("cantidad"), 0.0) * _safe_float(item.get("precio_unitario"), 0.0) for item in self.items)
        return total

    def _resolve_subtotal_bruto(self) -> float:
        subtotal = _safe_float(self.doc.get("subtotal"), None)
        if subtotal is not None:
            return subtotal
        return sum(_safe_float(item.get("cantidad"), 0.0) * _safe_float(item.get("precio_unitario"), 0.0) for item in self.items)

    def _resolve_line_discount_total(self) -> float:
        return sum(_safe_float(item.get("descuento_importe"), 0.0) for item in self.items)

    def _resolve_global_discount_total(self) -> float:
        desc = _safe_float(self.doc.get("descuento_importe"), 0.0)
        return desc if desc > 0 else 0.0

    def _resolve_iva_total(self) -> float:
        iva = _safe_float(self.doc.get("iva_total"), None)
        if iva is not None and iva > 0:
            return iva
        summary_iva = sum(bucket["iva"] for bucket in self.tax_summary.values())
        if summary_iva > 0:
            return summary_iva
        return iva if iva is not None else 0.0

    def _resolve_total(self) -> float:
        total = _safe_float(self.doc.get("total"), None)
        if total is not None:
            return total
        return self.neto + self.iva_total

    def _should_discriminate_iva(self) -> bool:
        letter = str(self.doc.get("letra") or "").strip().upper()
        doc_type = str(self.doc.get("tipo_documento") or "").upper()
        if not letter and "FACTURA A" in doc_type:
            letter = "A"
        return self.is_invoice and letter == "A"

    def _build_tax_summary(self) -> Dict[float, Dict[str, float]]:
        precomputed = self.doc.get("iva_breakdown")
        if isinstance(precomputed, list):
            buckets: Dict[float, Dict[str, float]] = {}
            for row in precomputed:
                pct = round(_safe_float((row or {}).get("porcentaje_iva"), 0.0), 2)
                if pct <= 0:
                    continue
                base = _safe_float((row or {}).get("base_imponible"), 0.0)
                iva = _safe_float((row or {}).get("importe"), 0.0)
                if not base and not iva:
                    continue
                bucket = buckets.setdefault(pct, {"base": 0.0, "iva": 0.0})
                bucket["base"] += base
                bucket["iva"] += iva
            if buckets:
                return buckets

        buckets: Dict[float, Dict[str, float]] = {}
        for item in self.items:
            pct = round(_safe_float(item.get("porcentaje_iva"), 0.0), 2)
            if pct <= 0:
                continue
            base = _safe_float(item.get("total_linea"), None)
            if base is None:
                qty = _safe_float(item.get("cantidad"), 0.0)
                unit = _safe_float(item.get("precio_unitario"), 0.0)
                base = qty * unit
            bucket = buckets.setdefault(pct, {"base": 0.0, "iva": 0.0})
            bucket["base"] += base
            bucket["iva"] += base * (pct / 100.0)
        return buckets

    def _is_invoice_document(self) -> bool:
        doc_type = str(self.doc.get("tipo_documento") or "").upper()
        return "FACTURA" in doc_type

    def _is_presupuesto_document(self) -> bool:
        doc_type = str(self.doc.get("tipo_documento") or "").strip().upper()
        return doc_type == "PRESUPUESTO"

    def _draw_document_info(self) -> None:
        """Draw document number, date, and status."""
        number = self.doc.get("numero_serie") or "-"
        date = self.doc.get("fecha") or datetime.now().strftime("%Y-%m-%d")
        state = str(self.doc.get("estado") or "").upper()
        
        label_width = 35
        
        self.set_font("helvetica", "B", 10)
        self.set_text_color(*COLOR_TEXT)
        self.cell(label_width, 6, "Número:", border=0)
        self.set_font("helvetica", "", 10)
        self.cell(0, 6, str(number), border=0, ln=1)
        
        self.set_font("helvetica", "B", 10)
        self.cell(label_width, 6, "Fecha emisión:", border=0)
        self.set_font("helvetica", "", 10)
        self.cell(0, 6, _format_date(date), border=0, ln=1)
        
        self.set_font("helvetica", "B", 10)
        self.cell(label_width, 6, "Estado:", border=0)
        self.set_font("helvetica", "", 10)
        self.cell(0, 6, state or "-", border=0, ln=1)
        
        self.ln(2)

    def _draw_afip_block(self) -> None:
        """Draw CAE/AFIP info box with QR code."""
        cae = self.doc.get("cae") or "Pendiente"
        cae_vto = self.doc.get("cae_vencimiento")
        cuit_emisor = self.doc.get("cuit_emisor") or self._get_company_cuit()
        qr_data = self.doc.get("qr_data")
        
        start_y = self.get_y()
        box_x = self.l_margin
        box_width = self.w - self.l_margin - self.r_margin
        qr_size = 55 if qr_data else 0
        text_width = box_width - qr_size - 10 if qr_data else box_width - 10
        
        # Draw border box
        self.set_draw_color(*COLOR_BORDER)
        self.set_line_width(0.3)
        box_height = 30
        self.rect(box_x, start_y, text_width + 10, box_height)
        
        # CAE info inside box
        self.set_xy(box_x + 4, start_y + 4)
        self.set_font("helvetica", "B", 9)
        self.set_text_color(*COLOR_TEXT)
        self.cell(text_width, 5, f"CAE: {cae}", border=0, ln=1)
        
        self.set_x(box_x + 4)
        self.set_font("helvetica", "", 9)
        if cae_vto:
            self.cell(text_width, 5, f"Vencimiento CAE: {cae_vto}", border=0, ln=1)
            self.set_x(box_x + 4)
        self.cell(text_width, 5, f"CUIT Emisor: {cuit_emisor}", border=0, ln=1)
        
        # QR code on the right
        if qr_data:
            qr_x = box_x + text_width + 15
            qr_y = start_y - 5
            
            image_path = _create_qr_png(qr_data)
            if image_path:
                self._register_temp_image(image_path)
                self.image(image_path, x=qr_x, y=qr_y, w=qr_size, h=qr_size)
                # QR label
                self.set_xy(qr_x, qr_y + qr_size + 1)
                self.set_font("helvetica", "", 8)
                self.set_text_color(*COLOR_TEXT_MUTED)
                self.cell(qr_size, 4, "QR fiscal", border=0, align="C")
        
        self.set_y(start_y + max(box_height, qr_size) + 8)
        self.set_text_color(*COLOR_TEXT)

    def _draw_client_block(self) -> None:
        """Draw client information section."""
        name = self.entity.get("nombre_completo") or self.entity.get("razon_social") or "Consumidor Final"
        cuit = self.entity.get("cuit") or "-"
        addr = self.entity.get("domicilio") or self.doc.get("direccion_entrega") or "-"
        condition = self.entity.get("condicion_iva") or "-"

        # Section header
        self.set_fill_color(*COLOR_PRIMARY)
        self.set_text_color(*COLOR_WHITE)
        self.set_font("helvetica", "B", 10)
        self.cell(0, 7, "  Datos del Cliente", ln=1, fill=True)

        # Client data
        self.set_text_color(*COLOR_TEXT)
        self.set_font("helvetica", "", 9)
        self.ln(2)
        
        self.set_font("helvetica", "B", 9)
        self.cell(30, 5, "Razón social:", border=0)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, name, border=0, ln=1)
        
        self.set_font("helvetica", "B", 9)
        self.cell(30, 5, "CUIT:", border=0)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, cuit, border=0, ln=1)
        
        self.set_font("helvetica", "B", 9)
        self.cell(30, 5, "Domicilio:", border=0)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, addr, border=0, ln=1)
        
        self.set_font("helvetica", "B", 9)
        self.cell(30, 5, "Condición IVA:", border=0)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, condition, border=0, ln=1)

    def _draw_items_table(self) -> None:
        """Draw items table with proper pagination."""
        if self.show_prices:
            headers = ["Descripción", "Cant.", "Precio", "IVA %", "Desc. %", "Desc. $", "Total"]
            col_ratios = [0.34, 0.08, 0.12, 0.10, 0.10, 0.12, 0.14]
        else:
            headers = ["Descripción", "Cant."]
            col_ratios = [0.84, 0.16]
        start_x = self.l_margin
        table_width = max(self.w - self.l_margin - self.r_margin, 20)
        col_widths = _distribute_width(table_width, col_ratios)
        
        def _draw_table_header() -> None:
            self.set_font("helvetica", "B", 9)
            self.set_fill_color(*COLOR_PRIMARY)
            self.set_text_color(*COLOR_WHITE)
            self.set_x(start_x)
            for width, title in zip(col_widths, headers):
                self.cell(width, 8, title, border=0, align="C", fill=True)
            self.ln()
            self.set_text_color(*COLOR_TEXT)
            self.set_font("helvetica", "", 9)

        self._table_header_drawer = _draw_table_header
        self._table_header_last_page = 0
        self._table_header_active = True
        _draw_table_header()
        self._table_header_last_page = self.page_no()

        if not self.items:
            self.set_font("helvetica", "", 9)
            self.set_text_color(*COLOR_TEXT_MUTED)
            self.cell(table_width, 8, "No hay ítems para este comprobante.", border=1, align="C")
            self._table_header_active = False
            return

        row_height = 7
        for idx, item in enumerate(self.items):
            qty = _safe_float(item.get("cantidad"), 0.0)
            unit = _safe_float(item.get("precio_unitario"), 0.0)
            iva_pct = _safe_float(item.get("porcentaje_iva"), 0.0)
            desc_pct = _safe_float(item.get("descuento_porcentaje"), 0.0)
            desc_imp = _safe_float(item.get("descuento_importe"), 0.0)
            total = _safe_float(item.get("total_linea"), None)
            if total is None:
                base = qty * unit
                sign = 1 if base >= 0 else -1
                total = base - (sign * max(0.0, desc_imp))
            desc = (item.get("articulo_nombre") or item.get("descripcion") or f"Artículo {item.get('id_articulo', '-')}")
            
            # Alternating row colors
            if idx % 2 == 0:
                self.set_fill_color(*COLOR_LIGHT_GRAY)
            else:
                self.set_fill_color(*COLOR_WHITE)
            
            # Check for page break
            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            if self.get_y() + row_height > page_break_at:
                self.add_page()
            
            self.set_x(start_x)
            self.set_draw_color(*COLOR_BORDER)
            
            # Description (truncated if needed)
            desc_width = col_widths[0]
            content_width = desc_width - (getattr(self, "c_margin", 0.5) * 2)
            desc_text = _truncate_text_to_width(self, desc, max(content_width, 0))
            
            self.cell(col_widths[0], row_height, f" {desc_text}", border="TB", fill=True, align="L")
            
            # Format quantity as integer if it's a whole number
            qty_str = str(int(qty)) if qty == int(qty) else f"{qty:.2f}"
            self.cell(col_widths[1], row_height, qty_str, border="TB", align="C", fill=True)
            if self.show_prices:
                self.cell(col_widths[2], row_height, _format_money(unit), border="TB", align="R", fill=True)
                self.cell(col_widths[3], row_height, format_percent(iva_pct, decimals=2), border="TB", align="C", fill=True)
                self.cell(col_widths[4], row_height, format_percent(desc_pct, decimals=2), border="TB", align="R", fill=True)
                self.cell(col_widths[5], row_height, _format_money(desc_imp), border="TB", align="R", fill=True)
                self.cell(col_widths[6], row_height, _format_money(total), border="TB", align="R", fill=True)
            self.ln()

        self._table_header_active = False
        self._table_header_drawer = None

    def _draw_totals_block(self) -> None:
        """Draw totals section aligned to the right."""
        if not self.show_prices:
            self.ln(2)
            self.set_font("helvetica", "I", 9)
            self.set_text_color(*COLOR_TEXT_MUTED)
            self.cell(0, 6, "Precios e importes ocultos por configuración de impresión.", border=0, ln=1, align="R")
            self.set_text_color(*COLOR_TEXT)
            return

        totals_width = 95
        label_width = 57
        value_width = 38
        x_start = self.w - self.r_margin - totals_width
        
        self.ln(2)
        
        # Subtotal bruto
        self.set_x(x_start)
        self.set_font("helvetica", "", 10)
        self.cell(label_width, 6, "Subtotal bruto:", border=0, align="R")
        self.cell(value_width, 6, _format_money(self.subtotal_bruto), border=0, align="R")
        self.ln()

        if self.descuento_lineas > 0:
            self.set_x(x_start)
            self.cell(label_width, 6, "Descuento líneas:", border=0, align="R")
            self.cell(value_width, 6, f"- {_format_money(self.descuento_lineas)}", border=0, align="R")
            self.ln()

        if self.descuento_global > 0:
            desc_pct = _safe_float(self.doc.get("descuento_porcentaje"), 0.0)
            self.set_x(x_start)
            label = f"Descuento global ({format_percent(desc_pct, decimals=2)}):" if desc_pct > 0 else "Descuento global:"
            self.cell(label_width, 6, label, border=0, align="R")
            self.cell(value_width, 6, f"- {_format_money(self.descuento_global)}", border=0, align="R")
            self.ln()

        self.set_x(x_start)
        self.cell(label_width, 6, "Neto gravado:", border=0, align="R")
        self.cell(value_width, 6, _format_money(self.neto), border=0, align="R")
        self.ln()
        
        # IVA details for Factura A
        if self._should_discriminate_iva():
            self.set_x(x_start)
            self.cell(label_width, 6, "IVA total:", border=0, align="R")
            self.cell(value_width, 6, _format_money(self.iva_total), border=0, align="R")
            self.ln()
            
            if self.tax_summary:
                self.set_x(x_start)
                self.set_font("helvetica", "B", 9)
                self.cell(totals_width, 5, "Detalle de IVA", border=0, align="C")
                self.ln()
                self.set_font("helvetica", "", 8)
                for pct, values in sorted(self.tax_summary.items(), key=lambda x: x[0]):
                    self.set_x(x_start)
                    self.cell(label_width, 5, f"Base {format_percent(pct, decimals=2)}: {_format_money(values['base'])}  |  IVA:", border=0, align="R")
                    self.cell(value_width, 5, _format_money(values["iva"]), border=0, align="R")
                    self.ln()
        
        self.ln(2)
        
        # Total box
        self.set_x(x_start)
        self.set_fill_color(*COLOR_PRIMARY)
        self.set_text_color(*COLOR_WHITE)
        self.set_font("helvetica", "B", 12)
        self.cell(label_width, 10, "TOTAL:", border=0, align="R", fill=True)
        self.cell(value_width, 10, _format_money(self.total), border=0, align="R", fill=True)
        self.set_text_color(*COLOR_TEXT)
        self.ln()

    def _draw_footer_remarks(self) -> None:
        """Draw footer message."""
        self.ln(10)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(*COLOR_TEXT_MUTED)
        self.cell(0, 5, "Gracias por su compra - Este comprobante es emitido conforme a la normativa de AFIP.", border=0, align="C")
        self.set_text_color(*COLOR_TEXT)


class AfipInvoicePDF(BaseDocumentPDF):
    """Classic AFIP-style PDF renderer for invoice documents."""

    def __init__(
        self,
        doc_data: Dict[str, Any],
        entity_data: Dict[str, Any],
        items_data: List[Dict[str, Any]],
        company_config: Optional[Dict[str, Any]] = None,
        show_prices: bool = True,
    ):
        super().__init__(doc_data, entity_data, items_data, company_config, show_prices=show_prices)
        self._doc_type_full = self._resolve_doc_type_label()
        self._doc_letter = self._resolve_doc_letter()
        self._qr_payload = _extract_afip_qr_payload(self.doc.get("qr_data"))
        self._voucher_point, self._voucher_number = self._resolve_voucher_numbers()
        self.neto_gravado = _safe_float(self.doc.get("neto"), 0.0) or 0.0
        self.importe_no_gravado = (
            _safe_float(self.doc.get("importe_no_gravado"), None)
            or _safe_float(self.doc.get("imp_tot_conc"), 0.0)
            or 0.0
        )
        self.importe_otros_tributos = (
            _safe_float(self.doc.get("importe_otros_tributos"), None)
            or _safe_float(self.doc.get("imp_trib"), 0.0)
            or 0.0
        )
        self.total = _safe_float(self.doc.get("total"), 0.0) or 0.0
        self.iva_amounts = self._build_iva_amounts()

    def _resolve_doc_type_label(self) -> str:
        doc_type = str(self.doc.get("tipo_documento") or "FACTURA").strip().upper()
        letter = str(self.doc.get("letra") or "").strip().upper()
        base = doc_type
        if letter and doc_type.endswith(f" {letter}"):
            base = doc_type[: -len(f" {letter}")].strip()
        if not letter:
            parts = [p for p in doc_type.split(" ") if p]
            if parts and parts[-1] in {"A", "B", "C", "M"}:
                letter = parts[-1]
                base = " ".join(parts[:-1]) or doc_type
        if letter:
            return f"{base} {letter}".strip()
        return base

    def _resolve_doc_letter(self) -> str:
        letter = str(self.doc.get("letra") or "").strip().upper()
        if letter in {"A", "B", "C", "M"}:
            return letter

        doc_type = str(self.doc.get("tipo_documento") or "").strip().upper()
        parts = [p for p in doc_type.split(" ") if p]
        if parts and parts[-1] in {"A", "B", "C", "M"}:
            return parts[-1]

        label_parts = [p for p in str(self._doc_type_full or "").split(" ") if p]
        if label_parts and label_parts[-1] in {"A", "B", "C", "M"}:
            return label_parts[-1]
        return ""

    def _should_discriminate_iva(self) -> bool:
        return self._doc_letter not in {"B", "C"}

    def _resolve_voucher_numbers(self) -> Tuple[str, str]:
        qr_pto = _safe_int(self._qr_payload.get("ptoVta"), None)
        qr_nro = _safe_int(self._qr_payload.get("nroCmp"), None)
        db_pto = _safe_int(self.doc.get("punto_venta"), None)

        local_number_text = str(self.doc.get("numero_serie") or "").strip()
        local_pto = None
        local_nro = None
        if "-" in local_number_text:
            parts = [p.strip() for p in local_number_text.split("-") if p.strip()]
            if parts:
                local_pto = _safe_int(parts[0], None)
                local_nro = _safe_int(parts[-1], None)
        if local_nro is None:
            local_nro = _safe_int(local_number_text, None)

        point = qr_pto if qr_pto is not None else (db_pto if db_pto is not None else local_pto)
        number = qr_nro if qr_nro is not None else local_nro

        return _format_numeric_code(point, 5), _format_numeric_code(number, 8)

    def _build_iva_amounts(self) -> Dict[float, float]:
        amounts = {rate: 0.0 for rate in AFIP_IVA_RATES}
        breakdown = self.doc.get("iva_breakdown")

        found_breakdown = False
        if isinstance(breakdown, list):
            for row in breakdown:
                if not isinstance(row, dict):
                    continue
                rate = round(_safe_float(row.get("porcentaje_iva"), 0.0) or 0.0, 2)
                amount = _safe_float(row.get("importe"), 0.0) or 0.0
                if rate in amounts:
                    amounts[rate] += amount
                    found_breakdown = True

        if found_breakdown:
            return {rate: round(val, 2) for rate, val in amounts.items()}

        for item in self.items:
            rate = round(
                _safe_float(item.get("afip_alicuota_iva", item.get("porcentaje_iva")), 0.0) or 0.0,
                2,
            )
            if rate not in amounts or rate < 0:
                continue

            subtotal_no_iva = _safe_float(item.get("afip_subtotal_sin_iva"), None)
            subtotal_with_iva = _safe_float(item.get("afip_subtotal_con_iva"), None)
            if subtotal_no_iva is not None and subtotal_with_iva is not None:
                iva_amount = subtotal_with_iva - subtotal_no_iva
            elif subtotal_no_iva is not None:
                iva_amount = subtotal_no_iva * (rate / 100.0)
            else:
                line_total = _safe_float(item.get("total_linea"), 0.0) or 0.0
                if rate > 0:
                    divisor = 1 + (rate / 100.0)
                    iva_amount = line_total - (line_total / divisor)
                else:
                    iva_amount = 0.0
            amounts[rate] += iva_amount

        return {rate: round(val, 2) for rate, val in amounts.items()}

    def _display_amount(self, value: Any) -> str:
        return _format_money(value) if self.show_prices else "---"

    def _format_qty(self, qty: Any) -> str:
        value = _safe_float(qty, 0.0) or 0.0
        if value == int(value):
            return str(int(value))
        return f"{value:.2f}"

    def _ensure_space(self, required_height: float) -> None:
        page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
        if self.get_y() + required_height > page_break_at:
            self.add_page()

    def header(self) -> None:
        if self.page_no() <= 1:
            self._draw_main_header()
        else:
            self._draw_continuation_header()

        if self._table_header_active and self._table_header_drawer:
            current_page = self.page_no()
            if current_page > self._table_header_last_page:
                self._table_header_drawer()
                self._table_header_last_page = current_page

    def footer(self) -> None:
        # Se imprime la referencia de página dentro del bloque fiscal inferior.
        return

    def _draw_main_header(self) -> None:
        content_width = self.w - self.l_margin - self.r_margin
        company_name = self._get_company_razon_social()
        company_cuit = self._get_company_cuit()
        company_address = self._get_company_address()
        doc_date = _format_date(self.doc.get("fecha") or datetime.now().strftime("%Y-%m-%d"))

        entity_name = (
            self.entity.get("razon_social")
            or self.entity.get("nombre_completo")
            or f"{self.entity.get('apellido') or ''} {self.entity.get('nombre') or ''}".strip()
            or "Consumidor Final"
        )
        entity_doc = self.entity.get("cuit") or "-"
        entity_condition = self.entity.get("condicion_iva") or "-"
        entity_address = self.entity.get("domicilio") or self.doc.get("direccion_entrega") or "-"

        self.set_text_color(*COLOR_TEXT)
        self.set_draw_color(*COLOR_BORDER)
        self.set_line_width(0.3)

        self.set_y(7)
        self.set_font("helvetica", "B", 12)
        self.cell(0, 6, "ORIGINAL", border=0, ln=1, align="C")
        self.ln(1)

        top_y = self.get_y()
        top_h = 28
        left_w = content_width * 0.58
        right_w = content_width - left_w
        self.rect(self.l_margin, top_y, content_width, top_h)
        self.line(self.l_margin + left_w, top_y, self.l_margin + left_w, top_y + top_h)

        left_x = self.l_margin + 2
        left_text_w = left_w - 4
        self.set_xy(left_x, top_y + 3)
        self.set_font("helvetica", "B", 9)
        self.cell(left_text_w, 4, _truncate_text_to_width(self, f"Razón social: {company_name}", left_text_w), border=0, ln=1)
        self.set_x(left_x)
        self.set_font("helvetica", "", 8)
        self.cell(left_text_w, 4, _truncate_text_to_width(self, f"CUIT: {company_cuit}", left_text_w), border=0, ln=1)
        self.set_x(left_x)
        self.cell(left_text_w, 4, _truncate_text_to_width(self, f"Domicilio: {company_address}", left_text_w), border=0, ln=1)

        right_x = self.l_margin + left_w + 2
        right_text_w = right_w - 4
        self.set_xy(right_x, top_y + 3)
        self.set_font("helvetica", "B", 9)
        self.cell(right_text_w, 4, _truncate_text_to_width(self, self._doc_type_full, right_text_w), border=0, ln=1, align="R")
        self.set_x(right_x)
        self.set_font("helvetica", "", 8)
        self.cell(
            right_text_w,
            4,
            _truncate_text_to_width(
                self,
                f"Pto. Vta.: {self._voucher_point}   Comp. Nro: {self._voucher_number}",
                right_text_w,
            ),
            border=0,
            ln=1,
            align="R",
        )
        self.set_x(right_x)
        self.cell(right_text_w, 4, _truncate_text_to_width(self, f"Fecha emisión: {doc_date}", right_text_w), border=0, ln=1, align="R")

        client_y = top_y + top_h + 2
        client_h = 20
        self.rect(self.l_margin, client_y, content_width, client_h)
        client_x = self.l_margin + 2
        client_text_w = content_width - 4
        self.set_xy(client_x, client_y + 2)
        self.set_font("helvetica", "B", 8)
        self.cell(client_text_w, 4, _truncate_text_to_width(self, f"Cliente: {entity_name}", client_text_w), border=0, ln=1)
        self.set_x(client_x)
        self.set_font("helvetica", "", 8)
        self.cell(
            client_text_w,
            4,
            _truncate_text_to_width(
                self,
                f"CUIT/DNI: {entity_doc}   Condición IVA: {entity_condition}",
                client_text_w,
            ),
            border=0,
            ln=1,
        )
        self.set_x(client_x)
        self.cell(client_text_w, 4, _truncate_text_to_width(self, f"Domicilio: {entity_address}", client_text_w), border=0, ln=1)

        self.set_y(client_y + client_h + 3)

    def _draw_continuation_header(self) -> None:
        content_width = self.w - self.l_margin - self.r_margin
        self.set_text_color(*COLOR_TEXT)
        self.set_draw_color(*COLOR_BORDER)
        self.set_line_width(0.2)

        self.set_y(7)
        self.set_font("helvetica", "B", 10)
        self.cell(0, 5, "ORIGINAL", border=0, ln=1, align="C")
        self.set_font("helvetica", "", 8)
        self.set_x(self.l_margin)
        self.cell(
            content_width,
            4,
            _truncate_text_to_width(
                self,
                f"{self._doc_type_full} | Pto. Vta.: {self._voucher_point} | Comp. Nro: {self._voucher_number}",
                content_width,
            ),
            border=0,
            ln=1,
        )
        line_y = self.get_y() + 1
        self.line(self.l_margin, line_y, self.w - self.r_margin, line_y)
        self.set_y(line_y + 2)

    def build(self) -> None:
        self._draw_items_table()
        self.ln(3)
        self._draw_totals_matrix()
        self.ln(3)
        self._draw_fiscal_footer_block()

    def _draw_items_table(self) -> None:
        discriminate_iva = self._should_discriminate_iva()
        table_width = max(self.w - self.l_margin - self.r_margin, 20)
        if discriminate_iva:
            headers = [
                "Código",
                "Producto",
                "Cantidad",
                "Unidad",
                "Precio unitario",
                "% Bonificación",
                "Subtotal",
                "Alícuota IVA",
                "Subtotal c/IVA",
            ]
            widths = _distribute_width(table_width, [0.10, 0.24, 0.08, 0.08, 0.12, 0.10, 0.10, 0.08, 0.10])
        else:
            headers = [
                "Código",
                "Producto",
                "Cantidad",
                "Unidad",
                "Precio unitario",
                "% Bonificación",
                "Importe",
            ]
            widths = _distribute_width(table_width, [0.11, 0.36, 0.09, 0.08, 0.14, 0.10, 0.12])
        row_height = 6
        start_x = self.l_margin

        def _draw_table_header() -> None:
            self.set_x(start_x)
            self.set_font("helvetica", "B", 7)
            self.set_fill_color(*COLOR_PRIMARY)
            self.set_text_color(*COLOR_WHITE)
            self.set_draw_color(*COLOR_PRIMARY)
            for width, title in zip(widths, headers):
                self.cell(width, 7, _truncate_text_to_width(self, title, width - 2), border=1, align="C", fill=True)
            self.ln()
            self.set_text_color(*COLOR_TEXT)
            self.set_draw_color(*COLOR_BORDER)
            self.set_font("helvetica", "", 8)

        self._table_header_drawer = _draw_table_header
        self._table_header_last_page = 0
        self._table_header_active = True
        _draw_table_header()
        self._table_header_last_page = self.page_no()

        if not self.items:
            self.set_font("helvetica", "", 9)
            self.set_text_color(*COLOR_TEXT_MUTED)
            self.cell(table_width, 8, "No hay ítems para esta factura.", border=1, align="C")
            self._table_header_active = False
            self._table_header_drawer = None
            self.set_text_color(*COLOR_TEXT)
            return

        for idx, item in enumerate(self.items):
            self._ensure_space(row_height)

            if idx % 2 == 0:
                self.set_fill_color(*COLOR_LIGHT_GRAY)
            else:
                self.set_fill_color(*COLOR_WHITE)

            code_text = str(item.get("articulo_codigo") or item.get("id_articulo") or "-")
            product_text = (
                item.get("descripcion_historica")
                or item.get("articulo_nombre")
                or item.get("descripcion")
                or f"Artículo {item.get('id_articulo', '-')}"
            )
            qty_text = self._format_qty(item.get("cantidad"))
            unit_text = str(item.get("unidad_abreviatura") or "UNI").strip().upper() or "UNI"
            bonif_pct = _safe_float(item.get("afip_bonificacion_pct", item.get("descuento_porcentaje")), 0.0) or 0.0
            alicuota = _safe_float(item.get("afip_alicuota_iva", item.get("porcentaje_iva")), 0.0) or 0.0
            unit_price = _safe_float(item.get("precio_unitario"), 0.0) or 0.0
            subtotal_no_iva = _safe_float(item.get("afip_subtotal_sin_iva"), None)
            if subtotal_no_iva is None:
                subtotal_no_iva = _safe_float(item.get("total_linea"), 0.0) or 0.0
            subtotal_with_iva = _safe_float(item.get("afip_subtotal_con_iva"), None)
            if subtotal_with_iva is None:
                subtotal_with_iva = _safe_float(item.get("total_linea"), 0.0) or 0.0

            if discriminate_iva:
                values = [
                    _truncate_text_to_width(self, code_text, widths[0] - 1.5),
                    _truncate_text_to_width(self, str(product_text), widths[1] - 1.5),
                    qty_text,
                    _truncate_text_to_width(self, unit_text, widths[3] - 1.5),
                    self._display_amount(unit_price),
                    format_percent(bonif_pct, decimals=2),
                    self._display_amount(subtotal_no_iva),
                    format_percent(alicuota, decimals=2),
                    self._display_amount(subtotal_with_iva),
                ]
                aligns = ["C", "L", "C", "C", "R", "R", "R", "C", "R"]
            else:
                values = [
                    _truncate_text_to_width(self, code_text, widths[0] - 1.5),
                    _truncate_text_to_width(self, str(product_text), widths[1] - 1.5),
                    qty_text,
                    _truncate_text_to_width(self, unit_text, widths[3] - 1.5),
                    self._display_amount(unit_price),
                    format_percent(bonif_pct, decimals=2),
                    self._display_amount(subtotal_with_iva),
                ]
                aligns = ["C", "L", "C", "C", "R", "R", "R"]

            self.set_x(start_x)
            for width, value, align in zip(widths, values, aligns):
                text = f" {value}" if align == "L" else value
                self.cell(width, row_height, text, border=1, align=align, fill=True)
            self.ln()

        self._table_header_active = False
        self._table_header_drawer = None

    def _draw_totals_matrix(self) -> None:
        if self._should_discriminate_iva():
            rows = [
                ("Importe neto no gravado", self.importe_no_gravado),
                ("Importe neto gravado", self.neto_gravado),
                ("IVA 27%", self.iva_amounts.get(27.0, 0.0)),
                ("IVA 21%", self.iva_amounts.get(21.0, 0.0)),
                ("IVA 10.5%", self.iva_amounts.get(10.5, 0.0)),
                ("IVA 5%", self.iva_amounts.get(5.0, 0.0)),
                ("IVA 2.5%", self.iva_amounts.get(2.5, 0.0)),
                ("IVA 0%", self.iva_amounts.get(0.0, 0.0)),
                ("Importe otros tributos", self.importe_otros_tributos),
                ("Importe total", self.total),
            ]
        else:
            subtotal_iva_incluido = self.total - self.importe_no_gravado - self.importe_otros_tributos
            rows = [
                ("Importe neto no gravado", self.importe_no_gravado),
                ("Subtotal (IVA incluido)", subtotal_iva_incluido),
                ("Importe otros tributos", self.importe_otros_tributos),
                ("Importe total", self.total),
            ]

        row_height = 6
        table_width = 104
        label_width = 70
        value_width = table_width - label_width
        x_start = self.w - self.r_margin - table_width
        self._ensure_space((len(rows) * row_height) + 2)

        for idx, (label, value) in enumerate(rows):
            is_total = idx == (len(rows) - 1)
            self.set_x(x_start)
            if is_total:
                self.set_fill_color(*COLOR_PRIMARY)
                self.set_text_color(*COLOR_WHITE)
                self.set_font("helvetica", "B", 9)
            else:
                self.set_fill_color(*COLOR_LIGHT_GRAY if idx % 2 == 0 else COLOR_WHITE)
                self.set_text_color(*COLOR_TEXT)
                self.set_font("helvetica", "", 8)
            self.set_draw_color(*COLOR_BORDER)
            self.cell(label_width, row_height, f" {label}", border=1, align="L", fill=True)
            self.cell(value_width, row_height, self._display_amount(value), border=1, align="R", fill=True)
            self.ln()

        self.set_text_color(*COLOR_TEXT)

    def _draw_fiscal_footer_block(self) -> None:
        block_height = 34
        self._ensure_space(block_height + 1)

        x = self.l_margin
        y = self.get_y()
        width = self.w - self.l_margin - self.r_margin
        qr_width = 34

        self.set_draw_color(*COLOR_BORDER)
        self.set_line_width(0.3)
        self.rect(x, y, width, block_height)
        self.line(x + qr_width, y, x + qr_width, y + block_height)

        qr_data = str(self.doc.get("qr_data") or "").strip()
        if qr_data:
            image_path = _create_qr_png(qr_data)
            if image_path:
                self._register_temp_image(image_path)
                self.image(image_path, x=x + 4, y=y + 4, w=26, h=26)
        else:
            self.set_xy(x + 3, y + 12)
            self.set_font("helvetica", "", 7)
            self.set_text_color(*COLOR_TEXT_MUTED)
            self.cell(qr_width - 6, 5, "Sin QR fiscal", border=0, align="C")

        right_x = x + qr_width + 2
        right_w = width - qr_width - 4
        cae = str(self.doc.get("cae") or "").strip()
        cae_vto = _format_date(self.doc.get("cae_vencimiento")) if self.doc.get("cae_vencimiento") else "-"
        status_text = "Comprobante autorizado" if cae else "Comprobante no autorizado"
        page_text = f"Pág. {self.page_no()} de {{nb}}"

        self.set_xy(right_x, y + 3)
        self.set_font("helvetica", "B", 9)
        if cae:
            self.set_text_color(16, 185, 129)
        else:
            self.set_text_color(220, 38, 38)
        self.cell(right_w, 5, status_text, border=0, ln=1)

        self.set_x(right_x)
        self.set_text_color(*COLOR_TEXT)
        self.set_font("helvetica", "", 8)
        self.cell(right_w, 4, page_text, border=0, ln=1)

        self.set_x(right_x)
        self.cell(right_w, 4, f"CAE: {cae or 'Pendiente'}", border=0, ln=1)
        self.set_x(right_x)
        self.cell(right_w, 4, f"Fecha vencimiento CAE: {cae_vto}", border=0, ln=1)

        self.set_y(y + block_height + 2)
        self.set_text_color(*COLOR_TEXT)


class RemitoPDF(BaseDocumentPDF):
    """PDF generator for remitos with presupuesto-like layout."""
    
    def __init__(
        self,
        remito_data: Dict[str, Any],
        entity_data: Dict[str, Any],
        items_data: List[Dict[str, Any]],
        company_config: Optional[Dict[str, Any]] = None,
        show_prices: bool = True,
    ):
        super().__init__(remito_data, entity_data, items_data, company_config, show_prices=show_prices)
        self.remito = remito_data or {}
        self._doc_type_label = "REMITO"
        self._doc_letter = ""

    def header(self) -> None:
        if self.page_no() <= 1:
            self._draw_remito_page_header()
        else:
            self._draw_remito_continuation_header()

    def build(self) -> None:
        self._draw_remito_items_table()
        self._draw_remito_totals_note_block()
        self._draw_remito_footer_block()

    def _resolve_client_name(self) -> str:
        razon_social = str(self.entity.get("razon_social") or "").strip()
        if razon_social:
            return razon_social

        nombre = str(self.entity.get("nombre") or "").strip()
        apellido = str(self.entity.get("apellido") or "").strip()
        nombre_apellido = f"{nombre} {apellido}".strip()
        if nombre_apellido:
            return nombre_apellido

        nombre_completo = str(self.entity.get("nombre_completo") or "").strip()
        return nombre_completo or "Consumidor Final"

    def _remito_table_headers(self) -> List[str]:
        return ["Código", "Cant.", "Artículos", "Costo/Uni", "Importe"]

    def _remito_table_widths(self) -> List[float]:
        table_width = max(self.w - self.l_margin - self.r_margin, 20)
        return _distribute_width(table_width, [0.14, 0.10, 0.42, 0.17, 0.17])

    def _draw_remito_table_header(self) -> None:
        headers = self._remito_table_headers()
        widths = self._remito_table_widths()

        self.set_x(self.l_margin)
        self.set_font("helvetica", "B", 9)
        self.set_fill_color(*COLOR_LIGHT_GRAY)
        self.set_text_color(*COLOR_TEXT)
        self.set_draw_color(*COLOR_BORDER)
        for width, title in zip(widths, headers):
            self.cell(width, 7, title, border=1, align="C", fill=True)
        self.ln()
        self.set_font("helvetica", "", 9)

    def _draw_remito_page_header(self) -> None:
        number = self.remito.get("numero") or "-"
        date = self.remito.get("fecha") or datetime.now().strftime("%Y-%m-%d")

        client_name = self._resolve_client_name()
        domicilio = self.entity.get("domicilio") or self.remito.get("direccion_entrega") or "-"
        provincia = self.entity.get("provincia") or "-"
        telefono = self.entity.get("telefono") or "-"
        condicion_iva = self.entity.get("condicion_iva") or "-"
        cuit = self.entity.get("cuit") or "-"

        content_width = self.w - self.l_margin - self.r_margin
        left_x = self.l_margin
        c_margin = getattr(self, "c_margin", 0.5)

        self.set_draw_color(*COLOR_TEXT)
        self.set_text_color(*COLOR_TEXT)
        self.set_y(10)
        self.set_x(left_x)

        self.set_font("helvetica", "B", 12)
        self.cell(content_width * 0.62, 6, f"REMITO N°: {number}", border=0, align="L")
        self.set_font("helvetica", "B", 10)
        self.cell(content_width * 0.38, 6, f"FECHA: {_format_date(date)}", border=0, align="R")
        self.ln(5)

        self.set_x(left_x)
        self.set_font("helvetica", "B", 10)
        self.cell(0, 5, "X", border=0, ln=1, align="C")

        self.set_x(left_x)
        self.set_font("helvetica", "B", 9)
        self.cell(0, 5, "DOCUMENTO NO VÁLIDO COMO FACTURA", border=0, ln=1, align="C")
        self.ln(1)

        left_width = content_width * 0.62
        right_width = content_width - left_width
        self.set_font("helvetica", "", 9)
        self.set_x(left_x)
        client_text = _truncate_text_to_width(
            self,
            f"Cliente: {client_name}",
            left_width - (c_margin * 2),
        )
        dom_text = _truncate_text_to_width(
            self,
            f"Dom: {domicilio}",
            right_width - (c_margin * 2),
        )
        self.cell(left_width, 5, client_text, border=0, align="L")
        self.cell(right_width, 5, dom_text, border=0, align="R")
        self.ln()

        details_widths = _distribute_width(content_width, [0.31, 0.29, 0.40])
        details = [
            f"Provincia: {provincia}",
            f"Teléfono: {telefono}",
            f"C.U.I.T.: {cuit}",
        ]
        self.set_x(left_x)
        for idx, (width, text) in enumerate(zip(details_widths, details)):
            clipped = _truncate_text_to_width(
                self,
                text,
                width - (c_margin * 2),
            )
            self.cell(width, 5, clipped, border=0, align="R" if idx == 2 else "L")
        # Advance by full row height to avoid overlap with the IVA row.
        self.ln(5)

        # Render IVA condition on its own row to avoid truncation.
        self.set_x(left_x)
        _safe_multicell(
            self,
            content_width,
            5,
            f"Condición IVA: {condicion_iva}",
            border=0,
            align="L",
        )
        self.ln(2)

        line_y = self.get_y()
        self.set_line_width(0.3)
        self.line(self.l_margin, line_y, self.w - self.r_margin, line_y)
        self.set_y(line_y + 1.5)

        self._draw_remito_table_header()

    def _draw_remito_continuation_header(self) -> None:
        self.set_draw_color(*COLOR_TEXT)
        self.set_text_color(*COLOR_TEXT)
        self.set_y(10)
        line_y = self.get_y() + 1
        self.set_line_width(0.3)
        self.line(self.l_margin, line_y, self.w - self.r_margin, line_y)
        self.set_y(line_y + 1.5)
        self._draw_remito_table_header()

    def _format_qty(self, qty: float) -> str:
        if qty == int(qty):
            return str(int(qty))
        return f"{qty:.2f}"

    def _remito_totals_note_height(self) -> float:
        line_height = 6
        return 3 + line_height + line_height

    def _remito_footer_content_height(self) -> float:
        line_height = 6
        signature_block_height = 21
        return 3 + line_height + line_height + 4 + line_height + signature_block_height

    def _remito_final_block_height(self) -> float:
        return self._remito_totals_note_height() + self._remito_footer_content_height()

    def _draw_remito_items_table(self) -> None:
        widths = self._remito_table_widths()
        row_height = 7
        c_margin = getattr(self, "c_margin", 0.5)
        footer_block_height = self._remito_final_block_height()

        if not self.items:
            self.set_font("helvetica", "", 9)
            self.set_text_color(*COLOR_TEXT_MUTED)
            self.set_x(self.l_margin)
            self.cell(sum(widths), 8, "No hay líneas disponibles para este remito.", border=1, align="C")
            self.set_text_color(*COLOR_TEXT)
            self.ln()
            return

        for idx, item in enumerate(self.items):
            qty = _safe_float(item.get("cantidad"), 0.0) or 0.0
            unit = _safe_float(item.get("precio_unitario"), None)
            total = _safe_float(item.get("total_linea"), None)
            if total is None and unit is not None:
                total = qty * unit

            code = str(item.get("articulo_codigo") or item.get("id_articulo") or "-")
            article = (
                item.get("descripcion_historica")
                or item.get("articulo_nombre")
                or item.get("articulo")
                or item.get("descripcion")
                or f"Artículo {item.get('id_articulo', '-')}"
            )

            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            is_last_row = idx == (len(self.items) - 1)
            required_height = row_height + (footer_block_height if is_last_row else 0.0)
            if self.get_y() + required_height > page_break_at:
                self.add_page()

            self.set_fill_color(*(COLOR_LIGHT_GRAY if idx % 2 == 0 else COLOR_WHITE))
            self.set_x(self.l_margin)
            self.set_draw_color(*COLOR_BORDER)

            code_text = _truncate_text_to_width(
                self,
                code,
                widths[0] - (c_margin * 2),
            )
            article_text = _truncate_text_to_width(
                self,
                str(article),
                widths[2] - (c_margin * 2),
            )
            unit_text = _format_money(unit) if self.show_prices and unit is not None else "---"
            total_text = _format_money(total) if self.show_prices and total is not None else "---"

            self.cell(widths[0], row_height, code_text, border=1, align="C", fill=True)
            self.cell(widths[1], row_height, self._format_qty(qty), border=1, align="C", fill=True)
            self.cell(widths[2], row_height, f" {article_text}", border=1, align="L", fill=True)
            self.cell(widths[3], row_height, unit_text, border=1, align="R", fill=True)
            self.cell(widths[4], row_height, total_text, border=1, align="R", fill=True)
            self.ln()

    def _resolve_remito_neto(self) -> float:
        neto = _safe_float(self.remito.get("neto"), None)
        if neto is not None:
            return neto

        computed = 0.0
        for item in self.items:
            total_line = _safe_float(item.get("total_linea"), None)
            if total_line is None:
                qty = _safe_float(item.get("cantidad"), 0.0) or 0.0
                unit = _safe_float(item.get("precio_unitario"), 0.0) or 0.0
                total_line = qty * unit
            computed += total_line
        return computed

    def _resolve_remito_total(self) -> float:
        total = _safe_float(self.remito.get("total"), None)
        if total is not None:
            return total
        return self._resolve_remito_neto()

    def _resolve_remito_line_discount_total(self) -> float:
        return sum(_safe_float(item.get("descuento_importe"), 0.0) or 0.0 for item in self.items)

    def _resolve_remito_global_discount_total(self) -> float:
        desc = _safe_float(self.remito.get("descuento_importe"), 0.0) or 0.0
        return desc if desc > 0 else 0.0

    def _draw_remito_totals_note_block(self) -> None:
        content_width = self.w - self.l_margin - self.r_margin
        line_height = 6
        required_height = self._remito_totals_note_height()
        final_block_height = self._remito_final_block_height()
        page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
        footer_start_y = page_break_at - final_block_height
        if self.get_y() > footer_start_y:
            self.add_page()
            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            footer_start_y = page_break_at - final_block_height
        self.set_y(max(self.get_y(), footer_start_y))

        line_count = len(self.items)
        qty_total = sum(_safe_float(item.get("cantidad"), 0.0) or 0.0 for item in self.items)
        neto = self._resolve_remito_neto()
        total = self._resolve_remito_total()
        desc_lineas = max(0.0, self._resolve_remito_line_discount_total())
        desc_global = max(0.0, self._resolve_remito_global_discount_total())
        desc_total = desc_lineas + desc_global
        desc_pct = max(0.0, _safe_float(self.remito.get("descuento_porcentaje"), 0.0) or 0.0)
        note = str(self.remito.get("observacion") or "-").strip() or "-"

        sep_y = self.get_y() + 1
        self.set_draw_color(*COLOR_TEXT)
        self.set_line_width(0.3)
        self.line(self.l_margin, sep_y, self.w - self.r_margin, sep_y)
        self.set_y(sep_y + 2)

        values = [
            f"Cant/Líneas: {line_count}",
            f"Cant/Prod: {self._format_qty(qty_total)}",
            f"Neto: {_format_money(neto)}" if self.show_prices else "Neto: ---",
            f"Desc: {_format_money(desc_total)}" if self.show_prices else "Desc: ---",
            (
                f"%Desc: {format_percent(desc_pct, decimals=2)} {_format_money(desc_global)}"
                if self.show_prices
                else "%Desc: ---"
            ),
            f"Total: {_format_money(total)}" if self.show_prices else "Total: ---",
        ]
        widths = _distribute_width(content_width, [0.13, 0.14, 0.16, 0.14, 0.22, 0.21])

        self.set_font("helvetica", "B", 9)
        self.set_text_color(*COLOR_TEXT)
        self.set_x(self.l_margin)
        for idx, (width, text) in enumerate(zip(widths, values)):
            max_width = width - (getattr(self, "c_margin", 0.5) * 2)
            clipped = _truncate_text_to_width(
                self,
                text,
                max_width,
                suffix="",
            )
            if clipped != text:
                clipped = clipped.rstrip(" ,.;:")
                if not clipped:
                    clipped = "-"
            self.cell(width, line_height, clipped, border=0, align="R" if idx == 5 else "L")
        self.ln()

        self.set_font("helvetica", "", 9)
        self.set_x(self.l_margin)
        note_text = _truncate_text_to_width(
            self,
            f"Nota: {note}",
            content_width - (getattr(self, "c_margin", 0.5) * 2),
        )
        self.cell(content_width, line_height, note_text, border=0, ln=1, align="L")

    def _draw_remito_footer_block(self) -> None:
        content_width = self.w - self.l_margin - self.r_margin
        c_margin = getattr(self, "c_margin", 0.5)
        direccion = str(self.remito.get("direccion_entrega") or "-").strip() or "-"
        observacion = str(self.remito.get("observacion") or "-").strip() or "-"

        line_height = 6
        required_height = self._remito_footer_content_height()
        page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
        if self.get_y() + required_height > page_break_at:
            self.add_page()

        sep_y = self.get_y() + 1
        self.set_draw_color(*COLOR_TEXT)
        self.set_line_width(0.3)
        self.line(self.l_margin, sep_y, self.w - self.r_margin, sep_y)
        self.set_y(sep_y + 2)

        self.set_font("helvetica", "", 9)
        direccion_text = _truncate_text_to_width(
            self,
            f"Dirección de entrega: {direccion}",
            content_width - (c_margin * 2),
        )
        self.set_x(self.l_margin)
        self.cell(content_width, line_height, direccion_text, border=0, ln=1, align="L")

        observacion_text = _truncate_text_to_width(
            self,
            f"Observación: {observacion}",
            content_width - (c_margin * 2),
        )
        self.set_x(self.l_margin)
        self.cell(content_width, line_height, observacion_text, border=0, ln=1, align="L")

        self.ln(3)
        self.set_font("helvetica", "B", 9)
        self.set_x(self.l_margin)
        self.cell(content_width, line_height, "Recibí conforme", border=0, ln=1, align="L")

        self.set_font("helvetica", "", 9)
        self.set_x(self.l_margin)
        self.cell(content_width, 6, "Firma: ________________________________", border=0, ln=1, align="L")
        self.set_x(self.l_margin)
        self.cell(content_width, 6, "Aclaración: ___________________________", border=0, ln=1, align="L")
        self.set_x(self.l_margin)
        self.cell(content_width, 6, "Fecha: ____/____/______", border=0, ln=1, align="L")


def generate_pdf_and_open(
    doc_data: Dict[str, Any],
    entity_data: Dict[str, Any],
    items_data: List[Dict[str, Any]],
    *,
    kind: str = "invoice",
    company_config: Optional[Dict[str, Any]] = None,
    show_prices: bool = True,
) -> str:
    """
    Generate a PDF document and open it.
    
    Args:
        doc_data: Document data (invoice, quote, remito, etc.)
        entity_data: Client/entity data
        items_data: Line items data
        kind: Document type - "invoice" or "remito"
        company_config: Optional company configuration dict with keys:
            - nombre_sistema: Company name
            - razon_social: Legal business name
            - cuit_empresa: Tax ID
            - domicilio_empresa: Company address
            - slogan: Company slogan
        show_prices: If False, hides monetary values in printed PDF.
    
    Returns:
        Path to the generated PDF file
    """
    doc_type_name = str(doc_data.get("tipo_documento") or "").upper()

    if kind == "remito":
        pdf = RemitoPDF(doc_data, entity_data, items_data, company_config, show_prices=show_prices)
    elif kind == "invoice" and "FACTURA" in doc_type_name:
        pdf = AfipInvoicePDF(doc_data, entity_data, items_data, company_config, show_prices=show_prices)
    else:
        pdf = InvoicePDF(doc_data, entity_data, items_data, company_config, show_prices=show_prices)

    path = pdf.generate()
    _open_pdf(path)
    return path
