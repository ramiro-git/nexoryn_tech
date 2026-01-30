import logging
import os
import platform
import subprocess
import tempfile
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from fpdf import FPDF

logger = logging.getLogger(__name__)


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _format_money(value: Any) -> str:
    number = _safe_float(value, None)
    if number is None:
        return "-"
    return f"${number:,.2f}"


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
        qr = qrcode.QRCode(box_size=3, border=1)
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
    def __init__(self, doc_data: Dict[str, Any], entity_data: Dict[str, Any], items_data: List[Dict[str, Any]]):
        super().__init__(orientation="P", unit="mm", format="A4")
        self.doc = doc_data or {}
        self.entity = entity_data or {}
        self.items = items_data or []
        self.set_auto_page_break(True, margin=25)
        self.set_margins(10, 35, 10)
        self._temp_images: List[str] = []
        self._table_header_active: bool = False
        self._table_header_drawer: Optional[Callable[[], None]] = None
        self._table_header_last_page: int = 0

    def header(self) -> None:  # pragma: no cover - visual
        self.set_fill_color(15, 23, 42)
        self.set_draw_color(15, 23, 42)
        self.rect(0, 0, self.w, 32, "F")
        self.set_font("helvetica", "B", 18)
        self.set_text_color(255, 255, 255)
        self.set_xy(12, 8)
        self.cell(0, 10, "NEXORYN TECH", border=0, ln=1)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, "Soluciones tecnológicas y logísticas", border=0, ln=1)
        self.set_text_color(0, 0, 0)
        self.ln(4)
        try:
            self.set_y(self.t_margin + 4)
        except Exception:
            pass
        if self._table_header_active and self._table_header_drawer:
            current_page = self.page_no()
            if current_page > self._table_header_last_page:
                self._table_header_drawer()
                self._table_header_last_page = current_page

    def footer(self) -> None:  # pragma: no cover - visual
        self.set_y(-18)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(100)
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
    def __init__(self, doc_data: Dict[str, Any], entity_data: Dict[str, Any], items_data: List[Dict[str, Any]]):
        super().__init__(doc_data, entity_data, items_data)
        self.is_invoice = self._is_invoice_document()
        self.tax_summary = self._build_tax_summary()
        self.neto = self._resolve_neto()
        self.iva_total = self._resolve_iva_total()
        self.total = self._resolve_total()

    def build(self) -> None:
        self._draw_document_header()
        self._draw_client_block()
        self.ln(2)
        self._draw_items_table()
        self.ln(4)
        self._draw_totals_block()
        self.ln(5)
        self._draw_footer_remarks()

    def _resolve_neto(self) -> float:
        neto = _safe_float(self.doc.get("neto"), None)
        if neto is not None and neto > 0:
            return neto
        total = sum(_safe_float(item.get("cantidad"), 0.0) * _safe_float(item.get("precio_unitario"), 0.0) for item in self.items)
        return total

    def _resolve_iva_total(self) -> float:
        iva = _safe_float(self.doc.get("iva_total"), None)
        if iva is not None and iva > 0:
            return iva
        return sum(bucket["iva"] for bucket in self.tax_summary.values())

    def _resolve_total(self) -> float:
        total = _safe_float(self.doc.get("total"), None)
        if total is not None and total > 0:
            return total
        return self.neto + self.iva_total

    def _should_discriminate_iva(self) -> bool:
        letter = str(self.doc.get("letra") or "").strip().upper()
        doc_type = str(self.doc.get("tipo_documento") or "").upper()
        if not letter and "FACTURA A" in doc_type:
            letter = "A"
        return self.is_invoice and letter == "A"

    def _build_tax_summary(self) -> Dict[float, Dict[str, float]]:
        buckets: Dict[float, Dict[str, float]] = {}
        for item in self.items:
            pct = round(_safe_float(item.get("porcentaje_iva"), 0.0), 2)
            if pct <= 0:
                continue
            qty = _safe_float(item.get("cantidad"), 0.0)
            unit = _safe_float(item.get("precio_unitario"), 0.0)
            base = qty * unit
            bucket = buckets.setdefault(pct, {"base": 0.0, "iva": 0.0})
            bucket["base"] += base
            bucket["iva"] += base * (pct / 100.0)
        return buckets

    def _draw_document_header(self) -> None:
        doc_type = str(self.doc.get("tipo_documento") or "COMPROBANTE").strip()
        letter = str(self.doc.get("letra") or "").strip().upper()
        number = self.doc.get("numero_serie") or "-"
        date = self.doc.get("fecha") or datetime.now().strftime("%Y-%m-%d")
        state = str(self.doc.get("estado") or "").upper()
        doc_type_upper = doc_type.upper()
        if letter:
            if doc_type_upper.endswith(f" {letter}") or f" {letter} " in doc_type_upper or doc_type_upper == letter:
                doc_label = doc_type
            else:
                doc_label = f"{doc_type} {letter}".strip()
        else:
            doc_label = doc_type or "COMPROBANTE"
        self.ln(4)

        self.set_font("helvetica", "B", 14)
        self.cell(0, 8, doc_label, ln=1)
        self.set_font("helvetica", "", 10)
        self.cell(0, 5, f"Número: {number}", ln=1)
        self.cell(0, 5, f"Fecha emisión: {date}", ln=1)
        self.cell(0, 5, f"Estado: {state or '-'}", ln=1)
        self.ln(2)
        if self.is_invoice:
            self._draw_afip_block()

    def _is_invoice_document(self) -> bool:
        doc_type = str(self.doc.get("tipo_documento") or "").upper()
        return "FACTURA" in doc_type

    def _draw_afip_block(self) -> None:
        cae = self.doc.get("cae") or "Pendiente"
        cae_vto = self.doc.get("cae_vencimiento")
        cuit_emisor = self.doc.get("cuit_emisor") or self.entity.get("cuit") or "-"
        qr_data = self.doc.get("qr_data")
        has_qr = bool(qr_data)
        block_height = 90 if has_qr else 72
        x = self.l_margin
        block_width = max(self.w - self.l_margin - self.r_margin, 0)
        y = self.get_y()
        text_margin = 6
        min_text_width = (getattr(self, "c_margin", 0.5) * 2) + 0.1
        desired_qr_width = min(130, max(90, block_width * (0.55 if has_qr else 0.38)))
        available_for_qr = max(0.0, block_width - min_text_width - text_margin)
        qr_section_width = min(desired_qr_width, available_for_qr)
        text_width = block_width - qr_section_width - text_margin
        if text_width < min_text_width:
            qr_section_width = max(0.0, block_width - min_text_width - text_margin)
            text_width = max(block_width - qr_section_width - text_margin, min_text_width)

        self.set_xy(x + 4, y + 4)
        self.set_font("helvetica", "B", 9)
        self.cell(text_width, 5, f"CAE: {cae}", ln=1)
        self.set_font("helvetica", "", 9)
        if cae_vto:
            self.cell(text_width, 5, f"Vencimiento CAE: {cae_vto}", ln=1)
        self.cell(text_width, 5, f"CUIT Emisor: {cuit_emisor}", ln=1)
        if qr_section_width > min_text_width:
            qr_x = x + block_width - qr_section_width - text_margin
            self._draw_qr_section(qr_x, y + 4, qr_section_width, block_height - 8, qr_data)
        self.set_y(y + block_height + 4)

    def _draw_qr_section(self, x: float, y: float, width: float, height: float, data: Optional[str]) -> None:
        self.set_xy(x, y)
        if not data:
            self.set_font("helvetica", "", 7)
            _safe_multicell(self, width, 4, "QR fiscal pendiente", border=0, align="C")
            return
        image_path = _create_qr_png(data)
        if image_path:
            self._register_temp_image(image_path)
            size = min(width, height)
            self.image(image_path, x=x + (width - size) / 2, y=y, w=size, h=size)
            self.set_xy(x, y + size + 2)
            self.set_font("helvetica", "", 7)
            _safe_multicell(self, width, 4, "QR fiscal", border=0, align="C")
        else:
            self.set_font("helvetica", "", 7)
            _safe_multicell(self, width, 4, data.strip()[:60], border=0, align="C")

    def _draw_client_block(self) -> None:
        name = self.entity.get("nombre_completo") or self.entity.get("razon_social") or "Consumidor Final"
        cuit = self.entity.get("cuit") or "-"
        addr = self.entity.get("domicilio") or self.doc.get("direccion_entrega") or "-"
        condition = self.entity.get("condicion_iva") or "-"
        delivery = self.doc.get("direccion_entrega")

        self.set_fill_color(15, 23, 42)
        self.set_text_color(255, 255, 255)
        self.set_font("helvetica", "B", 10)
        self.cell(0, 6, "Datos del Cliente", ln=1, fill=True)

        self.set_text_color(15, 23, 42)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, f"Razón social: {name}", ln=1)
        self.cell(0, 5, f"CUIT: {cuit}", ln=1)
        self.cell(0, 5, f"Domicilio: {addr}", ln=1)
        self.cell(0, 5, f"Condición IVA: {condition}", ln=1)
        if delivery:
            self.cell(0, 5, f"Entrega: {delivery}", ln=1)
        self.set_text_color(0, 0, 0)

    def _draw_items_table(self) -> None:
        headers = ["Descripción", "Cant.", "Precio", "IVA %", "Total"]
        start_x = self.l_margin
        table_width = max(self.w - self.l_margin - self.r_margin, 20)
        col_ratios = [0.5, 0.1, 0.16, 0.1, 0.14]
        col_widths = _distribute_width(table_width, col_ratios)
        total_width = sum(col_widths)
        if abs(total_width - table_width) > 0.01:
            col_widths[-1] = max(10.0, col_widths[-1] + (table_width - total_width))
        total_width = sum(col_widths)
        if total_width > table_width + 0.01:
            col_widths[-1] = max(10.0, col_widths[-1] - (total_width - table_width))
        def _draw_table_header() -> None:
            self.set_font("helvetica", "B", 10)
            self.set_fill_color(15, 23, 42)
            self.set_text_color(255, 255, 255)
            self.set_x(start_x)
            for width, title in zip(col_widths, headers):
                self.cell(width, 10, title, border=0, align="C", fill=True)
            self.ln()
            self.set_text_color(15, 23, 42)
            self.set_font("helvetica", "", 9)

        self._table_header_drawer = _draw_table_header
        self._table_header_last_page = 0
        self._table_header_active = True
        _draw_table_header()
        self._table_header_last_page = self.page_no()

        if not self.items:
            self.set_font("helvetica", "", 9)
            self.set_text_color(100)
            self.cell(table_width, 8, "No hay ítems para este comprobante.", border=1, align="C")
            return

        for idx, item in enumerate(self.items):
            qty = _safe_float(item.get("cantidad"), 0.0)
            unit = _safe_float(item.get("precio_unitario"), 0.0)
            iva_pct = _safe_float(item.get("porcentaje_iva"), 0.0)
            total = qty * unit
            desc = (item.get("articulo_nombre") or item.get("descripcion") or f"Artículo {item.get('id_articulo')}")
            shade = 245 if idx % 2 == 0 else 255
            self.set_fill_color(shade, shade, 255)
            self.set_draw_color(226, 232, 240)
            row_height = 8
            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            if self.get_y() + row_height > page_break_at:
                self.add_page()
            desc_width = col_widths[0]
            content_width = desc_width - (getattr(self, "c_margin", 0.5) * 2)
            desc_text = _truncate_text_to_width(self, desc, max(content_width, 0))
            self.cell(desc_width, row_height, desc_text, border="LR", fill=True)
            self.cell(col_widths[1], row_height, f"{qty:.2f}", border="LR", align="R", fill=True)
            self.cell(col_widths[2], row_height, _format_money(unit), border="LR", align="R", fill=True)
            self.cell(col_widths[3], row_height, f"{iva_pct:.2f}%", border="LR", align="R", fill=True)
            self.cell(col_widths[4], row_height, _format_money(total), border="LR", align="R", fill=True)
            self.ln()

        self._table_header_active = False
        self._table_header_drawer = None
        self.set_draw_color(15, 23, 42)
        self.set_line_width(0.5)
        self.line(start_x, self.get_y(), start_x + sum(col_widths), self.get_y())
        self.ln(1)

    def _draw_totals_block(self) -> None:
        self.set_font("helvetica", "", 10)
        self.cell(0, 6, f"Subtotal: {_format_money(self.neto)}", ln=1)
        if self._should_discriminate_iva():
            self.cell(0, 6, f"IVA total: {_format_money(self.iva_total)}", ln=1)
            if self.tax_summary:
                self.set_font("helvetica", "B", 10)
                self.cell(0, 6, "Detalle de IVA", ln=1)
                self.set_font("helvetica", "", 9)
                for pct, values in sorted(self.tax_summary.items(), key=lambda x: x[0]):
                    self.cell(0, 5, f"Base {pct:.2f}%: {_format_money(values['base'])}  |  IVA {_format_money(values['iva'])}", ln=1)
        self.set_font("helvetica", "B", 12)
        self.cell(0, 8, f"TOTAL: {_format_money(self.total)}", ln=1)

    def _draw_footer_remarks(self) -> None:
        return


class RemitoPDF(BaseDocumentPDF):
    def __init__(self, remito_data: Dict[str, Any], entity_data: Dict[str, Any], items_data: List[Dict[str, Any]]):
        super().__init__(remito_data, entity_data, items_data)
        self.remito = remito_data or {}

    def build(self) -> None:
        self._draw_remito_header()
        self._draw_entity_block()
        self.ln(4)
        self._draw_items_table()
        self.ln(4)
        self._draw_remito_footer()

    def _draw_remito_header(self) -> None:
        numero = self.remito.get("numero") or "-"
        fecha = self.remito.get("fecha") or datetime.now().strftime("%Y-%m-%d")
        estado = self.remito.get("estado") or "-"
        entrega = self.remito.get("fecha_entrega") or "-"
        documento = self.remito.get("documento_numero")

        self.ln(4)
        self.set_font("helvetica", "B", 14)
        self.cell(0, 8, f"Remito {numero}", ln=1)
        self.set_font("helvetica", "", 10)
        self.cell(0, 5, f"Fecha: {fecha}", ln=1)
        self.cell(0, 5, f"Estado: {estado}", ln=1)
        self.cell(0, 5, f"Entrega estimada: {entrega}", ln=1)
        valor_decl = self.remito.get("valor_declarado") or 0
        if valor_decl > 0:
            self.cell(0, 5, f"Valor declarado: ${valor_decl:,.2f}", ln=1)
        if documento:
            self.cell(0, 5, f"Documento asociado: {documento}", ln=1)

    def _draw_entity_block(self) -> None:
        name = self.entity.get("nombre_completo") or "Consumidor Final"
        cuit = self.entity.get("cuit") or "-"
        addr = self.entity.get("domicilio") or "-"

        self.set_fill_color(15, 23, 42)
        self.set_text_color(255, 255, 255)
        self.set_font("helvetica", "B", 10)
        self.cell(0, 6, "Datos del Cliente", ln=1, fill=True)

        self.set_text_color(15, 23, 42)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, f"Razón social: {name}", ln=1)
        self.cell(0, 5, f"CUIT: {cuit}", ln=1)
        self.cell(0, 5, f"Domicilio: {addr}", ln=1)
        self.set_text_color(0, 0, 0)

    def _draw_items_table(self) -> None:
        headers = ["Artículo", "Cantidad", "Observación"]
        start_x = self.l_margin
        content_width = max(self.w - self.l_margin - self.r_margin, 20)
        col_ratios = [0.55, 0.2, 0.25]
        widths = _distribute_width(content_width, col_ratios)
        total_width = sum(widths)
        if abs(total_width - content_width) > 0.01:
            widths[-1] = max(10.0, widths[-1] + (content_width - total_width))
        total_width = sum(widths)
        if total_width > content_width + 0.01:
            widths[-1] = max(10.0, widths[-1] - (total_width - content_width))
        def _draw_table_header() -> None:
            self.set_font("helvetica", "B", 10)
            self.set_fill_color(15, 23, 42)
            self.set_text_color(255, 255, 255)
            self.set_x(start_x)
            for w, title in zip(widths, headers):
                self.cell(w, 10, title, border=0, align="C", fill=True)
            self.ln()
            self.set_text_color(15, 23, 42)
            self.set_font("helvetica", "", 9)

        _draw_table_header()

        if not self.items:
            self.set_font("helvetica", "", 9)
            self.set_text_color(100)
            self.cell(sum(widths), 8, "No hay líneas disponibles para este remito.", border=1, align="C")
            return

        for idx, line in enumerate(self.items):
            shade = 245 if idx % 2 == 0 else 255
            self.set_fill_color(shade, shade, 255)
            desc = str(line.get("articulo") or line.get("observacion") or "-")
            cantidad = _safe_float(line.get("cantidad"), 0.0)
            observacion = line.get("observacion") or "-"
            row_height = 8
            page_break_at = getattr(self, "page_break_trigger", self.h - self.b_margin)
            if self.get_y() + row_height > page_break_at:
                self.add_page()
                self.ln(2)
                _draw_table_header()
            desc_text = _truncate_text_to_width(self, desc, widths[0] - (getattr(self, "c_margin", 0.5) * 2))
            obs_text = _truncate_text_to_width(self, observacion, widths[2] - (getattr(self, "c_margin", 0.5) * 2))
            self.cell(widths[0], row_height, desc_text, border="LR", fill=True)
            self.cell(widths[1], 8, f"{cantidad:.2f}", border="LR", align="R", fill=True)
            self.cell(widths[2], row_height, obs_text, border="LR", fill=True)
            self.ln()

        self.set_draw_color(15, 23, 42)
        self.set_line_width(0.4)
        self.line(start_x, self.get_y(), start_x + sum(widths), self.get_y())

    def _draw_remito_footer(self) -> None:
        direccion = self.remito.get("direccion_entrega") or "-"
        usuario = self.remito.get("usuario") or "-"

        self.set_font("helvetica", "B", 10)
        self.cell(0, 6, "Dirección de entrega", ln=1)
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, direccion, ln=1)
        self.ln(6)
        self.cell(0, 5, "Recibí conforme", ln=1)
        self.cell(0, 5, "Firma: ______________________________", ln=1)


def generate_pdf_and_open(
    doc_data: Dict[str, Any],
    entity_data: Dict[str, Any],
    items_data: List[Dict[str, Any]],
    *,
    kind: str = "invoice",
) -> str:
    if kind == "remito":
        pdf = RemitoPDF(doc_data, entity_data, items_data)
    else:
        pdf = InvoicePDF(doc_data, entity_data, items_data)

    path = pdf.generate()
    _open_pdf(path)
    return path
