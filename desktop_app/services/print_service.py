
from fpdf import FPDF
from datetime import datetime
import os
import tempfile
from typing import Any, Dict, List


def _format_money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except Exception:
        if value in (None, ""):
            return "—"
        return str(value)


class InvoicePDF(FPDF):
    def __init__(self, doc_data: Dict[str, Any], entity_data: Dict[str, Any], items_data: List[Dict[str, Any]]):
        super().__init__()
        self.doc = doc_data
        self.entity = entity_data
        self.items = items_data

    def header(self):
        self.set_fill_color(15, 23, 42)
        self.set_text_color(255, 255, 255)
        self.set_font("helvetica", "B", 18)
        self.rect(0, 0, 210, 28, "F")
        self.set_xy(12, 8)
        self.cell(0, 10, "NEXORYN TECH", border=0, ln=1, align="L")
        self.set_font("helvetica", "", 9)
        self.cell(0, 5, "Soluciones tecnológicas y logísticas", border=0, ln=1, align="L")
        self.ln(5)
        self.set_text_color(0, 0, 0)

    def footer(self):
        self.set_y(-18)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(100)
        self.cell(0, 10, f"Página {self.page_no()}/{{nb}}", 0, 0, "C")

    def generate(self) -> str:
        self.alias_nb_pages()
        self.add_page()
        self.set_auto_page_break(True, 25)

        self._draw_document_header()
        self._draw_client_block()
        self.ln(2)
        self._draw_items_table()
        self.ln(5)
        self._draw_totals_block()
        self._draw_footer_remarks()

        fd, path = tempfile.mkstemp(suffix=".pdf")
        os.close(fd)
        self.output(path)
        return path

    def _draw_document_header(self) -> None:
        doc_type = self.doc.get("tipo_documento") or "COMPROBANTE"
        series = self.doc.get("serie", "")
        number = self.doc.get("numero_serie", "---")
        cae = self.doc.get("cae") or "Pendiente"
        date = self.doc.get("fecha", datetime.now().strftime("%Y-%m-%d"))

        self.set_draw_color(226, 232, 240)
        self.set_fill_color(247, 250, 255)
        self.set_line_width(0.35)
        self.rect(10, 38, 190, 30, "FD")

        self.set_font("helvetica", "B", 11)
        self.set_text_color(15, 23, 42)
        self.set_xy(12, 44)
        self.cell(0, 6, f"{doc_type} {series}".strip(), ln=1)

        self.set_font("helvetica", "", 10)
        self.cell(0, 5, f"Número: {number}", ln=1)
        self.cell(0, 5, f"Fecha emisión: {date}", ln=1)
        self.cell(0, 5, f"CAE: {cae}", ln=1)

    def _draw_client_block(self) -> None:
        name = self.entity.get("nombre_completo") or self.entity.get("razon_social") or "Consumidor Final"
        cuit = self.entity.get("cuit") or "---"
        addr = self.entity.get("domicilio") or "---"
        delivery = self.doc.get("direccion_entrega")

        self.set_fill_color(15, 23, 42)
        self.set_text_color(255, 255, 255)
        self.set_font("helvetica", "B", 10)
        self.rect(10, 70, 190, 8, "F")
        self.set_xy(12, 72)
        self.cell(0, 5, "Datos del Cliente", ln=1)

        self.set_fill_color(255, 255, 255)
        self.set_text_color(15, 23, 42)
        self.set_font("helvetica", "", 9)
        self.set_xy(12, 78)
        self.cell(0, 5, f"Razón social: {name}", ln=1)
        self.cell(0, 5, f"CUIT: {cuit}", ln=1)
        self.cell(0, 5, f"Dirección: {addr}", ln=1)
        if delivery:
            self.cell(0, 5, f"Entrega: {delivery}", ln=1)

    def _draw_items_table(self) -> None:
        self.set_font("helvetica", "B", 10)
        self.set_fill_color(15, 23, 42)
        self.set_text_color(255, 255, 255)
        col_w = [95, 25, 30, 38]
        headers = ["Descripción", "Cant.", "Precio", "Total"]
        for w, title in zip(col_w, headers):
            self.cell(w, 10, title, border=0, align="C", fill=True)
        self.ln()

        self.set_font("helvetica", "", 9)
        self.set_text_color(15, 23, 42)
        total_calc = 0.0
        for idx, item in enumerate(self.items):
            qty = float(item.get("cantidad", 0) or 0)
            unit = float(item.get("precio_unitario", 0) or 0)
            line_total = qty * unit
            total_calc += line_total

            shade = 245 if idx % 2 == 0 else 255
            self.set_fill_color(shade, shade, 255)
            self.set_draw_color(226, 232, 240)
            desc = item.get("articulo_nombre") or item.get("descripcion", f"Artículo {item.get('id_articulo')}")
            self.cell(col_w[0], 8, str(desc)[:50], border="LR", fill=True)
            self.cell(col_w[1], 8, f"{qty:.2f}", border="LR", align="R", fill=True)
            self.cell(col_w[2], 8, _format_money(unit), border="LR", align="R", fill=True)
            self.cell(col_w[3], 8, _format_money(line_total), border="LR", align="R", fill=True)
            self.ln()

        self.set_draw_color(15, 23, 42)
        self.set_line_width(0.5)
        self.line(10, self.get_y(), 200, self.get_y())

        self.set_font("helvetica", "B", 10)
        self.cell(sum(col_w[:3]), 6, "Total Items", border=0, align="R")
        self.cell(col_w[3], 6, _format_money(total_calc), border=1, align="R", fill=True)
        self.ln(12)

    def _draw_totals_block(self) -> None:
        neto = self.doc.get("neto")
        iva = self.doc.get("iva_total")
        total = self.doc.get("total")
        if neto is None or iva is None:
            neto = sum(float(item.get("cantidad", 0) or 0) * float(item.get("precio_unitario", 0) or 0) for item in self.items)
            iva = float(self.doc.get("iva_total", 0) or 0)
            total = float(self.doc.get("total", neto + iva) or (neto + iva))

        y = self.get_y()
        self.set_fill_color(247, 250, 255)
        self.rect(110, y, 90, 35, "F")
        self.set_xy(112, y + 2)
        self.set_font("helvetica", "", 10)
        self.set_text_color(15, 23, 42)
        self.cell(40, 6, "Subtotal", border=0, align="L")
        self.cell(40, 6, _format_money(neto), border=0, align="R", ln=1)
        self.cell(40, 6, "IVA", border=0, align="L")
        self.cell(40, 6, _format_money(iva), border=0, align="R", ln=1)
        self.set_font("helvetica", "B", 11)
        self.cell(40, 8, "TOTAL", border=0, align="L")
        self.cell(40, 8, _format_money(total), border=0, align="R", ln=1)

    def _draw_footer_remarks(self) -> None:
        y = self.get_y() + 10
        self.set_xy(10, y)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(120)
        self.multi_cell(0, 5, "Recibimos tu consulta. Pronto estaremos emitiendo el CAE oficial. Este comprobante funciona como presupuesto y tiene validez comercial conforme la normativa vigente.")


def generate_pdf_and_open(doc_data: Dict[str, Any], entity_data: Dict[str, Any], items_data: List[Dict[str, Any]]) -> str:
    pdf = InvoicePDF(doc_data, entity_data, items_data)
    path = pdf.generate()
    os.startfile(path)
    return path
