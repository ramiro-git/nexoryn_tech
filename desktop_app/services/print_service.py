
from fpdf import FPDF
from datetime import datetime
import os
import tempfile

class InvoicePDF(FPDF):
    def __init__(self, doc_data, entity_data, items_data):
        super().__init__()
        self.doc = doc_data
        self.entity = entity_data
        self.items = items_data
    
    def header(self):
        # Logo placeholder
        # self.image('logo.png', 10, 8, 33)
        self.set_font('helvetica', 'B', 20)
        self.cell(0, 10, 'NEXORYN TECH', border=False, align='C', new_x="LMARGIN", new_y="NEXT")
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', align='C')

    def generate(self):
        self.add_page()
        
        # Document Info
        self.set_font('helvetica', 'B', 12)
        doc_type = "COMPROBANTE"  # Could be dynamic based on internal map if needed
        # We don't have the type name in doc_data locally usually, unless passed. 
        # But we can just use "Comprobante" or the series prefix.
        
        self.cell(0, 10, f"Comprobante: {self.doc.get('numero_serie', '---')}", new_x="LMARGIN", new_y="NEXT", align='R')
        self.set_font('helvetica', '', 10)
        self.cell(0, 6, f"Fecha: {self.doc.get('fecha', '')}", new_x="LMARGIN", new_y="NEXT", align='R')
        self.ln(10)

        # Client Info
        self.set_fill_color(240, 240, 240)
        self.set_font('helvetica', 'B', 10)
        self.cell(0, 8, "Datos del Cliente", fill=True, new_x="LMARGIN", new_y="NEXT")
        
        self.set_font('helvetica', '', 10)
        name = self.entity.get('nombre_completo') or self.entity.get('razon_social') or "Consumidor Final"
        cuit = self.entity.get('cuit') or "---"
        addr = self.entity.get('domicilio') or "---"
        
        self.cell(0, 6, f"Razón Social: {name}", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 6, f"CUIT: {cuit}", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 6, f"Dirección: {addr}", new_x="LMARGIN", new_y="NEXT")
        
        extra_addr = self.doc.get("direccion_entrega")
        if extra_addr:
             self.cell(0, 6, f"Dirección de Entrega: {extra_addr}", new_x="LMARGIN", new_y="NEXT")
        
        self.ln(10)

        # Items Table Header
        self.set_font('helvetica', 'B', 10)
        col_w = [100, 25, 30, 35] # Description, Qty, Unit, Total
        self.cell(col_w[0], 8, "Descripción", border=1, align='C')
        self.cell(col_w[1], 8, "Cant.", border=1, align='C')
        self.cell(col_w[2], 8, "Precio", border=1, align='C')
        self.cell(col_w[3], 8, "Total", border=1, align='C')
        self.ln()

        # Items Rows
        self.set_font('helvetica', '', 9)
        total_calc = 0
        for item in self.items:
            qty = float(item.get('cantidad', 0))
            price = float(item.get('precio_unitario', 0))
            sub = qty * price
            total_calc += sub
            
            # Truncate description if too long
            desc = item.get('articulo_nombre', f"Art {item.get('id_articulo')}")
            
            self.cell(col_w[0], 8, str(desc)[:50], border=1)
            self.cell(col_w[1], 8, f"{qty:.2f}", border=1, align='R')
            self.cell(col_w[2], 8, f"${price:,.2f}", border=1, align='R')
            self.cell(col_w[3], 8, f"${sub:,.2f}", border=1, align='R')
            self.ln()

        self.ln(5)

        # Totals
        self.set_font('helvetica', '', 10)
        
        # Pull totals from doc if available, else calc
        neto = self.doc.get('neto', total_calc)
        iva = self.doc.get('iva_total', 0)
        total = self.doc.get('total', neto + iva)

        x_start = 140
        self.set_x(x_start)
        self.cell(30, 8, "Subtotal:", align='R')
        self.cell(35, 8, f"${neto:,.2f}", border=1, align='R', new_x="LMARGIN", new_y="NEXT")
        
        self.set_x(x_start)
        self.cell(30, 8, "IVA:", align='R')
        self.cell(35, 8, f"${iva:,.2f}", border=1, align='R', new_x="LMARGIN", new_y="NEXT")
        
        self.set_font('helvetica', 'B', 11)
        self.set_x(x_start)
        self.cell(30, 10, "TOTAL:", align='R')
        self.cell(35, 10, f"${total:,.2f}", border=1, align='R', fill=False)

        # Output
        fd, path = tempfile.mkstemp(suffix=".pdf")
        os.close(fd)
        self.output(path)
        return path

def generate_pdf_and_open(doc_data, entity_data, items_data):
    pdf = InvoicePDF(doc_data, entity_data, items_data)
    path = pdf.generate()
    os.startfile(path)
    return path
