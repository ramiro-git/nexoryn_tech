from fpdf import FPDF
import datetime
import re
from pathlib import Path

class ManualPDF(FPDF):
    def header(self):
        self.set_font('helvetica', 'B', 15)
        self.set_text_color(99, 102, 241) # Indigo 500
        self.cell(0, 10, 'NEXORYN TECH - Manual Maestro de Usuario', border=False, ln=True, align='R')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(128)
        self.cell(0, 10, f'Página {self.page_no()} | Confidencial - Uso Interno | Generado: {datetime.datetime.now().strftime("%d/%m/%Y %H:%M")}', 0, 0, 'C')

    def chapter_title(self, label):
        self.set_font('helvetica', 'B', 16)
        self.set_text_color(30, 41, 59) # Slate 800
        self.ln(5)
        self.cell(0, 10, label, ln=True, align='L')
        self.ln(4)
        self.set_draw_color(226, 232, 240)
        self.line(self.get_x(), self.get_y(), self.get_x() + 190, self.get_y())
        self.ln(5)

    def section_title(self, label):
        self.set_font('helvetica', 'B', 12)
        self.set_text_color(79, 70, 229) # Indigo 600
        self.ln(3)
        self.cell(0, 8, label, ln=True, align='L')
        self.ln(2)

    def subsection_title(self, label):
        self.set_font('helvetica', 'B', 11)
        self.set_text_color(30, 41, 59) # Slate 800
        self.ln(2)
        self.cell(0, 7, label, ln=True, align='L')
        self.ln(1)

    def chapter_body(self, text):
        self.set_font('helvetica', '', 11)
        self.set_text_color(51, 65, 85) # Slate 700
        self.multi_cell(0, 7, text)
        self.ln()

    def add_shortcut(self, key, description):
        self.set_font('helvetica', 'B', 10)
        self.set_text_color(79, 70, 229)
        self.cell(45, 7, key, border=False)
        self.set_font('helvetica', '', 10)
        self.set_text_color(51, 65, 85)
        self.cell(0, 7, f' - {description}', border=False, ln=True)

    def bullet_item(self, text):
        self.set_font('helvetica', '', 11)
        self.set_text_color(51, 65, 85)
        self.multi_cell(0, 6, f"- {text}")
        self.ln(1)

    def quote_line(self, text):
        self.set_font('helvetica', 'I', 10)
        self.set_text_color(100, 116, 139)
        self.multi_cell(0, 6, f"> {text}")
        self.ln(1)

    def code_block(self, text):
        self.set_font('courier', '', 9)
        self.set_text_color(30, 41, 59)
        self.set_fill_color(241, 245, 249)
        self.set_draw_color(226, 232, 240)
        self.multi_cell(0, 5, text, border=1, fill=True)
        self.ln(2)

    def error_box(self, title, text):
        self.set_fill_color(254, 242, 242) # Red 50
        self.set_draw_color(239, 68, 68)  # Red 500
        self.set_text_color(153, 27, 27) # Red 800
        self.set_font('helvetica', 'B', 11)
        self.cell(0, 8, f" ALERTA: {title}", border='TLR', ln=True, fill=True)
        self.set_font('helvetica', '', 10)
        self.multi_cell(0, 6, text, border='BLR', fill=True)
        self.ln(5)

def sanitize_text(text):
    if text is None:
        return ""
    replacements = {
        "\u201c": '"',
        "\u201d": '"',
        "\u2018": "'",
        "\u2019": "'",
        "\u2013": "-",
        "\u2014": "-",
        "\u2026": "...",
        "\u00a0": " ",
        "\u2022": "-",
    }
    for src, repl in replacements.items():
        text = text.replace(src, repl)
    text = text.replace("\t", "    ")
    try:
        text.encode("latin-1")
    except UnicodeEncodeError:
        text = text.encode("latin-1", "replace").decode("latin-1")
    return text

def render_markdown(pdf, text):
    lines = text.splitlines()
    in_code = False
    paragraph = []
    code_lines = []

    def flush_paragraph():
        if paragraph:
            pdf.chapter_body(sanitize_text(" ".join(paragraph).strip()))
            paragraph.clear()

    def flush_code():
        nonlocal code_lines
        if code_lines:
            pdf.code_block(sanitize_text("\n".join(code_lines)))
            code_lines = []

    for line in lines:
        raw = line.rstrip("\n")
        stripped = raw.strip()

        if stripped.startswith("```"):
            if in_code:
                flush_code()
                in_code = False
            else:
                flush_paragraph()
                in_code = True
            continue

        if in_code:
            code_lines.append(raw)
            continue

        if not stripped:
            flush_paragraph()
            continue

        if stripped.startswith("# "):
            flush_paragraph()
            pdf.chapter_title(sanitize_text(stripped[2:].strip()))
            continue
        if stripped.startswith("## "):
            flush_paragraph()
            pdf.section_title(sanitize_text(stripped[3:].strip()))
            continue
        if stripped.startswith("### "):
            flush_paragraph()
            pdf.subsection_title(sanitize_text(stripped[4:].strip()))
            continue
        if stripped.startswith(">"):
            flush_paragraph()
            quote = stripped.lstrip(">").strip()
            pdf.quote_line(sanitize_text(quote))
            continue

        list_match = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", raw)
        if list_match:
            flush_paragraph()
            item_text = list_match.group(3).strip()
            pdf.bullet_item(sanitize_text(item_text))
            continue

        paragraph.append(stripped)

    if in_code:
        flush_code()
    flush_paragraph()

def generate_manual():
    pdf = ManualPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # --- PORTADA ---
    pdf.set_y(60)
    pdf.set_font('helvetica', 'B', 45)
    pdf.set_text_color(99, 102, 241)
    pdf.cell(0, 20, 'Manual Maestro', ln=True, align='C')
    pdf.set_font('helvetica', 'B', 28)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(0, 15, 'Sistema Nexoryn Tech', ln=True, align='C')
    pdf.ln(10)
    pdf.set_font('helvetica', '', 16)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(0, 10, 'Guía de Operación, Fallos y Resolución de Problemas', ln=True, align='C')
    
    pdf.set_y(220)
    pdf.set_font('helvetica', 'B', 12)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(0, 10, 'Sistema de Gestión Integral', ln=True, align='C')
    pdf.set_font('helvetica', '', 10)
    pdf.cell(0, 10, 'Este manual contiene detalles críticos sobre AFIP, Stock y Cuentas.', ln=True, align='C')

    pdf.add_page()

    # --- INDICE ---
    pdf.chapter_title('Contenido del Manual')
    pdf.chapter_body(
        "1. Conceptos Fundamentales\n"
        "2. Navegación y Atajos de Eficiencia\n"
        "3. Gestión de Inventario y Stock Crítico\n"
        "4. Entidades y Cuentas Corrientes\n"
        "5. Facturación Electrónica (AFIP/ARCA)\n"
        "6. Caja y Movimientos Financieros\n"
        "7. Resolución de Problemas y Qué hacer si algo falla\n"
        "8. Apéndice técnico (Documentación)"
    )

    # --- CAPITULO 1: CONCEPTOS ---
    pdf.chapter_title('1. Conceptos Fundamentales')
    pdf.chapter_body(
        "Nexoryn Tech es un sistema de gestión en tiempo real. Esto significa que cada acción (vender, cobrar, mover mercadería) "
        "impacta de forma inmediata en los saldos y las estadísticas del tablero de control.\n\n"
        "El sistema separa las operaciones en 'Comprobantes' (documentos legales o internos) y 'Movimientos' (el flujo físico o monetario)."
    )

    # --- CAPITULO 2: NAVEGACION ---
    pdf.chapter_title('2. Navegación y Atajos de Eficiencia')
    pdf.section_title('Uso de Tablas Inteligentes')
    pdf.chapter_body(
        "Las tablas son el núcleo del sistema. Permiten editar datos directamente (vistas rápidas) o entrar al detalle completo."
    )
    pdf.add_shortcut('Rueda Mouse', 'Scroll vertical lento/rápido.')
    pdf.add_shortcut('Shift + Rueda', 'Scroll horizontal (vital para tablas con muchas columnas).')
    pdf.add_shortcut('Doble Clic en Celda', 'Si la columna es editable, abre el editor instantáneo.')
    pdf.add_shortcut('Ctrl + F', 'Foco rápido en el buscador global de la tabla.')
    pdf.add_shortcut('F5', 'Refresca los datos de la tabla (útil en red local).')
    
    pdf.section_title('Filtros Avanzados')
    pdf.chapter_body(
        "No use solo el buscador global. Use los 'Filtros Avanzados' para buscar por 'Stock Bajo', 'Fecha de Alta' o 'Deuda de Cliente'. "
        "Esto reduce la carga del sistema y le da resultados exactos."
    )
    pdf.section_title('Seguridad por Inactividad')
    pdf.chapter_body(
        "Si no hay actividad durante 5 minutos, el sistema cierra la sesión automáticamente para proteger los datos."
    )

    # --- CAPITULO 3: INVENTARIO ---
    pdf.chapter_title('3. Gestión de Inventario y Stock Crítico')
    pdf.chapter_body(
        "Cada artículo tiene un 'Stock Mínimo'. Cuando el stock actual es igual o menor a este valor, el sistema marcará el producto "
        "con una alerta visual en el inventario y en el tablero de control."
    )
    pdf.error_box(
        "Diferencia de Stock",
        "Si el stock físico no coincide con el del sistema, NO edite el número directamente en la ficha del producto. "
        "Vaya a 'Movimientos' y realice un 'AJUSTE DE STOCK' con el motivo correspondiente para que quede registro de quién y por qué se cambió."
    )

    # --- CAPITULO 4: ENTIDADES ---
    pdf.chapter_title('4. Entidades y Cuentas Corrientes')
    pdf.chapter_body(
        "El saldo de un cliente se compone de: Facturas (+) y Pagos (-).\n\n"
        "- Saldo Positivo: El cliente le debe dinero.\n"
        "- Saldo Negativo: El cliente tiene saldo a favor (le pagó de más o hubo una devolución).\n\n"
        "Importante: Para que un pago impacte en el saldo, debe estar asociado a la Entidad Comercial correcta."
    )

    # --- CAPITULO 5: AFIP ---
    pdf.add_page()
    pdf.chapter_title('5. Facturación Electrónica (AFIP/ARCA)')
    pdf.chapter_body(
        "El sistema se comunica con AFIP en tres pasos silenciosos:\n"
        "1. Solicita un 'LoginTicket' (Token) usando sus certificados digitales.\n"
        "2. Envía los datos del comprobante.\n"
        "3. Recibe el CAE y la fecha de vencimiento."
    )
    pdf.section_title('Condiciones para el ÉXITO de la factura:')
    pdf.chapter_body(
        "- El comprobante debe estar confirmado y sin CAE antes de autorizar.\n"
        "- Para comprobantes letra A se requiere CUIT válido (11 dígitos) y condición IVA del receptor.\n"
        "- Para letra B/C se admite CUIT (11 dígitos) o DNI (hasta 8 dígitos).\n"
        "- Los certificados (.crt y .key) deben estar vigentes (vencen anualmente).\n"
        "- El Punto de Venta debe ser tipo 'Web Services' (RECE).\n"
        "- AFIP funciona en el .exe sin Bash, pero requiere OpenSSL accesible."
    )
    pdf.error_box(
        "Error de Conexión AFIP",
        "Si AFIP no responde, el comprobante quedará sin CAE. NO intente facturar de nuevo el mismo "
        "documento sin verificar antes en 'Comprobantes' si ya obtuvo CAE. Si el error persiste, verifique su conexión a internet "
        "o si la página de AFIP está caída (es común en horarios pico)."
    )

    # --- CAPITULO 6: CAJA ---
    pdf.chapter_title('6. Caja y Movimientos Financieros')
    pdf.chapter_body(
        "La caja refleja el dinero disponible. Cada usuario tiene asignada una caja o puede usar una caja central.\n\n"
        "Recuerde realizar el 'CIERRE DE CAJA' al final del día para comparar el efectivo real con lo que el sistema indica."
    )

    # --- CAPITULO 7: RESOLUCIÓN DE FALLOS ---
    pdf.add_page()
    pdf.chapter_title('7. Resolución de Problemas y Errores Comunes')
    
    pdf.section_title('Problema A: El sistema no inicia o dice "Error de Base de Datos"')
    pdf.chapter_body(
        "- Causa: El servicio de PostgreSQL está detenido o el firewall bloquea la conexión.\n"
        "- Solución: Reinicie la PC o verifique que el servicio de Postgres esté 'En Ejecución' en el administrador de tareas."
    )

    pdf.section_title('Problema B: "No se encuentra el archivo .env" o credenciales')
    pdf.chapter_body(
        "- Causa: Se borró el archivo de configuración o está en una ubicación no soportada.\n"
        "- Solución: Verifique que exista un `.env` en %APPDATA%\\Nexoryn_Tech\\ o junto al ejecutable, y que tenga las credenciales correctas."
    )

    pdf.section_title('Problema C: "openssl no encontrado" al facturar AFIP')
    pdf.chapter_body(
        "- Causa: OpenSSL no está instalado o no está accesible desde la app.\n"
        "- Solución: Instale OpenSSL o copie `openssl.exe` junto con `libcrypto-*.dll` y `libssl-*.dll` al directorio del ejecutable."
    )

    pdf.section_title('Problema D: El PDF no se genera o no se abre')
    pdf.chapter_body(
        "- Causa: Un antivirus bloquea el acceso a la carpeta temporal o no tiene un lector de PDF instalado.\n"
        "- Solución: Verifique que puede abrir otros archivos PDF en su computadora."
    )

    pdf.error_box(
        "CORRUPCIÓN DE DATOS / CORTE DE LUZ",
        "Si hubo un corte de luz mientras el sistema estaba guardando, es posible que el último registro falle. "
        "El sistema está protegido por 'transacciones' para evitar pérdida total, pero siempre verifique el último "
        "comprobante emitido tras un reinicio forzado."
    )

    # --- RESUMEN DE ATAJOS ---
    pdf.ln(10)
    pdf.chapter_title('Resumen Final de Atajos')
    pdf.add_shortcut('Enter', 'Acepta el formulario o busca en la tabla.')
    pdf.add_shortcut('Esc', 'Cancela la acción o cierra el modal actual.')
    pdf.add_shortcut('Ctrl + S', 'En algunas pantallas, guarda el borrador.')
    pdf.add_shortcut('Tab / Shift+Tab', 'Navega rápidamente entre campos de texto.')
    pdf.add_shortcut('Alt + [Letra]', 'Navega entre las pestañas del menú lateral.')

    pdf.ln(15)
    pdf.set_font('helvetica', 'I', 10)
    pdf.set_text_color(100, 116, 139)
    pdf.multi_cell(0, 10, 'Este manual es una herramienta dinámica. Si detecta un error no documentado, reporte al equipo técnico para su inclusión.', align='C')

    # --- APENDICE TECNICO ---
    pdf.add_page()
    pdf.chapter_title('8. Apéndice técnico')
    pdf.chapter_body(
        "Este apéndice incluye la documentación técnica actual del proyecto. "
        "Si se actualizan los documentos en /docs, vuelva a generar el PDF para reflejar los cambios."
    )

    appendix_docs = [
        Path("docs/REQUISITOS_INSTALACION.md"),
        Path("docs/GUIA_EMPAQUETADO.md"),
        Path("docs/GUIA_RED_LOCAL.md"),
        Path("docs/AFIP_ARCA.md"),
        Path("docs/GUIA_AFIP_PORTAL.md"),
        Path("docs/BACKUP_SYSTEM.md"),
        Path("docs/DATABASE.md"),
    ]

    for doc_path in appendix_docs:
        pdf.add_page()
        if not doc_path.exists():
            pdf.section_title(f"Documento no encontrado: {doc_path}")
            pdf.chapter_body("No se pudo cargar este documento en el apéndice.")
            continue
        content = doc_path.read_text(encoding="utf-8")
        render_markdown(pdf, content)

    output_path = "MANUAL_MAESTRO_NEXORYN_TECH.pdf"
    pdf.output(output_path)
    print(f"Manual Maestro generado con éxito en: {output_path}")

if __name__ == "__main__":
    generate_manual()
