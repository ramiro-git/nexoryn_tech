from fpdf import FPDF
from fpdf.enums import XPos, YPos
import datetime
import re
from pathlib import Path

class ManualPDF(FPDF):
    def __init__(self, generated_at=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generated_at = generated_at or datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    def header(self):
        self.set_font('helvetica', 'B', 15)
        self.set_text_color(99, 102, 241) # Indigo 500
        self.cell(
            0, 10, sanitize_text('NEXORYN TECH - Manual Maestro'),
            border=False, align='R',
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(128)
        self.cell(
            0, 10, sanitize_text(f'Página {self.page_no()} | Generado: {self.generated_at}'),
            border=0, align='C',
            new_x=XPos.RIGHT, new_y=YPos.TOP,
        )

    def chapter_title(self, label):
        self.set_font('helvetica', 'B', 16)
        self.set_text_color(30, 41, 59) # Slate 800
        self.ln(5)
        self.cell(
            0, 10, sanitize_text(label), align='L',
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )
        self.ln(4)
        self.set_draw_color(226, 232, 240)
        self.line(self.get_x(), self.get_y(), self.get_x() + 190, self.get_y())
        self.ln(5)

    def section_title(self, label):
        self.set_font('helvetica', 'B', 12)
        self.set_text_color(79, 70, 229) # Indigo 600
        self.ln(3)
        self.cell(
            0, 8, sanitize_text(label), align='L',
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )
        self.ln(2)

    def subsection_title(self, label):
        self.set_font('helvetica', 'B', 11)
        self.set_text_color(30, 41, 59) # Slate 800
        self.ln(2)
        self.cell(
            0, 7, sanitize_text(label), align='L',
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )
        self.ln(1)

    def chapter_body(self, text):
        self.set_font('helvetica', '', 11)
        self.set_text_color(51, 65, 85) # Slate 700
        self.multi_cell(0, 7, sanitize_text(text))
        self.ln()

    def add_shortcut(self, key, description):
        self.set_font('helvetica', 'B', 10)
        self.set_text_color(79, 70, 229)
        self.cell(
            45, 7, sanitize_text(key), border=False,
            new_x=XPos.RIGHT, new_y=YPos.TOP,
        )
        self.set_font('helvetica', '', 10)
        self.set_text_color(51, 65, 85)
        self.cell(
            0, 7, sanitize_text(f' - {description}'), border=False,
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )

    def bullet_item(self, text):
        self.set_font('helvetica', '', 11)
        self.set_text_color(51, 65, 85)
        self.multi_cell(0, 6, sanitize_text(f"- {text}"))
        self.ln(1)

    def quote_line(self, text):
        self.set_font('helvetica', 'I', 10)
        self.set_text_color(100, 116, 139)
        self.multi_cell(0, 6, sanitize_text(f"> {text}"))
        self.ln(1)

    def code_block(self, text):
        self.set_font('courier', '', 9)
        self.set_text_color(30, 41, 59)
        self.set_fill_color(241, 245, 249)
        self.set_draw_color(226, 232, 240)
        self.multi_cell(0, 5, sanitize_text(text), border=1, fill=True)
        self.ln(2)

    def error_box(self, title, text):
        self.set_fill_color(254, 242, 242) # Red 50
        self.set_draw_color(239, 68, 68)  # Red 500
        self.set_text_color(153, 27, 27) # Red 800
        self.set_font('helvetica', 'B', 11)
        self.cell(
            0, 8, sanitize_text(f" ALERTA: {title}"), border='TLR', fill=True,
            new_x=XPos.LMARGIN, new_y=YPos.NEXT,
        )
        self.set_font('helvetica', '', 10)
        self.multi_cell(0, 6, sanitize_text(text), border='BLR', fill=True)
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
    generated_at = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    pdf = ManualPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()

    # --- PORTADA ---
    pdf.set_y(60)
    pdf.set_font('helvetica', 'B', 45)
    pdf.set_text_color(99, 102, 241)
    pdf.cell(
        0, 20, sanitize_text('Manual Maestro'), align='C',
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )
    pdf.set_font('helvetica', 'B', 28)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(
        0, 15, sanitize_text('Sistema Nexoryn Tech'), align='C',
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )
    pdf.ln(10)
    pdf.set_font('helvetica', '', 16)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(
        0, 10, sanitize_text('Guía de Operación y Resolución de Problemas'), align='C',
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )
    
    pdf.set_y(220)
    pdf.set_font('helvetica', 'B', 12)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(
        0, 10, sanitize_text('Sistema de Gestión Integral'), align='C',
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )
    pdf.set_font('helvetica', '', 10)
    pdf.cell(
        0, 10,
        sanitize_text('Este manual le guiará en el uso del sistema Nexoryn Tech, que gestiona las áreas de AFIP, Inventario y Cuentas.'),
        align='C',
        new_x=XPos.LMARGIN, new_y=YPos.NEXT,
    )

    pdf.add_page()

    # --- INDICE ---
    pdf.chapter_title('Contenido del Manual')
    pdf.chapter_body(
        "1. Conceptos Básicos\n"
        "2. Navegación y Atajos\n"
        "3. Tablero de Control\n"
        "4. Entidades\n"
        "5. Inventario (Artículos)\n"
        "6. Stock\n"
        "7. Comprobantes y Facturación\n"
        "8. Facturación Electrónica (AFIP/ARCA)\n"
        "9. Remitos\n"
        "10. Movimientos de Stock\n"
        "11. Caja y Pagos\n"
        "12. Cuentas Corrientes\n"
        "13. Lista de Precios\n"
        "14. Actualización Masiva\n"
        "15. Resolución de Problemas y ¿Qué Hacer si Algo Falla?\n"
        "16. Resumen Final de Atajos"
    )

    # --- CAPITULO 1: CONCEPTOS ---
    pdf.chapter_title('1. Conceptos Básicos')
    pdf.chapter_body(
        "Nexoryn Tech es un sistema de gestión en tiempo real. Esto significa que cada acción (vender, cobrar o mover mercadería) "
        "actualiza inmediatamente los saldos y las estadísticas del panel de control.\n\n"
        "El sistema organiza las operaciones en dos categorías:\n"
        "- Comprobantes: documentos legales o internos que registran una transacción.\n"
        "- Movimientos: el flujo físico o monetario real de la transacción."
    )

    # --- CAPITULO 2: NAVEGACION ---
    pdf.chapter_title('2. Navegación y Atajos')
    pdf.section_title('Búsqueda y filtros')
    pdf.chapter_body(
        "Cada vista principal se apoya en tablas. Use el buscador global para localizar registros por texto y los filtros avanzados "
        "para acotar por estado, fechas, montos o categorías. Esto acelera la búsqueda y evita resultados mezclados."
    )
    pdf.section_title('Orden y acciones en tablas')
    pdf.chapter_body(
        "Puede ordenar por columnas haciendo clic en el título. Los botones más comunes son: "
        "'Actualizar' para recargar datos, 'Reiniciar filtros' para volver a los valores iniciales y "
        "'Exportar datos' para descargar el listado cuando esté disponible."
    )
    pdf.section_title('Atajos útiles')
    pdf.add_shortcut('Rueda del Ratón', 'Scroll vertical lento/rápido.')
    pdf.add_shortcut('Shift + Rueda', 'Scroll horizontal (ideal para tablas con muchas columnas).')
    pdf.add_shortcut('Doble clic en celda', 'Abre el editor instantáneo si la columna es editable.')
    pdf.add_shortcut('Ctrl + F', 'Busca rápidamente dentro de la tabla.')
    pdf.add_shortcut('Enter', 'En comprobantes, avanza por campos y ejecuta el botón enfocado.')
    pdf.add_shortcut('Esc', 'Cancela la acción o cierra el modal actual.')
    pdf.add_shortcut('Ctrl + S', 'En algunas pantallas, guarda el borrador.')
    pdf.add_shortcut('Tab / Shift+Tab', 'En comprobantes, recorre campos, selectores y botones del modal.')
    pdf.add_shortcut('Alt + [Letra]', 'Navega entre las pestañas del menú lateral.')

    pdf.section_title('Seguridad por inactividad')
    pdf.chapter_body(
        "Si no hay actividad durante 5 minutos, el sistema cierra la sesión automáticamente para proteger los datos."
    )

    pdf.add_page()

    # --- CAPITULO 3: TABLERO ---
    pdf.chapter_title('3. Tablero de Control')
    pdf.chapter_body(
        "El Tablero de Control reúne los indicadores principales del negocio: ventas del período, alertas de stock, "
        "entidades activas, operaciones recientes y movimientos financieros.\n\n"
        "Los datos se actualizan de forma automática y también puede forzar una actualización con el botón "
        "'Actualizar ahora'.\n\n"
        "Use el selector de período (Hoy/Semana/Mes/Año) para cambiar el rango de análisis y el selector de intervalo "
        "(30 seg, 1 min, 5 min, 10 min, desactivado) para ajustar la frecuencia de actualización."
    )

    # --- CAPITULO 4: ENTIDADES ---
    pdf.chapter_title('4. Entidades')
    pdf.section_title('Qué es una entidad')
    pdf.chapter_body(
        "Las entidades representan clientes y proveedores. Se usan en comprobantes, pagos y cuentas corrientes."
    )
    pdf.section_title('Alta rápida y edición')
    pdf.chapter_body(
        "Para crear una entidad complete nombre, CUIT/DNI, condición IVA y datos de contacto. "
        "Puede editar campos directamente en la tabla o abrir el detalle completo para ver más información."
    )
    pdf.section_title('Notas y estado')
    pdf.chapter_body(
        "Use el campo de notas para observaciones. El estado activo/inactivo permite mantener histórico sin eliminar registros."
    )
    pdf.section_title('Filtros útiles')
    pdf.chapter_body(
        "Filtre por tipo, CUIT, fecha de alta, localidad o saldo para encontrar rápidamente una entidad."
    )

    # --- CAPITULO 5: INVENTARIO ---
    pdf.chapter_title('5. Inventario (Artículos)')
    pdf.section_title('Alta de artículos')
    pdf.chapter_body(
        "Registre nombre, marca, rubro, unidad, IVA y precio base. Datos completos reducen errores en ventas y movimientos."
    )
    pdf.section_title('Edición rápida')
    pdf.chapter_body(
        "Las columnas editables permiten actualizar datos sin salir de la lista. Use doble clic en la celda para editar."
    )
    pdf.section_title('Stock mínimo y alertas')
    pdf.chapter_body(
        "Defina un stock mínimo por artículo. Cuando el stock llega al mínimo, el sistema muestra alertas en Inventario "
        "y en el panel de control."
    )
    pdf.section_title('Precios por lista')
    pdf.chapter_body(
        "Si trabaja con listas de precios, puede asignar valores por lista y ajustar porcentajes según corresponda."
    )
    pdf.error_box(
        "Diferencia de Stock",
        "Si el stock físico no coincide con el del sistema, NO edite el número directamente en la ficha del producto. "
        "En su lugar, vaya a 'Movimientos' y realice un 'Ajuste de Stock' con el motivo correspondiente para registrar el cambio."
    )
    pdf.section_title('Exportación')
    pdf.chapter_body(
        "Cuando esté disponible, use 'Exportar datos' para descargar el listado filtrado."
    )

    # --- CAPITULO 6: STOCK ---
    pdf.chapter_title('6. Stock')
    pdf.chapter_body(
        "La vista de Stock muestra alertas de artículos por debajo del mínimo. Úsela para priorizar reposición "
        "y evitar quiebres.\n\n"
        "El botón 'Actualizar' recarga la lista de alertas."
    )

    pdf.add_page()

    # --- CAPITULO 7: COMPROBANTES ---
    pdf.chapter_title('7. Comprobantes y Facturación')
    pdf.chapter_body(
        "Desde aquí se crean facturas, presupuestos y compras. El flujo típico es: crear, guardar borrador y confirmar.\n\n"
        "Acciones habituales: ver detalle, copiar como nuevo e imprimir.\n\n"
        "Estados comunes: Borrador (editable), Confirmado (impacta stock y cuentas) y Anulado (reversa). "
        "Si necesita rehacer un comprobante, primero anule el anterior si corresponde."
    )

    # --- CAPITULO 8: AFIP ---
    pdf.chapter_title('8. Facturación Electrónica (AFIP/ARCA)')
    pdf.chapter_body(
        "El sistema se comunica automáticamente con AFIP en tres pasos:\n"
        "1. Solicita un 'LoginTicket' (Token) usando sus certificados digitales.\n"
        "2. Envía los datos del comprobante.\n"
        "3. Recibe el CAE y la fecha de vencimiento."
    )
    pdf.section_title('Condiciones para el éxito de la factura')
    pdf.chapter_body(
        "- El comprobante debe estar confirmado y sin CAE antes de autorizar.\n"
        "- Para comprobantes letra A se requiere CUIT válido (11 dígitos) y condición IVA del receptor.\n"
        "- Para comprobantes letra B/C, se admite CUIT (11 dígitos) o DNI (hasta 8 dígitos).\n"
        "- Los certificados (.crt y .key) deben estar vigentes.\n"
        "- El Punto de Venta debe ser tipo 'Web Services' (RECE).\n"
        "- AFIP funciona usando el ejecutable, pero requiere OpenSSL accesible."
    )
    pdf.error_box(
        "Error de Conexión AFIP",
        "Si AFIP no responde, el comprobante quedará sin CAE. No intente facturar de nuevo el mismo "
        "documento sin verificar antes en 'Comprobantes' si ya obtuvo CAE. Si el error persiste, verifique su conexión a internet "
        "o si la página de AFIP está caída."
    )
    pdf.section_title('Trámites en el Portal (Resumen)')
    pdf.chapter_body(
        "1. Ingrese a AFIP con CUIT y clave fiscal.\n"
        "2. Habilite los servicios de Certificados Digitales y Puntos de Venta.\n"
        "3. Cree un Punto de Venta tipo 'Web Services'.\n"
        "4. Suba el certificado y vincule el alias al servicio WSFEv1.\n\n"
        "Para el paso a paso detallado, solicite la guía completa al soporte."
    )

    # --- CAPITULO 9: REMITOS ---
    pdf.chapter_title('9. Remitos')
    pdf.chapter_body(
        "Los remitos registran el despacho y la entrega de mercadería. Permiten seguir el estado de cada envío "
        "y relacionarlo con un comprobante.\n\n"
        "Estados típicos: Pendiente, Despachado, Entregado, Anulado.\n\n"
        "Use filtros por entidad, fechas y depósito para ubicar un remito rápidamente."
    )

    # --- CAPITULO 10: MOVIMIENTOS ---
    pdf.chapter_title('10. Movimientos de Stock')
    pdf.chapter_body(
        "Esta vista es el historial de entradas, salidas y ajustes. Cada movimiento muestra artículo, tipo, cantidad, "
        "depósito y, cuando aplica, el comprobante asociado.\n\n"
        "Use los filtros por artículo, tipo de movimiento, depósito y rango de fechas. "
        "Las observaciones ayudan a entender el motivo del cambio."
    )

    pdf.add_page()

    # --- CAPITULO 11: CAJA Y PAGOS ---
    pdf.chapter_title('11. Caja y Pagos')
    pdf.chapter_body(
        "Registra ingresos y egresos de caja. Para cargar un pago: seleccione la entidad, el comprobante pendiente, "
        "la forma de pago, el monto y la fecha. Puede agregar referencia u observaciones.\n\n"
        "Recomendación: revise los datos antes de confirmar."
    )

    # --- CAPITULO 12: CUENTAS CORRIENTES ---
    pdf.chapter_title('12. Cuentas Corrientes')
    pdf.chapter_body(
        "Muestra el saldo por entidad. Estados comunes: Deudor, A favor y Al día.\n\n"
        "Puede abrir el historial de movimientos para ver Debe/Haber y el saldo acumulado."
    )

    # --- CAPITULO 13: LISTA DE PRECIOS ---
    pdf.chapter_title('13. Lista de Precios')
    pdf.chapter_body(
        "Cree listas con nombre y orden para definir prioridades. Puede activar o desactivar listas según necesidad. "
        "Las listas se aplican a artículos y permiten trabajar con diferentes precios por segmento."
    )

    # --- CAPITULO 14: ACTUALIZACION MASIVA ---
    pdf.chapter_title('14. Actualización Masiva')
    pdf.section_title('1. Filtrar artículos')
    pdf.chapter_body(
        "Use filtros de nombre, marca, rubro, proveedor, lista de precios y estado para delimitar el conjunto."
    )
    pdf.section_title('2. Definir objetivo y ajuste')
    pdf.chapter_body(
        "Elija el objetivo (Costo Base o una lista). "
        "El cálculo siempre parte del costo: si ajusta Costo Base, todas las listas se recalculan desde allí; "
        "si ajusta una lista, el sistema deriva el nuevo costo y luego recalcula todas las listas. "
        "Puede usar porcentaje, monto fijo o valor exacto."
    )
    pdf.section_title('3. Vista previa y aplicación')
    pdf.chapter_body(
        "Genere la vista previa, seleccione filas y aplique los cambios. "
        "En modo Costo Base verá una columna por cada lista activa con 'actual -> nuevo (variación)'. "
        "Si falta una lista para un artículo se mostrará '—'."
    )

    # --- CAPITULO 15: RESOLUCIÓN DE FALLOS ---
    pdf.add_page()
    pdf.chapter_title('15. Resolución de Problemas y ¿Qué Hacer si Algo Falla?')

    pdf.section_title('Problema A: El sistema no inicia o dice "Error de Base de Datos"')
    pdf.chapter_body(
        "- Causa: El servicio de PostgreSQL está detenido o el firewall bloquea la conexión.\n"
        "- Solución: Reinicie la PC o verifique que el servicio de Postgres esté 'En Ejecución' en Servicios de Windows."
    )

    pdf.section_title('Problema B: "openssl no encontrado" al facturar AFIP')
    pdf.chapter_body(
        "- Causa: OpenSSL no está instalado o no está accesible desde la app.\n"
        "- Solución: Instale OpenSSL o copie `openssl.exe` junto con `libcrypto-*.dll` y `libssl-*.dll` al directorio del ejecutable."
    )

    pdf.section_title('Problema C: El PDF no se genera o no se abre')
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
    pdf.chapter_body(
        "Si aparece una falla no documentada o un comportamiento inesperado, comuníquese de inmediato con soporte para "
        "recibir asistencia y resolverlo cuanto antes."
    )

    # --- RESUMEN DE ATAJOS ---
    pdf.ln(10)
    pdf.chapter_title('16. Resumen Final de Atajos')
    pdf.add_shortcut('Enter', 'En comprobantes, avanza por campos y ejecuta el botón enfocado.')
    pdf.add_shortcut('Esc', 'Cancela la acción o cierra el modal actual.')
    pdf.add_shortcut('Ctrl + S', 'En algunas pantallas, guarda el borrador.')
    pdf.add_shortcut('Tab / Shift+Tab', 'En comprobantes, recorre campos, selectores y botones del modal.')
    pdf.add_shortcut('Alt + [Letra]', 'Navega entre las pestañas del menú lateral.')
    pdf.add_shortcut('Botón Actualizar', 'Recarga los datos de la tabla o la vista actual.')

    pdf.ln(15)
    pdf.set_font('helvetica', 'I', 10)
    pdf.set_text_color(100, 116, 139)
    pdf.multi_cell(
        0, 10,
        sanitize_text('Este manual es una herramienta dinámica. Si detecta un error no documentado, comuníquese con soporte para su inclusión.'),
        align='C',
    )

    output_path = "MANUAL_MAESTRO_NEXORYN_TECH.pdf"
    pdf.output(output_path)
    print(f"Manual Maestro generado con éxito en: {output_path}")

if __name__ == "__main__":
    generate_manual()
