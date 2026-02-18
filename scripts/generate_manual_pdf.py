from __future__ import annotations

import argparse
import datetime
import re
import sys
from pathlib import Path
from typing import List

from fpdf import FPDF
from fpdf.enums import XPos, YPos


DEFAULT_SOURCE = Path("docs/MANUAL_OPERATIVO.md")
DEFAULT_OUTPUT = Path("MANUAL_MAESTRO_NEXORYN_TECH.pdf")


class ManualPDF(FPDF):
    def __init__(self, generated_at: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.generated_at = generated_at or datetime.datetime.now().strftime("%d/%m/%Y %H:%M")

    def header(self):
        self.set_font("helvetica", "B", 15)
        self.set_text_color(99, 102, 241)
        self.cell(
            0,
            10,
            sanitize_text("NEXORYN TECH - Manual Maestro"),
            border=False,
            align="R",
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(128)
        self.cell(
            0,
            10,
            sanitize_text(f"Página {self.page_no()} | Generado: {self.generated_at}"),
            border=0,
            align="C",
            new_x=XPos.RIGHT,
            new_y=YPos.TOP,
        )

    def chapter_title(self, label: str):
        self.set_font("helvetica", "B", 16)
        self.set_text_color(30, 41, 59)
        self.ln(5)
        self.cell(
            0,
            10,
            sanitize_text(label),
            align="L",
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        self.ln(4)
        self.set_draw_color(226, 232, 240)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(5)

    def section_title(self, label: str):
        self.set_font("helvetica", "B", 12)
        self.set_text_color(79, 70, 229)
        self.ln(3)
        self.cell(
            0,
            8,
            sanitize_text(label),
            align="L",
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        self.ln(2)

    def subsection_title(self, label: str):
        self.set_font("helvetica", "B", 11)
        self.set_text_color(30, 41, 59)
        self.ln(2)
        self.cell(
            0,
            7,
            sanitize_text(label),
            align="L",
            new_x=XPos.LMARGIN,
            new_y=YPos.NEXT,
        )
        self.ln(1)

    def chapter_body(self, text: str):
        self.set_font("helvetica", "", 11)
        self.set_text_color(51, 65, 85)
        self.multi_cell(0, 7, sanitize_text(text))
        self.ln()

    def bullet_item(self, text: str, indent_level: int = 0, marker: str = "-"):
        self.set_font("helvetica", "", 11)
        self.set_text_color(51, 65, 85)
        indent = min(max(indent_level, 0), 8) * 4
        self.set_x(self.l_margin + indent)
        self.multi_cell(0, 6, sanitize_text(f"{marker} {text}"))
        self.ln(1)

    def quote_line(self, text: str):
        self.set_font("helvetica", "I", 10)
        self.set_text_color(100, 116, 139)
        self.multi_cell(0, 6, sanitize_text(f"> {text}"))
        self.ln(1)

    def code_block(self, text: str):
        self.set_font("courier", "", 9)
        self.set_text_color(30, 41, 59)
        self.set_fill_color(241, 245, 249)
        self.set_draw_color(226, 232, 240)
        self.multi_cell(0, 5, sanitize_text(text), border=1, fill=True)
        self.ln(2)


def sanitize_text(text: str | None) -> str:
    if text is None:
        return ""
    if not isinstance(text, str):
        text = str(text)
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


def load_markdown(source_path: Path) -> str:
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"No existe el archivo fuente: {source_path}")
    content = source_path.read_text(encoding="utf-8")
    if not content.strip():
        raise ValueError(f"El archivo fuente está vacío: {source_path}")
    return content


def extract_top_level_titles(markdown_text: str) -> List[str]:
    titles: List[str] = []
    in_code = False
    for raw_line in markdown_text.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("```"):
            in_code = not in_code
            continue
        if in_code:
            continue
        if stripped.startswith("# "):
            title = stripped[2:].strip()
            if title:
                titles.append(sanitize_text(title))
    return titles


def render_markdown(pdf: ManualPDF, markdown_text: str):
    in_code = False
    paragraph: List[str] = []
    code_lines: List[str] = []

    def flush_paragraph():
        if paragraph:
            full = " ".join(paragraph).strip()
            if full:
                pdf.chapter_body(full)
            paragraph.clear()

    def flush_code():
        nonlocal code_lines
        if code_lines:
            pdf.code_block("\n".join(code_lines))
            code_lines = []

    for raw_line in markdown_text.splitlines():
        raw = raw_line.rstrip("\n")
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
            pdf.chapter_title(stripped[2:].strip())
            continue

        if stripped.startswith("## "):
            flush_paragraph()
            pdf.section_title(stripped[3:].strip())
            continue

        if stripped.startswith("### "):
            flush_paragraph()
            pdf.subsection_title(stripped[4:].strip())
            continue

        if stripped.startswith(">"):
            flush_paragraph()
            quote = re.sub(r"^>+\s?", "", stripped)
            pdf.quote_line(quote)
            continue

        list_match = re.match(r"^(\s*)([-*]|\d+\.)\s+(.*)$", raw)
        if list_match:
            flush_paragraph()
            leading_spaces = list_match.group(1) or ""
            marker = list_match.group(2).strip()
            item_text = list_match.group(3).strip()
            indent_level = len(leading_spaces) // 2
            if marker in {"-", "*"}:
                marker = "-"
            pdf.bullet_item(item_text, indent_level=indent_level, marker=marker)
            continue

        paragraph.append(stripped)

    if in_code:
        flush_code()
    flush_paragraph()


def add_cover(pdf: ManualPDF):
    pdf.add_page()
    pdf.set_y(60)
    pdf.set_font("helvetica", "B", 45)
    pdf.set_text_color(99, 102, 241)
    pdf.cell(
        0,
        20,
        sanitize_text("Manual Maestro"),
        align="C",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("helvetica", "B", 28)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(
        0,
        15,
        sanitize_text("Sistema Nexoryn Tech"),
        align="C",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.ln(10)
    pdf.set_font("helvetica", "", 16)
    pdf.set_text_color(100, 116, 139)
    pdf.cell(
        0,
        10,
        sanitize_text("Guía Operativa"),
        align="C",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )

    pdf.set_y(220)
    pdf.set_font("helvetica", "B", 12)
    pdf.set_text_color(30, 41, 59)
    pdf.cell(
        0,
        10,
        sanitize_text("Sistema de Gestión Integral"),
        align="C",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("helvetica", "", 10)
    pdf.cell(
        0,
        10,
        sanitize_text("Documento generado automáticamente desde fuente Markdown."),
        align="C",
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )


def add_table_of_contents(pdf: ManualPDF, titles: List[str]):
    pdf.add_page()
    pdf.chapter_title("Contenido del Manual")
    if not titles:
        pdf.chapter_body("No se detectaron capítulos en la fuente Markdown.")
        return
    toc = [f"{idx}. {title}" for idx, title in enumerate(titles, start=1)]
    pdf.chapter_body("\n".join(toc))


def generate_manual(source_path: Path, output_path: Path, generated_at: str | None = None) -> Path:
    markdown_text = load_markdown(source_path)
    titles = extract_top_level_titles(markdown_text)

    pdf = ManualPDF(generated_at=generated_at)
    pdf.set_auto_page_break(auto=True, margin=15)

    add_cover(pdf)
    add_table_of_contents(pdf, titles)
    pdf.add_page()
    render_markdown(pdf, markdown_text)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(output_path))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Genera el Manual Maestro en PDF desde una fuente Markdown."
    )
    parser.add_argument(
        "--source",
        default=str(DEFAULT_SOURCE),
        help="Ruta del archivo Markdown fuente.",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT),
        help="Ruta de salida del PDF.",
    )
    parser.add_argument(
        "--generated-at",
        default=None,
        help="Timestamp opcional para pie de página (ej: 10/02/2026 12:00).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    source_path = Path(args.source)
    output_path = Path(args.output)

    try:
        generated = generate_manual(source_path, output_path, generated_at=args.generated_at)
    except Exception as exc:
        print(f"Error al generar manual: {exc}", file=sys.stderr)
        return 1

    print(f"Manual Maestro generado con éxito en: {generated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
