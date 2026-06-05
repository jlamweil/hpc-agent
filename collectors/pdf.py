"""PDF text extraction with section heading detection."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

try:
    import fitz  # PyMuPDF

    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False
    fitz = None  # type: ignore


def extract_text(pdf_path: str | Path) -> dict[str, Any]:
    """Extract text from a PDF file.
    
    Returns dict with:
        - text: str — full extracted text
        - pages: int — number of pages
        - sections: list[dict] — detected sections (heading, level, content)
        - total_chars: int
        - warnings: list[str] — extraction quality notes
    
    Args:
        pdf_path: Path to PDF file.
    
    Returns:
        Extraction result dict. On error, returns dict with error field.
    """
    if not HAS_PYMUPDF:
        return {
            "error": "PyMuPDF not installed. Install: pip install pymupdf",
            "text": "",
            "pages": 0,
            "sections": [],
            "total_chars": 0,
            "warnings": ["PyMuPDF not available"],
        }

    path = Path(pdf_path)
    if not path.exists():
        return {
            "error": f"File not found: {path}",
            "text": "",
            "pages": 0,
            "sections": [],
            "total_chars": 0,
            "warnings": [],
        }

    try:
        doc = fitz.open(str(path))
    except Exception as e:
        return {
            "error": f"Failed to open PDF: {e}",
            "text": "",
            "pages": 0,
            "sections": [],
            "total_chars": 0,
            "warnings": [],
        }

    pages_text: list[str] = []
    sections: list[dict[str, Any]] = []
    warnings: list[str] = []
    total_chars = 0

    for page_num, page in enumerate(doc):
        try:
            page_text = page.get_text()

            # Detect headings by font size heuristics
            blocks = page.get_text("dict").get("blocks", [])
            for block in blocks:
                if block.get("type") != 0:  # 0 = text block
                    continue
                for line in block.get("lines", []):
                    for span in line.get("spans", []):
                        text = span.get("text", "").strip()
                        font_size = span.get("size", 0)
                        if not text:
                            continue
                        # Heuristic: larger font = heading
                        # Typical body text is 9-12pt, headings are 14pt+
                        if font_size >= 14:
                            sections.append({
                                "heading": text,
                                "level": _estimate_heading_level(font_size),
                                "page": page_num + 1,
                                "content": "",
                            })

            if not page_text.strip():
                warnings.append(f"Page {page_num + 1}: empty or image-only")
            else:
                pages_text.append(page_text)
                total_chars += len(page_text)

        except Exception as e:
            warnings.append(f"Page {page_num + 1}: extraction error: {e}")
            continue

    doc.close()

    result = {
        "text": "\n\n".join(pages_text),
        "pages": len(doc),
        "sections": sections,
        "total_chars": total_chars,
        "warnings": warnings,
    }

    # Quality notes
    if total_chars < 100:
        result["warnings"].append("Very little text extracted — PDF may be scanned images or have table-based layout")

    return result


def _estimate_heading_level(font_size: float) -> int:
    """Estimate heading level from font size."""
    if font_size >= 24:
        return 1  # Title / H1
    elif font_size >= 18:
        return 2  # H2
    elif font_size >= 14:
        return 3  # H3
    elif font_size >= 12:
        return 4  # H4
    return 5  # H5 / bold body
