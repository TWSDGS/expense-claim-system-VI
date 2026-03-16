
import io
import os
import re
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple, Optional

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None

from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

# Background image native size (pixels)
BG_W_PX = 1448
BG_H_PX = 2048

# A4 size in points
PAGE_W, PAGE_H = A4
SCALE = PAGE_W / BG_W_PX
DEFAULT_BG_NAME = 'voucher_bg.png'

# Digit boxes boundaries detected from the provided image (pixels)
DIGIT_BOX_XS = [568, 661, 753, 846, 938, 1030, 1123, 1215, 1307]
DIGIT_CENTER_X = [(DIGIT_BOX_XS[i] + DIGIT_BOX_XS[i + 1]) / 2 for i in range(8)]
DIGIT_CENTER_Y_PX = 915


def px_to_pt(x_px: float, y_px: float) -> Tuple[float, float]:
    """Convert image pixel coords (origin top-left) to PDF points (origin bottom-left)."""
    x_pt = x_px * SCALE
    y_pt = (BG_H_PX - y_px) * SCALE
    return x_pt, y_pt


def _draw_mark_rect(c: canvas.Canvas, x_px: float, y_px: float, size_px: float = 16, pad_px: float = 2) -> None:
    """Draw a filled black square mark inside a checkbox area."""
    x_pt = (x_px + pad_px) * SCALE
    bottom_y_px = y_px + pad_px + size_px
    y_pt = (BG_H_PX - bottom_y_px) * SCALE
    w = size_px * SCALE
    h = size_px * SCALE
    c.saveState()
    c.setFillColorRGB(0, 0, 0)
    c.setStrokeColorRGB(0, 0, 0)
    c.rect(x_pt, y_pt, w, h, stroke=0, fill=1)
    c.restoreState()


def _try_register_tc_font() -> str:
    """Prefer a Traditional Chinese font if present in ./fonts to avoid garbled Chinese in PDF."""
    candidates = [
        ('bkai00mp', os.path.join('fonts', 'bkai00mp.ttf')),
        ('gkai00mp', os.path.join('fonts', 'gkai00mp.ttf')),
    ]

    here = os.path.dirname(__file__)
    for name, rel_path in candidates:
        full_path = rel_path if os.path.isabs(rel_path) else os.path.join(here, rel_path)
        if os.path.isfile(full_path):
            try:
                pdfmetrics.registerFont(TTFont(name, full_path))
                return name
            except Exception:
                pass

    try:
        pdfmetrics.registerFont(UnicodeCIDFont('MSung-Light'))
        return 'MSung-Light'
    except Exception:
        return 'Helvetica'


def _wrap_text(text: str, font_name: str, font_size: int, max_width_pt: float) -> List[str]:
    """Simple CJK-friendly wrapping by characters."""
    if not text:
        return []
    lines: List[str] = []
    buf = ''
    for ch in str(text):
        if ch == '\n':
            lines.append(buf)
            buf = ''
            continue
        w = pdfmetrics.stringWidth(buf + ch, font_name, font_size)
        if w <= max_width_pt:
            buf += ch
        else:
            if buf:
                lines.append(buf)
            buf = ch
    if buf:
        lines.append(buf)
    return lines


def _to_int_amount(amount_val: Any) -> int:
    if amount_val in (None, ''):
        return 0
    try:
        d = Decimal(str(amount_val).replace(',', '').strip())
        return int(d)
    except (InvalidOperation, ValueError):
        return 0


def _extract_attachment_paths(record: Dict[str, Any], attachment_paths: Optional[List[str]] = None) -> List[str]:
    out: List[str] = []
    src = attachment_paths if attachment_paths is not None else record.get('attachment_files') or record.get('attachments') or []
    if isinstance(src, str):
        try:
            parsed = json.loads(src)
            src = parsed if isinstance(parsed, list) else [src]
        except Exception:
            src = [src]
    for item in src:
        if isinstance(item, dict):
            p = str(item.get('path', '')).strip()
            if p:
                out.append(p)
        elif isinstance(item, str):
            p = item.strip()
            if p:
                out.append(p)
    return out


def _resolve_bg_image_path(bg_image_path: Optional[str] = None) -> str:
    here = os.path.dirname(__file__)
    candidates = []
    if bg_image_path:
        candidates.append(bg_image_path)
        if not os.path.isabs(bg_image_path):
            candidates.append(os.path.join(here, bg_image_path))
    candidates.extend([
        os.path.join(here, 'templates', DEFAULT_BG_NAME),
        os.path.join(here, DEFAULT_BG_NAME),
        DEFAULT_BG_NAME,
    ])
    for p in candidates:
        if p and os.path.exists(p):
            return p
    return candidates[0] if candidates else ''


def _image_to_pdf_bytes(image_path: str) -> bytes:
    """Convert a single image to a 1-page A4 PDF, scaled to fit with margins."""
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    page_w, page_h = A4
    margin = 24

    with Image.open(image_path) as im:
        im = im.convert('RGB')
        iw, ih = im.size
        img_ratio = iw / ih if ih else 1.0

    max_w = page_w - 2 * margin
    max_h = page_h - 2 * margin
    box_ratio = max_w / max_h if max_h else 1.0

    if img_ratio >= box_ratio:
        draw_w = max_w
        draw_h = max_w / img_ratio
    else:
        draw_h = max_h
        draw_w = max_h * img_ratio

    x = (page_w - draw_w) / 2
    y = (page_h - draw_h) / 2

    c.drawImage(ImageReader(image_path), x, y, width=draw_w, height=draw_h, preserveAspectRatio=True, mask='auto')
    c.showPage()
    c.save()
    return buf.getvalue()


def _merge_attachments(base_pdf: bytes, attachment_paths: List[str]) -> bytes:
    """Append attachment files (PDFs or images) after the first page."""
    if not attachment_paths or not PdfWriter or not PdfReader:
        return base_pdf

    writer = PdfWriter()
    base_reader = PdfReader(io.BytesIO(base_pdf))
    for p in base_reader.pages:
        writer.add_page(p)

    for pth in attachment_paths:
        if not pth or not os.path.exists(pth):
            continue
        lower = pth.lower()
        try:
            if lower.endswith('.pdf'):
                r = PdfReader(pth)
                for page in r.pages:
                    writer.add_page(page)
            elif lower.endswith(('.png', '.jpg', '.jpeg', '.webp', '.bmp')):
                img_pdf = _image_to_pdf_bytes(pth)
                r = PdfReader(io.BytesIO(img_pdf))
                for page in r.pages:
                    writer.add_page(page)
        except Exception:
            continue

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def _resolve_build_args(bg_image_path=None, attachment_paths=None):
    """Support both old and new call signatures.

    Accepted patterns:
    - build_pdf_bytes(record)
    - build_pdf_bytes(record, attachment_paths=[...])
    - build_pdf_bytes(record, bg_image_path='...')
    - build_pdf_bytes(record, 'path/to/bg.png', [...])
    - build_pdf_bytes(record, [...])  # legacy positional attachment paths
    """
    bg = None
    atts = None

    if isinstance(bg_image_path, (list, tuple)) and attachment_paths is None:
        atts = list(bg_image_path)
    else:
        bg = bg_image_path
        atts = attachment_paths

    if atts is None:
        atts = []
    return bg, list(atts)


def build_pdf_bytes(record: Dict[str, Any], bg_image_path=None, attachment_paths=None) -> bytes:
    bg_image_path, attachment_paths = _resolve_build_args(bg_image_path, attachment_paths)
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    resolved_bg = _resolve_bg_image_path(bg_image_path)
    if resolved_bg and os.path.exists(resolved_bg):
        c.drawImage(ImageReader(resolved_bg), 0, 0, width=PAGE_W, height=PAGE_H, mask='auto')

    font = _try_register_tc_font()
    c.setFont(font, 11)

    form_date = record.get('form_date', '')
    ymd = None
    if form_date:
        s = str(form_date).strip()
        try:
            date_obj = datetime.fromisoformat(s.replace('/', '-')).date()
            ymd = (date_obj.year, date_obj.month, date_obj.day)
        except Exception:
            m = re.match(r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})', s)
            if m:
                ymd = (int(m.group(1)), int(m.group(2)), int(m.group(3)))

    if ymd:
        y_px = 346
        x_year, y = px_to_pt(1111, y_px)
        x_month, y = px_to_pt(1199, y_px)
        x_day, y = px_to_pt(1290, y_px)
        c.drawRightString(x_year, y, f'{ymd[0]:04d}')
        c.drawRightString(x_month, y, f'{ymd[1]:02d}')
        c.drawRightString(x_day, y, f'{ymd[2]:02d}')
    else:
        s = str(form_date).strip()
        if s:
            x, y = px_to_pt(943, 356)
            c.drawString(x, y, s)

    x, y = px_to_pt(425, 395)
    c.drawString(x, y, str(record.get('plan_code', '')))

    purpose = str(record.get('purpose_desc') or record.get('purpose') or '')
    max_w = (1300 - 403) * SCALE
    lines = _wrap_text(purpose, font, 11, max_w)
    start_x, start_y = px_to_pt(403, 480)
    line_h = 14
    for i, line in enumerate(lines[:3]):
        c.drawString(start_x, start_y - i * line_h, line)

    mode = str(record.get('payment_mode') or record.get('payment_target_type') or '').strip()
    if mode == 'advance_offset':
        mode = 'advance'

    is_adv = str(record.get('is_advance_offset', '')).lower() in ('true', '1', 'yes') or str(record.get('advance_offset_enabled', '')).lower() in ('true', '1', 'yes')
    is_vendor = (
        str(record.get('is_direct_vendor_pay', '')).lower() in ('true', '1', 'yes')
        or record.get('payee_type', '') == 'vendor'
        or str(record.get('vendor_enabled', '')).lower() in ('true', '1', 'yes')
    )
    is_employee = str(record.get('employee_enabled', '')).lower() in ('true', '1', 'yes')

    if mode not in ('employee', 'advance', 'vendor'):
        if is_adv:
            mode = 'advance'
        elif is_vendor:
            mode = 'vendor'
        elif is_employee:
            mode = 'employee'
        else:
            mode = 'employee'

    if mode == 'employee':
        _draw_mark_rect(c, 400, 528, size_px=18, pad_px=4)
    elif mode == 'advance':
        _draw_mark_rect(c, 400, 588, size_px=18, pad_px=4)
    elif mode == 'vendor':
        _draw_mark_rect(c, 400, 738, size_px=18, pad_px=4)
    c.setFont(font, 11)

    x, y = px_to_pt(562, 546)
    c.drawString(x, y, str(record.get('employee_name', '')))
    x, y = px_to_pt(923, 546)
    c.drawString(x, y, str(record.get('employee_no', '')))

    show_adv = mode == 'advance' or is_adv
    if show_adv:
        _draw_mark_rect(c, 400, 588, size_px=18, pad_px=4)
        c.setFont(font, 11)

        x, y = px_to_pt(548, 667)
        c.drawString(x, y, str(int(record.get('advance_amount') or 0)))
        x, y = px_to_pt(760, 667)
        c.drawString(x, y, str(int(record.get('offset_amount') or 0)))
        x, y = px_to_pt(965, 667)
        c.drawString(x, y, str(int(record.get('balance_refund_amount') or record.get('refund_amount') or 0)))
        x, y = px_to_pt(1178, 667)
        c.drawString(x, y, str(int(record.get('supplement_amount') or 0)))

    x, y = px_to_pt(600, 760)
    c.drawString(x, y, str(record.get('vendor_name', '')))
    x, y = px_to_pt(600, 800)
    c.drawString(x, y, str(record.get('vendor_address', '')))
    x, y = px_to_pt(600, 835)
    c.drawString(x, y, str(record.get('vendor_payee_name') or record.get('payee_name') or '',))

    x, y = px_to_pt(210, 915)
    c.drawString(x, y, str(record.get('receipt_no') or record.get('receipt_count') or ''))

    amt_int = _to_int_amount(record.get('amount_total') or record.get('amount') or record.get('total_amount'))
    if 0 <= amt_int <= 99999999:
        digits = f'{amt_int:08d}'
        c.setFont(font, 14)
        for i, dch in enumerate(digits):
            cx_pt, cy_pt = px_to_pt(DIGIT_CENTER_X[i], DIGIT_CENTER_Y_PX)
            c.drawCentredString(cx_pt, cy_pt - 5, dch)
        c.setFont(font, 11)

    sig_y_px = 1048
    sig_specs = [
        ('handler_name', 229),
        ('project_manager_name', 484),
        ('dept_manager_name', 776),
        ('accountant_name', 1051),
    ]
    for key, x_px in sig_specs:
        val = str(record.get(key, '')).strip()
        if val:
            x, y = px_to_pt(x_px, sig_y_px)
            c.drawCentredString(x, y, val)

    c.showPage()
    c.save()

    base_pdf = buf.getvalue()
    merged_paths = _extract_attachment_paths(record, attachment_paths)
    if merged_paths:
        return _merge_attachments(base_pdf, merged_paths)
    return base_pdf


def merge_expense_pdf_with_attachments(record_or_pdf, attachment_paths=None) -> bytes:
    """Compatibility wrapper used by expense.py / apps.expense.py."""
    if isinstance(record_or_pdf, (bytes, bytearray)):
        return _merge_attachments(bytes(record_or_pdf), attachment_paths or [])
    return build_pdf_bytes(record_or_pdf, attachment_paths=attachment_paths)
