import io, os, json, re
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Tuple

from PIL import Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfgen import canvas

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = PdfWriter = None

PAGE_W, PAGE_H = A4
BG_PATH = os.path.join('/mnt/data', 'voucher_bg.png')
BG_W_PX = 1448
BG_H_PX = 2048
SCALE = PAGE_W / BG_W_PX
DIGIT_CENTER_X = [614.5, 707.0, 799.5, 892.0, 984.0, 1076.5, 1169.0, 1261.0]
DIGIT_CENTER_Y_PX = 915


def px_to_pt(x_px: float, y_px: float) -> Tuple[float, float]:
    return x_px * SCALE, (BG_H_PX - y_px) * SCALE


def _font() -> str:
    try:
        pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
        return 'STSong-Light'
    except Exception:
        return 'Helvetica'


def _safe(v: Any) -> str:
    return '' if v is None else str(v).strip()


def _to_int(v: Any) -> int:
    try:
        return int(Decimal(str(v or 0).replace(',', '')))
    except (InvalidOperation, ValueError):
        return 0


def _draw_text(c: canvas.Canvas, text: str, x_px: float, y_px: float, size: int = 12, font: str = None):
    if not text:
        return
    font = font or _font()
    x, y = px_to_pt(x_px, y_px)
    c.setFont(font, size)
    c.drawString(x, y, str(text))


def _draw_center(c: canvas.Canvas, text: str, x_px: float, y_px: float, size: int = 12, font: str = None):
    if not text:
        return
    font = font or _font()
    x, y = px_to_pt(x_px, y_px)
    c.setFont(font, size)
    c.drawCentredString(x, y, str(text))


def _draw_fill_box(c: canvas.Canvas, x_px: float, y_px: float, size_px: float = 18):
    x, y = px_to_pt(x_px, y_px + size_px)
    s = size_px * SCALE
    c.saveState()
    c.setFillGray(0)
    c.rect(x, y, s, s, fill=1, stroke=0)
    c.restoreState()


def _draw_line(c: canvas.Canvas, x1_px: float, y1_px: float, x2_px: float, y2_px: float, width: float = 1.0):
    x1, y1 = px_to_pt(x1_px, y1_px)
    x2, y2 = px_to_pt(x2_px, y2_px)
    c.saveState()
    c.setLineWidth(width)
    c.line(x1, y1, x2, y2)
    c.restoreState()


def _draw_wrapped(c: canvas.Canvas, text: str, left_px: float, top_px: float, width_px: float, line_h_px: float, max_lines: int = 2, size: int = 12, font: str = None):
    if not text:
        return
    font = font or _font()
    max_w = width_px * SCALE
    buf = ''
    lines: List[str] = []
    for ch in str(text).replace('\r', ''):
        if ch == '\n':
            lines.append(buf)
            buf = ''
            continue
        if pdfmetrics.stringWidth(buf + ch, font, size) <= max_w:
            buf += ch
        else:
            lines.append(buf)
            buf = ch
        if len(lines) >= max_lines:
            break
    if buf and len(lines) < max_lines:
        lines.append(buf)
    c.setFont(font, size)
    for i, line in enumerate(lines[:max_lines]):
        x, y = px_to_pt(left_px, top_px + i * line_h_px)
        c.drawString(x, y, line)


def _extract_attachment_paths(record: Dict[str, Any], attachment_paths=None) -> List[str]:
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
            p = _safe(item.get('path'))
            if p:
                out.append(p)
        elif isinstance(item, str):
            p = _safe(item)
            if p:
                out.append(p)
    return out


def _image_grid_pdf_bytes(image_paths: List[str]) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    margin = 28
    gap = 12
    usable_w = PAGE_W - 2 * margin
    usable_h = PAGE_H - 2 * margin
    cols, rows = 2, 2
    cell_w = (usable_w - gap) / cols
    cell_h = (usable_h - gap) / rows
    per_page = cols * rows
    for idx, path in enumerate(image_paths):
        if idx and idx % per_page == 0:
            c.showPage()
        slot = idx % per_page
        col = slot % cols
        row = slot // cols
        x = margin + col * (cell_w + gap)
        y = PAGE_H - margin - (row + 1) * cell_h - row * gap
        try:
            with Image.open(path) as im:
                iw, ih = im.size
        except Exception:
            continue
        ratio = iw / ih if ih else 1
        box_ratio = cell_w / cell_h if cell_h else 1
        if ratio >= box_ratio:
            dw = cell_w; dh = cell_w / ratio
        else:
            dh = cell_h; dw = cell_h * ratio
        dx = x + (cell_w - dw) / 2
        dy = y + (cell_h - dh) / 2
        c.drawImage(ImageReader(path), dx, dy, width=dw, height=dh, preserveAspectRatio=True, mask='auto')
    c.save()
    return buf.getvalue()


def _merge_attachments(base_pdf: bytes, paths: List[str]) -> bytes:
    if not paths or not PdfReader or not PdfWriter:
        return base_pdf
    writer = PdfWriter()
    for p in PdfReader(io.BytesIO(base_pdf)).pages:
        writer.add_page(p)
    img_group: List[str] = []

    def flush_imgs():
        nonlocal img_group
        if not img_group:
            return
        r = PdfReader(io.BytesIO(_image_grid_pdf_bytes(img_group)))
        for p in r.pages:
            writer.add_page(p)
        img_group = []

    for pth in paths:
        if not pth or not os.path.exists(pth):
            continue
        low = pth.lower()
        try:
            if low.endswith('.pdf'):
                flush_imgs()
                for p in PdfReader(pth).pages:
                    writer.add_page(p)
            elif low.endswith(('.png', '.jpg', '.jpeg', '.webp', '.bmp')):
                img_group.append(pth)
        except Exception:
            continue
    flush_imgs()
    out = io.BytesIO(); writer.write(out); return out.getvalue()


def _roc_ymd(date_str: str):
    m = re.match(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', _safe(date_str))
    if not m:
        return '', '', ''
    y, mo, d = m.groups()
    return str(int(y)-1911), f'{int(mo):02d}', f'{int(d):02d}'


def _form_id(record: Dict[str, Any]) -> str:
    rid = _safe(record.get('record_id'))
    if rid:
        return rid
    emp = _safe(record.get('employee_no')) or '00000'
    y, m, d = _roc_ymd(_safe(record.get('form_date')))
    roc = f'{int(y):03d}{m}{d}' if y else '0000000'
    return f'EX{emp}{roc}001'


def _draw_alignment_overlays(c: canvas.Canvas):
    # 1) employee name / employee no underline slightly lower
    _draw_line(c, 448, 560, 706, 560, 0.8)
    _draw_line(c, 947, 560, 1123, 560, 0.8)

    # 2) vendor section full height down to payee bottom line
    # reinforce bottom boundary of vendor block at same level as payee underline
    _draw_line(c, 286, 913, 1289, 913, 0.8)
    # reinforce left vertical of payment section to vendor bottom
    _draw_line(c, 286, 488, 286, 913, 0.8)

    # 3) extend receipt-number lower gridline through amount title cell
    _draw_line(c, 286, 941, 495, 941, 0.8)


def build_pdf_bytes(record: Dict[str, Any], attachment_paths=None) -> bytes:
    font = _font()
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)

    if os.path.exists(BG_PATH):
        c.drawImage(ImageReader(BG_PATH), 0, 0, width=PAGE_W, height=PAGE_H)

    _draw_alignment_overlays(c)

    _draw_text(c, f'表單ID：{_form_id(record)}', 1125, 160, 9, font)
    y, m, d = _roc_ymd(record.get('form_date', ''))
    _draw_center(c, y, 1167, 297, 15, font)
    _draw_center(c, m, 1254, 297, 15, font)
    _draw_center(c, d, 1338, 297, 15, font)

    _draw_text(c, _safe(record.get('plan_code')), 387, 392, 13, font)
    purpose = _safe(record.get('purpose_desc') or record.get('purpose') or record.get('trip_purpose'))
    _draw_wrapped(c, purpose, 387, 470, 900, 24, max_lines=2, size=12, font=font)

    target = _safe(record.get('payment_target_type') or (
        'employee' if str(record.get('employee_enabled')).lower() in {'true','1','yes'} else
        'advance_offset' if str(record.get('advance_offset_enabled')).lower() in {'true','1','yes'} else
        'vendor' if str(record.get('vendor_enabled')).lower() in {'true','1','yes'} else ''
    ))
    if target == 'employee':
        _draw_fill_box(c, 377, 535, 18)
        _draw_text(c, _safe(record.get('employee_name')), 512, 556, 12, font)
        _draw_text(c, _safe(record.get('employee_no')), 920, 556, 12, font)
    elif target == 'advance_offset':
        _draw_fill_box(c, 377, 616, 18)
        _draw_text(c, _safe(record.get('advance_amount')), 421, 684, 12, font)
        _draw_text(c, _safe(record.get('offset_amount')), 654, 684, 12, font)
        _draw_text(c, _safe(record.get('refund_amount')), 906, 684, 12, font)
        _draw_text(c, _safe(record.get('supplement_amount')), 1160, 684, 12, font)
    elif target == 'vendor':
        _draw_fill_box(c, 377, 741, 18)
        _draw_text(c, _safe(record.get('vendor_name')), 514, 802, 12, font)
        _draw_text(c, _safe(record.get('vendor_address')), 515, 852, 12, font)
        _draw_text(c, _safe(record.get('payee_name')), 515, 896, 12, font)

    _draw_text(c, _safe(record.get('receipt_count') or record.get('receipt_no')), 389, 941, 12, font)
    amount_total = _to_int(record.get('amount_total') or record.get('amount') or record.get('total_amount'))
    digits = list(str(amount_total).zfill(8)[-8:])
    for xpx, ch in zip(DIGIT_CENTER_X, digits):
        _draw_center(c, ch, xpx, DIGIT_CENTER_Y_PX, 16, font)

    c.save()
    base = buf.getvalue()
    return _merge_attachments(base, _extract_attachment_paths(record, attachment_paths))


def merge_expense_pdf_with_attachments(record_or_pdf, attachment_paths=None) -> bytes:
    if isinstance(record_or_pdf, (bytes, bytearray)):
        return _merge_attachments(bytes(record_or_pdf), attachment_paths or [])
    return build_pdf_bytes(record_or_pdf, attachment_paths=attachment_paths)
