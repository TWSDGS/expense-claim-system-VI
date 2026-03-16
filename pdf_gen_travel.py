
import io
import os
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.lib.utils import ImageReader

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None

# Background image native size (pixels)
BG_W_PX = 1448
BG_H_PX = 2048
PAGE_W, PAGE_H = A4
SCALE = PAGE_W / BG_W_PX
DEFAULT_BG_NAME = 'voucher_travel_bg.png'

TOTAL_AMOUNT_DIGIT_X_PX = [420, 560, 671, 785, 912, 1035]
TOTAL_AMOUNT_DIGIT_Y_PX = 1820


def _amount_to_digit_boxes(amount) -> List[str]:
    try:
        if amount is None:
            return ['X'] * 6
        amt = int(round(float(str(amount).replace(',', '').strip() or '0')))
    except Exception:
        return ['X'] * 6
    if amt <= 0:
        return ['X'] * 6
    place_vals = [100000, 10000, 1000, 100, 10, 1]
    first_idx = None
    for i, v in enumerate(place_vals):
        if amt >= v:
            first_idx = i
            break
    if first_idx is None:
        first_idx = 5
    out: List[str] = []
    for i, v in enumerate(place_vals):
        d = (amt // v) % 10
        if i < first_idx:
            out.append('X')
        else:
            out.append(str(d))
    return out


def px_to_pt(x_px: float, y_px: float) -> Tuple[float, float]:
    x_pt = x_px * SCALE
    y_pt = (BG_H_PX - y_px) * SCALE
    return x_pt, y_pt


def _draw_mark_rect(c: canvas.Canvas, x_px: float, y_px: float, size_px: float = 18, pad_px: float = 4) -> None:
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
    candidates = [
        ('bkai00mp', os.path.join('fonts', 'bkai00mp.ttf')),
        ('gkai00mp', os.path.join('fonts', 'gkai00mp.ttf')),
    ]
    here = os.path.dirname(__file__)
    for name, rel in candidates:
        full_path = rel if os.path.isabs(rel) else os.path.join(here, rel)
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


def _image_to_pdf_bytes(image_path: str) -> bytes:
    from PIL import Image

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


def _truthy(v) -> bool:
    if v is True:
        return True
    if v is False or v is None:
        return False
    s = str(v).strip().lower()
    return s in {'1', 'true', 'yes', 'y', 'on'}


def _parse_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s))
    except Exception:
        try:
            return datetime.fromisoformat(str(s).replace('/', '-'))
        except Exception:
            return None


def _safe_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace(',', '')
        if not s:
            return float(default)
        return float(s)
    except Exception:
        return float(default)


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


def _normalize_legacy_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """Map older apps/travel.py field names to the newer coordinate-based schema."""
    if not isinstance(record, dict):
        return {}

    out = dict(record)

    if out.get('traveler') and not out.get('traveler_name'):
        out['traveler_name'] = out['traveler']
    if out.get('project_id') and not out.get('plan_code'):
        out['plan_code'] = out['project_id']
    if out.get('purpose') and not out.get('purpose_desc'):
        out['purpose_desc'] = out['purpose']
    if out.get('location') and not out.get('travel_route'):
        out['travel_route'] = out['location']
    if out.get('start_date') and not out.get('start_time'):
        out['start_time'] = out['start_date']
    if out.get('end_date') and not out.get('end_time'):
        out['end_time'] = out['end_date']
    if out.get('total_expense') and not out.get('estimated_cost'):
        out['estimated_cost'] = out['total_expense']

    transport_options = out.get('transport_options') or out.get('transport_mode') or out.get('transport_tools') or []
    if isinstance(transport_options, str):
        try:
            parsed = json.loads(transport_options)
            if isinstance(parsed, list):
                transport_options = parsed
            else:
                transport_options = [x.strip() for x in transport_options.split(',') if x.strip()]
        except Exception:
            transport_options = [x.strip() for x in transport_options.split(',') if x.strip()]
    transport_set = set(transport_options or [])
    out.setdefault('is_gov_car', '公務車' in transport_set)
    out.setdefault('is_private_car', '私車公用' in transport_set)
    out.setdefault('is_taxi', '計程車' in transport_set)
    out.setdefault('is_hsr', '高鐵' in transport_set)
    out.setdefault('is_airplane', '飛機' in transport_set)
    out.setdefault('is_dispatch_car', '派車' in transport_set)
    out.setdefault('is_other_transport', '其他' in transport_set)

    rows = out.get('expense_rows')
    if rows is None:
        rows = out.get('expense_json') or out.get('detail_json') or out.get('details') or []
    if isinstance(rows, str):
        try:
            rows = json.loads(rows)
        except Exception:
            rows = []
    normalized_rows = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        nr = dict(r)
        nr.setdefault('date_md', r.get('date_md') or r.get('date') or r.get('日期') or out.get('start_time') or '')
        nr.setdefault('route', r.get('route') or r.get('location') or r.get('起訖地點') or '')
        nr.setdefault('transport_type', r.get('transport_type') or r.get('vehicle_type') or r.get('車別') or '')
        nr.setdefault('transport_amt', r.get('transport_amt') or r.get('交通費') or 0)
        nr.setdefault('per_diem_amt', r.get('per_diem_amt') or r.get('misc_fee') or r.get('膳雜費') or 0)
        nr.setdefault('accommodation_amt', r.get('accommodation_amt') or r.get('lodging_fee') or r.get('住宿費') or 0)
        nr.setdefault('other_amt', r.get('other_amt') or r.get('其它') or r.get('other_fee') or 0)
        nr.setdefault('receipt_no', r.get('receipt_no') or r.get('單據編號') or '')
        normalized_rows.append(nr)
    out['expense_rows'] = normalized_rows
    return out


def build_pdf_bytes(record: Dict, attachment_paths: Optional[List[str]] = None, bg_image_path: Optional[str] = None) -> bytes:
    """Generate Domestic Trip PDF using background image coordinates."""
    record = _normalize_legacy_record(record)
    attachment_paths = attachment_paths or []

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    font = _try_register_tc_font()

    resolved_bg = _resolve_bg_image_path(bg_image_path)
    if resolved_bg and os.path.exists(resolved_bg):
        c.drawImage(ImageReader(resolved_bg), 0, 0, width=PAGE_W, height=PAGE_H, mask='auto')

    def draw_text(x_px: float, y_px: float, text: str, size: int = 11, align: str = 'left') -> None:
        if text is None:
            return
        s = str(text).strip()
        if not s:
            return
        x_pt, y_pt = px_to_pt(x_px, y_px)
        c.setFont(font, size)
        if align == 'right':
            c.drawRightString(x_pt, y_pt, s)
        elif align == 'center':
            c.drawCentredString(x_pt, y_pt, s)
        else:
            c.drawString(x_pt, y_pt, s)

    form_date = record.get('form_date', '') or record.get('start_time', '')
    dt_form = _parse_date(form_date)

    if dt_form:
        y_px = 310
        draw_text(1165, y_px, f'{dt_form.year:04d}', size=11, align='right')
        draw_text(1240, y_px, f'{dt_form.month:02d}', size=11, align='right')
        draw_text(1310, y_px, f'{dt_form.day:02d}', size=11, align='right')
    else:
        draw_text(1120, 288, str(form_date), size=11, align='left')

    draw_text(460, 365, record.get('traveler_name', ''), size=14)
    draw_text(980, 365, record.get('plan_code', ''), size=14)

    purpose = str(record.get('purpose_desc', '') or '').strip()
    if purpose:
        max_width_pt = (1344 - 460 - 12) * SCALE
        lines = _wrap_text(purpose, font, 13, max_width_pt)
        base_y = 440
        for i, line in enumerate(lines[:2]):
            draw_text(460, base_y + i * 16, line, size=13)

    draw_text(460, 510, record.get('travel_route', ''), size=14)

    dt_start = _parse_date(record.get('start_time', ''))
    dt_end = _parse_date(record.get('end_time', ''))

    if dt_start:
        draw_text(542, 556, f'{dt_start.year:04d}', size=11, align='right')
        draw_text(632, 556, f'{dt_start.month:02d}', size=11, align='right')
        draw_text(722, 556, f'{dt_start.day:02d}', size=11, align='right')
        draw_text(805, 556, dt_start.strftime('%H'), size=11, align='right')

    if dt_end:
        draw_text(542, 595, f'{dt_end.year:04d}', size=11, align='right')
        draw_text(632, 595, f'{dt_end.month:02d}', size=11, align='right')
        draw_text(722, 595, f'{dt_end.day:02d}', size=11, align='right')
        draw_text(805, 595, dt_end.strftime('%H'), size=11, align='right')

    days_val = record.get('travel_days', '') or record.get('trip_days', '')
    days = ''
    try:
        if str(days_val).strip():
            days = str(int(float(str(days_val).strip())))
    except Exception:
        days = ''
    if not days and dt_start and dt_end:
        try:
            delta = (dt_end.date() - dt_start.date()).days + 1
            days = str(max(1, delta))
        except Exception:
            days = ''
    if days:
        draw_text(1010, 595, days, size=11, align='right')

    CB_GOV = (460, 621)
    CB_PRIVATE = (461, 666)
    CB_TAXI = (830, 619)
    CB_HSR = (1005, 618)
    CB_AIR = (1121, 617)
    CB_DISPATCH = (1005, 663)
    CB_OTHER = (1122, 663)

    if _truthy(record.get('is_gov_car')):
        _draw_mark_rect(c, *CB_GOV)
        draw_text(646, 638, record.get('gov_car_no', ''), size=11)

    if _truthy(record.get('is_private_car')):
        _draw_mark_rect(c, *CB_PRIVATE)
        km = record.get('private_car_km', '')
        km_s = ''
        try:
            km_s = str(int(float(km))) if str(km).strip() else ''
        except Exception:
            km_s = str(km).strip()
        if km_s:
            draw_text(703, 684, km_s, size=11)
        draw_text(849, 684, record.get('private_car_no', ''), size=11)

    if _truthy(record.get('is_taxi')):
        _draw_mark_rect(c, *CB_TAXI)
    if _truthy(record.get('is_hsr')):
        _draw_mark_rect(c, *CB_HSR)
    if _truthy(record.get('is_airplane')):
        _draw_mark_rect(c, *CB_AIR)
    if _truthy(record.get('is_dispatch_car')):
        _draw_mark_rect(c, *CB_DISPATCH)
    if _truthy(record.get('is_other_transport')):
        _draw_mark_rect(c, *CB_OTHER)
        draw_text(1240, 684, record.get('other_transport_desc', ''), size=11)

    est = _safe_float(record.get('estimated_cost'), 0.0)
    if est:
        draw_text(460, 750, f'{est:.0f}', size=12)

    rows = record.get('expense_rows', []) or []
    if not isinstance(rows, list):
        rows = []

    X_LEFT = 147
    X_MONTH = 218
    X_DAY = 294
    X_ROUTE = 541
    X_VEH = 688
    X_TRANSPORT = 836
    X_PERDIEM = 965
    X_ACCOM = 1052
    X_OTHER = 1185
    X_RECEIPT = 1185
    Y_LINES = [1112, 1175, 1238, 1295, 1358, 1422, 1484, 1542, 1605, 1668, 1730]

    def cell_center(xa: int, xb: int) -> float:
        return (xa + xb) / 2

    total_t = 0.0
    total_p = 0.0
    total_a = 0.0
    total_o = 0.0

    for i in range(min(10, len(rows))):
        r = rows[i] or {}
        y_mid = (Y_LINES[i] + Y_LINES[i + 1]) / 2
        y_text = y_mid + 5

        md = str(r.get('date_md', '') or '').strip()
        if md:
            md = md.split('T')[0].split(' ')[0]
        mm = ''
        dd = ''
        if md:
            parts = md.replace('-', '/').split('/')
            if len(parts) >= 3 and parts[0].isdigit() and len(parts[0]) == 4:
                mm = parts[1].zfill(2) if parts[1].isdigit() else parts[1]
                dd = parts[2].zfill(2) if parts[2].isdigit() else parts[2]
            elif len(parts) >= 2:
                mm = parts[0].zfill(2) if parts[0].isdigit() else parts[0]
                dd = parts[1].zfill(2) if parts[1].isdigit() else parts[1]

        if mm:
            draw_text(cell_center(X_LEFT, X_MONTH), y_text, mm, size=11, align='center')
        if dd:
            draw_text(cell_center(X_MONTH, X_DAY), y_text, dd, size=11, align='center')

        draw_text(X_DAY + 8, y_text, r.get('route', ''), size=11)
        draw_text(cell_center(X_ROUTE, X_VEH), y_text, r.get('transport_type', ''), size=11, align='center')

        t_amt = _safe_float(r.get('transport_amt'), 0.0)
        p_amt = _safe_float(r.get('per_diem_amt'), 0.0)
        a_amt = _safe_float(r.get('accommodation_amt'), 0.0)
        o_amt = _safe_float(r.get('other_amt'), 0.0)

        if t_amt:
            draw_text(X_TRANSPORT - 8, y_text, f'{t_amt:.0f}', size=11, align='right')
        if p_amt:
            draw_text(X_PERDIEM - 8, y_text, f'{p_amt:.0f}', size=11, align='right')
        if a_amt:
            draw_text(X_ACCOM - 8, y_text, f'{a_amt:.0f}', size=11, align='right')
        if o_amt:
            draw_text(X_OTHER - 8, y_text, f'{o_amt:.0f}', size=11, align='right')

        draw_text(X_RECEIPT + 8, y_text, r.get('receipt_no', ''), size=10)

        total_t += t_amt
        total_p += p_amt
        total_a += a_amt
        total_o += o_amt

    y_sum = (1730 + 1793) / 2 + 5
    if total_t:
        draw_text(X_TRANSPORT - 8, y_sum, f'{total_t:.0f}', size=11, align='right')
    if total_p:
        draw_text(X_PERDIEM - 8, y_sum, f'{total_p:.0f}', size=11, align='right')
    if total_a:
        draw_text(X_ACCOM - 8, y_sum, f'{total_a:.0f}', size=11, align='right')
    if total_o:
        draw_text(X_OTHER - 8, y_sum, f'{total_o:.0f}', size=11, align='right')

    total_all = total_t + total_p + total_a + total_o
    digits = _amount_to_digit_boxes(total_all)
    for x_px, ch in zip(TOTAL_AMOUNT_DIGIT_X_PX, digits):
        draw_text(x_px, TOTAL_AMOUNT_DIGIT_Y_PX + 10, ch, size=13, align='center')

    c.showPage()
    c.save()

    base_pdf = buf.getvalue()
    merged_paths = _extract_attachment_paths(record, attachment_paths)
    return _merge_attachments(base_pdf, merged_paths)


def generate_pdf_travel(record: Dict[str, Any], template_path: Optional[str], output_path: str) -> bool:
    """Compatibility wrapper for apps/travel.py old interface."""
    try:
        pdf_bytes = build_pdf_bytes(record, bg_image_path=template_path)
        out_dir = os.path.dirname(output_path)
        if out_dir:
            os.makedirs(out_dir, exist_ok=True)
        with open(output_path, 'wb') as f:
            f.write(pdf_bytes)
        return True
    except Exception:
        return False
