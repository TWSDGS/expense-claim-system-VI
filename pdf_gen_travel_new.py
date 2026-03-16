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
BG_W_PX = 1448
BG_H_PX = 2048
SCALE = PAGE_W / BG_W_PX


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


def _draw_text(c, text, x_px, y_px, size=12, font=None):
    if not text:
        return
    font = font or _font()
    x, y = px_to_pt(x_px, y_px)
    c.setFont(font, size)
    c.drawString(x, y, str(text))


def _draw_center(c, text, x_px, y_px, size=12, font=None):
    if not text:
        return
    font = font or _font()
    x, y = px_to_pt(x_px, y_px)
    c.setFont(font, size)
    c.drawCentredString(x, y, str(text))


def _draw_wrap(c, text, left_px, top_px, width_px, line_h_px, max_lines=2, size=12, font=None):
    if not text:
        return
    font = font or _font()
    max_w = width_px * SCALE
    lines=[]; buf=''
    for ch in str(text).replace('\r',''):
        if ch=='\n':
            lines.append(buf); buf=''; continue
        if pdfmetrics.stringWidth(buf+ch, font, size) <= max_w:
            buf += ch
        else:
            lines.append(buf); buf = ch
        if len(lines) >= max_lines:
            break
    if buf and len(lines)<max_lines:
        lines.append(buf)
    c.setFont(font,size)
    for i,line in enumerate(lines[:max_lines]):
        x,y = px_to_pt(left_px, top_px + i*line_h_px)
        c.drawString(x,y,line)


def _draw_box(c, x1, y1, x2, y2, lw=0.9):
    x1p, y1p = px_to_pt(x1, y1)
    x2p, y2p = px_to_pt(x2, y2)
    left=min(x1p,x2p); bottom=min(y1p,y2p); width=abs(x2p-x1p); height=abs(y2p-y1p)
    c.setLineWidth(lw)
    c.rect(left,bottom,width,height,stroke=1,fill=0)


def _line(c, x1, y1, x2, y2, lw=0.9):
    a,b = px_to_pt(x1,y1); c1,d = px_to_pt(x2,y2)
    c.setLineWidth(lw); c.line(a,b,c1,d)


def _extract_attachment_paths(record: Dict[str, Any], attachment_paths=None) -> List[str]:
    out=[]
    src = attachment_paths if attachment_paths is not None else record.get('attachment_files') or record.get('attachments') or []
    if isinstance(src,str):
        try:
            j=json.loads(src); src=j if isinstance(j,list) else [src]
        except Exception:
            src=[src]
    for item in src:
        if isinstance(item,dict):
            p=_safe(item.get('path'))
            if p: out.append(p)
        elif isinstance(item,str):
            p=_safe(item)
            if p: out.append(p)
    return out


def _image_grid_pdf_bytes(image_paths: List[str]) -> bytes:
    buf = io.BytesIO(); c = canvas.Canvas(buf,pagesize=A4)
    margin=28; gap=12; usable_w=PAGE_W-2*margin; usable_h=PAGE_H-2*margin; cols=2; rows=2
    cell_w=(usable_w-gap)/cols; cell_h=(usable_h-gap)/rows; per_page=cols*rows
    for idx,path in enumerate(image_paths):
        if idx and idx%per_page==0: c.showPage()
        slot=idx%per_page; col=slot%cols; row=slot//cols
        x=margin+col*(cell_w+gap); y=PAGE_H-margin-(row+1)*cell_h-row*gap
        try:
            with Image.open(path) as im: iw,ih=im.size
        except Exception:
            continue
        ratio=iw/ih if ih else 1; box=cell_w/cell_h if cell_h else 1
        if ratio>=box: dw=cell_w; dh=cell_w/ratio
        else: dh=cell_h; dw=cell_h*ratio
        dx=x+(cell_w-dw)/2; dy=y+(cell_h-dh)/2
        c.drawImage(ImageReader(path),dx,dy,width=dw,height=dh,preserveAspectRatio=True,mask='auto')
    c.save(); return buf.getvalue()


def _merge_attachments(base_pdf: bytes, paths: List[str]) -> bytes:
    if not paths or not PdfReader or not PdfWriter:
        return base_pdf
    writer=PdfWriter()
    for p in PdfReader(io.BytesIO(base_pdf)).pages: writer.add_page(p)
    imgs=[]
    def flush():
        nonlocal imgs
        if not imgs: return
        for p in PdfReader(io.BytesIO(_image_grid_pdf_bytes(imgs))).pages: writer.add_page(p)
        imgs=[]
    for pth in paths:
        if not pth or not os.path.exists(pth): continue
        low=pth.lower()
        try:
            if low.endswith('.pdf'):
                flush()
                for p in PdfReader(pth).pages: writer.add_page(p)
            elif low.endswith(('.png','.jpg','.jpeg','.webp','.bmp')):
                imgs.append(pth)
        except Exception:
            continue
    flush(); out=io.BytesIO(); writer.write(out); return out.getvalue()


def _roc(date_str: str):
    m = re.match(r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})', _safe(date_str))
    if not m: return '','',''
    y,mo,d=m.groups(); return str(int(y)-1911), f'{int(mo):02d}', f'{int(d):02d}'


def _form_id(record: Dict[str, Any]) -> str:
    rid=_safe(record.get('record_id'))
    if rid: return rid
    emp=_safe(record.get('employee_no')) or '00000'
    y,m,d=_roc(_safe(record.get('form_date') or record.get('trip_date_start')))
    roc=f'{int(y):03d}{m}{d}' if y else '0000000'
    return f'TR{emp}{roc}001'


def _norm_details(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    src = record.get('expense_rows') or record.get('detail_json') or record.get('details') or []
    if isinstance(src,str):
        try:
            src=json.loads(src)
        except Exception:
            src=[]
    out=[]
    for row in src or []:
        if not isinstance(row,dict):
            continue
        date_s = _safe(row.get('日期') or row.get('date') or row.get('trip_date') or record.get('trip_date_start'))
        mo=day=''
        m = re.match(r'\d{4}[-/](\d{1,2})[-/](\d{1,2})', date_s)
        if m: mo,day = f'{int(m.group(1)):02d}', f'{int(m.group(2)):02d}'
        out.append({
            'mo': mo,
            'day': day,
            'route': _safe(row.get('起訖地點') or row.get('route')),
            'vehicle': _safe(row.get('車別') or row.get('vehicle_type')),
            'transport_fee': _to_int(row.get('交通費') or row.get('transport_fee')),
            'misc_fee': _to_int(row.get('膳雜費') or row.get('misc_fee') or row.get('meal_fee')),
            'lodging_fee': _to_int(row.get('住宿費') or row.get('lodging_fee')),
            'other_fee': _to_int(row.get('其它') or row.get('other_fee')),
            'receipt': _safe(row.get('單據編號') or row.get('receipt_no')),
        })
    if not out:
        out=[{'mo':'','day':'','route':'','vehicle':'','transport_fee':0,'misc_fee':0,'lodging_fee':0,'other_fee':0,'receipt':''}]
    return out


def build_pdf_bytes(record: Dict[str, Any], attachment_paths=None) -> bytes:
    f=_font(); buf=io.BytesIO(); c=canvas.Canvas(buf,pagesize=A4)

    # title + header
    _draw_center(c, '財團法人安全衛生技術中心', 724, 118, 18, f)
    _draw_center(c, '國內出差申請單', 724, 170, 18, f)
    _draw_text(c, '（生效日期：20110101）', 1158, 174, 9, f)
    _draw_text(c, '填寫日期：', 1040, 220, 12, f)
    y,m,d = _roc(record.get('form_date',''))
    _draw_center(c, y, 1240, 220, 14, f); _draw_center(c, m, 1310, 220, 14, f); _draw_center(c, d, 1370, 220, 14, f)

    # main top table geometry approximated to scan
    left, top, right = 124, 304, 1342
    ys = [304, 395, 484, 573, 664, 756, 941, 1129, 1213, 1808, 1894, 2034]
    for ypx in ys:
        _line(c,left,ypx,right,ypx)
    _line(c,left,304,left,2034); _line(c,right,304,right,2034)
    # top rows internal verticals
    for xpx in [438, 812, 930]:
        _line(c,xpx,304,xpx,395)
    _line(c,438,395,438,484)
    _line(c,438,484,438,573)
    _line(c,438,573,438,664)
    _line(c,438,664,438,756)
    _line(c,438,756,438,941)
    for xpx in [732, 1035, right]:
        pass
    # trip date block verticals
    for xpx in [466, 546, 635, 717, 804, 893, 973, 1062]:
        _line(c,xpx,573,xpx,664)
    # approval row verticals
    for xpx in [730, 1038]:
        _line(c,xpx,941,xpx,1129)

    # detail table verticals
    for xpx in [194, 272, 531, 682, 835, 984, 1134, 1274]:
        _line(c,xpx,1213,xpx,1808)
    # detail horizontal rows (12 rows)
    y=1296
    while y < 1808:
        _line(c,124,y,right,y,0.7); y += 70

    # labels
    labels=[
        (280,350,'出      差      人'), (870,350,'計  畫\n代  號'), (280,440,'出   差   事   由'),
        (280,530,'出 差 往 返 地 點'), (280,615,'出   差   日   期'), (280,710,'特  殊  交  通  工  具'),
        (280,850,'出 差 費 預 估'), (280,1035,'出 差 單 簽 核 順 序：'),
    ]
    for x,yv,t in labels:
        for i,part in enumerate(t.split('\n')):
            _draw_center(c, part, x, yv + i*24, 13, f)

    # values
    _draw_text(c, _safe(record.get('employee_name') or record.get('traveler')), 470, 352, 13, f)
    _draw_text(c, _safe(record.get('plan_code') or record.get('project_id')), 970, 352, 13, f)
    _draw_wrap(c, _safe(record.get('trip_purpose') or record.get('purpose')), 460, 438, 820, 22, 2, 12, f)
    _draw_wrap(c, f"{_safe(record.get('from_location') or record.get('departure_location'))} 至 {_safe(record.get('to_location') or record.get('destination_location'))}".strip(' 至 '), 460, 527, 820, 22, 2, 12, f)

    sy,sm,sd = _roc(record.get('trip_date_start','')); ey,em,ed = _roc(record.get('trip_date_end',''))
    _draw_center(c, sy, 490, 610, 12, f); _draw_center(c, sm, 631, 610, 12, f); _draw_center(c, sd, 721, 610, 12, f)
    _draw_center(c, _safe(record.get('trip_time_start') or '09'), 813, 610, 12, f)
    _draw_center(c, ey, 490, 645, 12, f); _draw_center(c, em, 631, 645, 12, f); _draw_center(c, ed, 721, 645, 12, f)
    _draw_center(c, _safe(record.get('trip_time_end') or '18'), 813, 645, 12, f)
    days = _safe(record.get('trip_days') or '1')
    _draw_center(c, days, 1038, 645, 12, f)

    # transport options row text
    opts = record.get('transport_tools') or record.get('transport_mode') or record.get('transport_options') or []
    if isinstance(opts,str):
        try:
            parsed=json.loads(opts); opts = parsed if isinstance(parsed,list) else [x.strip() for x in opts.split(',') if x.strip()]
        except Exception:
            opts = [x.strip() for x in opts.split(',') if x.strip()]
    opts = set(opts)
    def mark(text, chosen):
        return ('■' if chosen else '□') + text
    _draw_text(c, mark('公務車(車號__________)', '公務車' in opts), 470, 706, 12, f)
    _draw_text(c, mark('計程車', '計程車' in opts), 860, 706, 12, f)
    _draw_text(c, mark('高鐵', '高鐵' in opts), 1050, 706, 12, f)
    _draw_text(c, mark('飛機', '飛機' in opts), 1190, 706, 12, f)
    _draw_text(c, mark('私車公用(公里數____ 車號________)', '私車公用' in opts), 470, 756, 12, f)
    _draw_text(c, mark('派車', '派車' in opts), 1050, 756, 12, f)
    _draw_text(c, mark('其他(      )', '其他' in opts), 1170, 756, 12, f)
    _draw_text(c, _safe(record.get('gov_car_no')), 665, 706, 11, f)
    _draw_text(c, _safe(record.get('private_car_km')), 640, 756, 11, f)
    _draw_text(c, _safe(record.get('private_car_no')), 820, 756, 11, f)
    _draw_text(c, _safe(record.get('other_transport_desc') or record.get('other_transport')), 1250, 756, 11, f)

    # approval names blank except labels
    _draw_center(c, '出差人', 584, 1020, 14, f)
    _draw_center(c, '計畫主持人', 882, 1020, 14, f)
    _draw_center(c, '部門主管', 1188, 1020, 14, f)

    # detail section title/labels
    _draw_center(c, '差  旅  費  報  支  單', 724, 1178, 18, f)
    _draw_center(c, '日期', 198, 1265, 13, f)
    _draw_center(c, '交通費', 614, 1265, 13, f)
    _draw_center(c, '膳雜費', 910, 1265, 13, f)
    _draw_center(c, '住宿費', 1060, 1265, 13, f)
    _draw_center(c, '其它', 1204, 1265, 13, f)
    _draw_center(c, '單據\n編號', 1310, 1265, 13, f)
    _draw_center(c, '月', 159, 1326, 12, f); _draw_center(c, '日', 232, 1326, 12, f)
    _draw_center(c, '起訖地點', 403, 1326, 12, f); _draw_center(c, '車別', 607, 1326, 12, f); _draw_center(c, '金額', 758, 1326, 12, f)

    details = _norm_details(record)
    row_y = 1400
    max_rows = 7
    for row in details[:max_rows]:
        _draw_center(c, row['mo'], 159, row_y, 12, f)
        _draw_center(c, row['day'], 232, row_y, 12, f)
        _draw_text(c, row['route'], 292, row_y, 11, f)
        _draw_text(c, row['vehicle'], 547, row_y, 11, f)
        _draw_center(c, str(row['transport_fee']), 758, row_y, 11, f)
        _draw_center(c, str(row['misc_fee']), 910, row_y, 11, f)
        _draw_center(c, str(row['lodging_fee']), 1060, row_y, 11, f)
        _draw_center(c, str(row['other_fee']), 1204, row_y, 11, f)
        _draw_text(c, row['receipt'], 1288, row_y, 10, f)
        row_y += 70

    transport_total = sum(r['transport_fee'] for r in details)
    misc_total = sum(r['misc_fee'] for r in details)
    lodging_total = sum(r['lodging_fee'] for r in details)
    other_total = sum(r['other_fee'] for r in details)
    total = transport_total + misc_total + lodging_total + other_total
    _draw_text(c, '合計', 140, 1848, 13, f)
    _draw_center(c, str(transport_total), 758, 1848, 11, f)
    _draw_center(c, str(misc_total), 910, 1848, 11, f)
    _draw_center(c, str(lodging_total), 1060, 1848, 11, f)
    _draw_center(c, str(other_total), 1204, 1848, 11, f)

    digits=list(str(total).zfill(6)[-6:])
    # bottom amount line
    _draw_text(c, '總計 新台幣', 150, 1938, 13, f)
    for x,ch in zip([527, 651, 776, 899, 1026, 1150], digits):
        _draw_center(c, ch, x, 1938, 13, f)
    _draw_text(c, '元整', 1230, 1938, 13, f)
    _draw_text(c, '出差人', 160, 2016, 13, f)
    _draw_text(c, '計畫主持人', 530, 2016, 13, f)
    _draw_text(c, '部門主管', 860, 2016, 13, f)
    _draw_text(c, '管理處會計', 1160, 2016, 13, f)

    c.save(); base = buf.getvalue()
    return _merge_attachments(base, _extract_attachment_paths(record, attachment_paths))
