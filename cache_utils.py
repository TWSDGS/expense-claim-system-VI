from __future__ import annotations

import json
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
ATTACHMENTS_DIR = CACHE_DIR / "attachments"
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
PENDING_QUEUE_FILE = CACHE_DIR / "pending_sync_queue.json"
SIGNATURES_DIR = CACHE_DIR / "signatures"
SIGNATURES_DIR.mkdir(parents=True, exist_ok=True)
DELETED_ARCHIVE_JSON = CACHE_DIR / "deleted_archive.json"
DELETED_ARCHIVE_XLSX = CACHE_DIR / "deleted_archive.xlsx"
AUDIT_LOG_JSON = CACHE_DIR / "sync_audit_log.json"


def _cache_path(filename: str) -> Path:
    return CACHE_DIR / filename


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _queue_scope_key(email: Optional[str] = None) -> str:
    email = str(email or "").strip().lower()
    if not email:
        return "global"
    safe = re.sub(r"[^a-z0-9._-]+", "_", email)
    return safe or "global"


def _pending_queue_path(email: Optional[str] = None) -> Path:
    scope = _queue_scope_key(email)
    if scope == "global":
        return PENDING_QUEUE_FILE
    return CACHE_DIR / f"pending_sync_queue__{scope}.json"




def _snapshot_filename(system_type: str, owner_email: Optional[str] = None) -> str:
    return f"master_snapshot__{str(system_type or '').strip().lower()}__{_queue_scope_key(owner_email)}.json"


def save_master_snapshot(system_type: str, owner_email: Optional[str], rows: List[Dict[str, Any]]) -> None:
    save_json_cache(_snapshot_filename(system_type, owner_email), rows or [])


def load_master_snapshot(system_type: str, owner_email: Optional[str]) -> List[Dict[str, Any]]:
    rows = load_json_cache(_snapshot_filename(system_type, owner_email), default=[])
    return rows if isinstance(rows, list) else []


def append_sync_audit(event: Dict[str, Any]) -> None:
    rows = load_json_cache(AUDIT_LOG_JSON.name, default=[])
    if not isinstance(rows, list):
        rows = []
    entry = dict(event or {})
    entry.setdefault('logged_at', datetime.now().isoformat(timespec='seconds'))
    rows.append(entry)
    if len(rows) > 5000:
        rows = rows[-5000:]
    save_json_cache(AUDIT_LOG_JSON.name, rows)

def save_json_cache(filename: str, data: Any) -> None:
    path = _cache_path(filename)
    _atomic_write_json(path, data)


def load_json_cache(filename: str, default: Any = None) -> Any:
    path = _cache_path(filename)
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_users_cache(rows: List[Dict[str, Any]]) -> None:
    save_json_cache("users_cache.json", rows)


def load_users_cache() -> List[Dict[str, Any]]:
    return load_json_cache("users_cache.json", default=[])


def save_options_cache(rows: List[Dict[str, Any]]) -> None:
    save_json_cache("options_cache.json", rows)


def load_options_cache() -> List[Dict[str, Any]]:
    return load_json_cache("options_cache.json", default=[])


def save_user_defaults_cache(rows: List[Dict[str, Any]]) -> None:
    save_json_cache("user_defaults_cache.json", rows)


def load_user_defaults_cache() -> List[Dict[str, Any]]:
    return load_json_cache("user_defaults_cache.json", default=[])


def save_cloud_backup_excel(
    dataframes: Dict[str, pd.DataFrame],
    filename: str = "cloud_backup.xlsx",
) -> Path:
    path = _cache_path(filename)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for sheet_name, df in dataframes.items():
            safe_df = df.copy() if df is not None else pd.DataFrame()
            safe_df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    return path




def _archive_identity(row: Dict[str, Any]) -> str:
    record_id = str((row or {}).get("record_id", "")).strip()
    archived_at = str((row or {}).get("archived_at", "")).strip()
    system_type = str((row or {}).get("archive_system_type", "")).strip().lower()
    return f"{system_type}::{record_id}::{archived_at}"


def archive_deleted_record(record: Dict[str, Any], system_type: str = "unknown", actor_email: str = "") -> None:
    row = dict(record or {})
    row["archive_system_type"] = str(system_type or "unknown")
    row["archive_actor_email"] = str(actor_email or "").strip().lower()
    row["archived_at"] = datetime.now().isoformat(timespec="seconds")
    row.setdefault("archive_restored", False)
    row.setdefault("restored_at", "")
    row.setdefault("restored_by", "")
    row.setdefault("restore_target_status", "")
    row["archive_id"] = _archive_identity(row)
    rows = load_json_cache(DELETED_ARCHIVE_JSON.name, default=[])
    if not isinstance(rows, list):
        rows = []
    rows.append(row)
    save_json_cache(DELETED_ARCHIVE_JSON.name, rows)
    try:
        df = pd.DataFrame(rows)
        with pd.ExcelWriter(DELETED_ARCHIVE_XLSX, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="deleted_archive", index=False)
    except Exception:
        pass


def load_deleted_archive_rows(system_type: Optional[str] = None, include_restored: bool = False) -> List[Dict[str, Any]]:
    rows = load_json_cache(DELETED_ARCHIVE_JSON.name, default=[])
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        r = dict(row)
        r.setdefault("archive_id", _archive_identity(r))
        r.setdefault("archive_restored", False)
        r.setdefault("restored_at", "")
        r.setdefault("restored_by", "")
        r.setdefault("restore_target_status", "")
        if system_type and str(r.get("archive_system_type", "")).strip().lower() != str(system_type).strip().lower():
            continue
        if (not include_restored) and bool(r.get("archive_restored")):
            continue
        out.append(r)
    out.sort(key=lambda x: str(x.get("archived_at", "")), reverse=True)
    return out


def mark_deleted_archive_restored(archive_id: str, restored_by: str = "", restore_target_status: str = "") -> bool:
    archive_id = str(archive_id or "").strip()
    if not archive_id:
        return False
    rows = load_json_cache(DELETED_ARCHIVE_JSON.name, default=[])
    if not isinstance(rows, list):
        return False
    changed = False
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        current = dict(row)
        current.setdefault("archive_id", _archive_identity(current))
        if str(current.get("archive_id", "")).strip() != archive_id:
            rows[i] = current
            continue
        current["archive_restored"] = True
        current["restored_at"] = datetime.now().isoformat(timespec="seconds")
        current["restored_by"] = str(restored_by or "").strip().lower()
        current["restore_target_status"] = str(restore_target_status or "").strip().lower()
        rows[i] = current
        changed = True
        break
    if changed:
        save_json_cache(DELETED_ARCHIVE_JSON.name, rows)
        try:
            df = pd.DataFrame(rows)
            with pd.ExcelWriter(DELETED_ARCHIVE_XLSX, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name="deleted_archive", index=False)
        except Exception:
            pass
    return changed

def load_backup_sheet_df(sheet_name: str, filename: str = "cloud_backup.xlsx") -> pd.DataFrame:
    path = _cache_path(filename)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def get_user_defaults_from_cache(email: str) -> Dict[str, Any]:
    email = str(email or "").strip().lower()
    rows = load_user_defaults_cache()
    for row in rows:
        if str(row.get("email", "")).strip().lower() == email:
            return row
    return {}


def filter_options_from_cache(option_type: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = load_options_cache()
    if option_type:
        return [r for r in rows if str(r.get("option_type", "")).strip() == option_type]
    return rows


def ensure_record_attachment_dir(record_key: str) -> Path:
    record_key = str(record_key or "temp").strip() or "temp"
    path = ATTACHMENTS_DIR / record_key
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_uploaded_attachments(record_key: str, uploaded_files: List[Any]) -> List[Dict[str, Any]]:
    record_dir = ensure_record_attachment_dir(record_key)
    manifests: List[Dict[str, Any]] = []
    for idx, file_obj in enumerate(uploaded_files or []):
        original_name = Path(getattr(file_obj, 'name', f'attachment_{idx+1}')).name
        target_name = f"{idx+1:02d}_{original_name}"
        target = record_dir / target_name
        target.write_bytes(file_obj.getvalue())
        manifests.append({
            'name': original_name,
            'saved_name': target_name,
            'path': str(target),
            'mime_type': getattr(file_obj, 'type', ''),
            'size': target.stat().st_size,
        })
    return manifests


def load_attachment_manifest(record_key: str) -> List[Dict[str, Any]]:
    manifest_path = ensure_record_attachment_dir(record_key) / 'manifest.json'
    if not manifest_path.exists():
        return []
    try:
        return json.loads(manifest_path.read_text(encoding='utf-8'))
    except Exception:
        return []


def save_attachment_manifest(record_key: str, manifest: List[Dict[str, Any]]) -> None:
    manifest_path = ensure_record_attachment_dir(record_key) / 'manifest.json'
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')


def remove_record_attachments(record_key: str) -> None:
    target = ATTACHMENTS_DIR / str(record_key or '').strip()
    if target.exists() and target.is_dir():
        shutil.rmtree(target, ignore_errors=True)


def queue_pending_sync(operation: str, actor: Dict[str, Any], payload: Dict[str, Any], queue_owner_email: Optional[str] = None) -> None:
    owner_email = str(queue_owner_email or actor.get("email") or payload.get("user_email") or "").strip().lower()
    queue = load_pending_sync_queue(owner_email)
    payload = dict(payload or {})
    record_id = str(payload.get('record_id') or '').strip()
    queued_at = datetime.now().isoformat(timespec='seconds')
    event_id = str(payload.get('event_id') or uuid.uuid4().hex).strip()
    payload['event_id'] = event_id
    if payload.get('expected_version') is None and payload.get('base_version') is None:
        ver_raw = payload.get('version')
        try:
            if ver_raw not in (None, ''):
                ver_num = int(ver_raw)
                payload['expected_version'] = ver_num
                payload['base_version'] = ver_num
        except Exception:
            pass
    item = {
        'event_id': event_id,
        'operation': operation,
        'actor': actor,
        'payload': payload,
        'queued_at': queued_at,
        'queue_owner_email': owner_email,
    }
    replaced = False
    for i, existing in enumerate(queue):
        existing_payload = existing.get('payload') or {}
        existing_record_id = str(existing_payload.get('record_id') or '').strip()
        existing_owner_email = str(existing.get('queue_owner_email') or (existing.get('actor') or {}).get('email') or existing_payload.get('user_email') or '').strip().lower()
        if record_id and existing_record_id == record_id and existing_owner_email == owner_email:
            queue[i] = item
            replaced = True
            break
    if not replaced:
        queue.append(item)
    save_pending_sync_queue(queue, owner_email)


def load_pending_sync_queue(email: Optional[str] = None) -> List[Dict[str, Any]]:
    path = _pending_queue_path(email)
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return []


def save_pending_sync_queue(queue: List[Dict[str, Any]], email: Optional[str] = None) -> None:
    path = _pending_queue_path(email)
    _atomic_write_json(path, queue)


def remove_pending_sync_item(owner_email: str, event_id: str = '', record_id: str = '', system_type: Optional[str] = None) -> int:
    queue = load_pending_sync_queue(owner_email)
    newq = []
    removed = 0
    for item in queue:
        payload = dict(item.get('payload') or {})
        item_event = str(item.get('event_id') or payload.get('event_id') or '').strip()
        item_record = str(payload.get('record_id') or '').strip()
        item_system = payload.get('system_type') or ('travel' if 'travel' in str(item.get('operation', '')).lower() else 'expense')
        matched = False
        if event_id and item_event == str(event_id).strip():
            matched = True
        elif record_id and item_record == str(record_id).strip() and (not system_type or item_system == system_type):
            matched = True
        if matched:
            removed += 1
            continue
        newq.append(item)
    if removed:
        save_pending_sync_queue(newq, owner_email)
    return removed


def update_pending_sync_item(owner_email: str, event_id: str, new_item: Dict[str, Any]) -> bool:
    queue = load_pending_sync_queue(owner_email)
    changed = False
    for i, item in enumerate(queue):
        payload = dict(item.get('payload') or {})
        item_event = str(item.get('event_id') or payload.get('event_id') or '').strip()
        if item_event == str(event_id).strip():
            queue[i] = new_item
            changed = True
            break
    if changed:
        save_pending_sync_queue(queue, owner_email)
    return changed


def list_pending_conflicts(owner_email: str, system_type: Optional[str] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in load_pending_sync_queue(owner_email):
        payload = dict(item.get('payload') or {})
        item_system = payload.get('system_type') or ('travel' if 'travel' in str(item.get('operation', '')).lower() else 'expense')
        if system_type and item_system != system_type:
            continue
        status = str(payload.get('sync_status') or '').strip().lower()
        last_error = str(item.get('last_error') or payload.get('sync_message') or '')
        if status == 'conflict' or 'VERSION_CONFLICT' in last_error or payload.get('sync_conflict'):
            rows.append(item)
    return rows


def save_signature_file(owner_email: str, uploaded_file: Any) -> Dict[str, Any]:
    owner_key = _queue_scope_key(owner_email)
    target_dir = SIGNATURES_DIR / owner_key
    target_dir.mkdir(parents=True, exist_ok=True)
    original_name = Path(getattr(uploaded_file, 'name', 'signature.png')).name
    ext = Path(original_name).suffix.lower() or '.png'
    target = target_dir / f'signature{ext}'
    target.write_bytes(uploaded_file.getvalue())
    manifest = {
        'name': original_name,
        'path': str(target),
        'mime_type': getattr(uploaded_file, 'type', ''),
        'size': target.stat().st_size,
        'updated_at': datetime.now().isoformat(timespec='seconds'),
    }
    _atomic_write_json(target_dir / 'manifest.json', manifest)
    return manifest


def load_signature_file(owner_email: str) -> Dict[str, Any]:
    owner_key = _queue_scope_key(owner_email)
    manifest_path = SIGNATURES_DIR / owner_key / 'manifest.json'
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding='utf-8'))
    except Exception:
        return {}


# ===== Added compatibility/local record helpers =====
EXPENSE_DRAFTS_FILE = CACHE_DIR / "expense_drafts.json"
TRAVEL_RECORDS_FILE = CACHE_DIR / "travel_records.json"


def _read_json_list(path: Path) -> list:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _write_json_list(path: Path, rows: list) -> None:
    _atomic_write_json(path, rows)


def _roc_ymd_from_date(value: str | None = None) -> str:
    raw = str(value or '').strip().replace('/', '-')
    try:
        dt = datetime.fromisoformat(raw).date() if raw else datetime.now().date()
    except Exception:
        dt = datetime.now().date()
    roc_year = dt.year - 1911
    return f"{roc_year:03d}{dt.month:02d}{dt.day:02d}"


def _next_prefixed_id(prefix: str, employee_no: str, form_date: str, existing_ids: list[str]) -> str:
    emp = ''.join(ch for ch in str(employee_no or '') if ch.isdigit()) or '00000'
    ymd = _roc_ymd_from_date(form_date)
    base = f"{prefix}{emp}{ymd}"
    max_seq = 0
    for rid in existing_ids:
        rid = str(rid or '').strip()
        if rid.startswith(base):
            tail = rid[len(base):]
            if tail.isdigit():
                max_seq = max(max_seq, int(tail))
    return f"{base}{max_seq + 1:03d}"


def save_uploaded_attachment(owner_email: str, uploaded_file: Any, category: str = "attachment") -> Dict[str, Any]:
    owner_key = _queue_scope_key(owner_email)
    target_dir = ATTACHMENTS_DIR / owner_key / category
    target_dir.mkdir(parents=True, exist_ok=True)
    original_name = Path(getattr(uploaded_file, "name", f"{category}.bin")).name
    target = target_dir / f"{datetime.now().strftime('%Y%m%d%H%M%S%f')}_{original_name}"
    target.write_bytes(uploaded_file.getvalue())
    return {
        "name": original_name,
        "path": str(target),
        "mime_type": getattr(uploaded_file, "type", ""),
        "size": target.stat().st_size,
        "category": category,
        "owner_email": str(owner_email or "").strip().lower(),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def delete_saved_file(meta: Dict[str, Any]) -> None:
    try:
        path = Path(str((meta or {}).get("path", "")))
        if path.exists() and path.is_file():
            path.unlink()
    except Exception:
        pass


def load_local_expense_drafts(email: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _read_json_list(EXPENSE_DRAFTS_FILE)
    email = str(email or "").strip().lower()
    if not email:
        return rows
    return [r for r in rows if str(r.get("user_email") or r.get("owner_email") or "").strip().lower() == email]


def upsert_local_expense_draft(email: str, payload: Dict[str, Any]) -> str:
    rows = _read_json_list(EXPENSE_DRAFTS_FILE)
    email = str(email or payload.get("user_email") or "").strip().lower()
    payload = dict(payload)
    record_id = str(payload.get("record_id") or "").strip()
    if not record_id:
        existing_ids = [str(r.get("record_id") or "") for r in rows]
        record_id = _next_prefixed_id("EX", payload.get("employee_no") or email, payload.get("form_date"), existing_ids)
    payload["record_id"] = record_id
    payload["status"] = str(payload.get("status") or "draft")
    payload["user_email"] = email
    payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
    replaced = False
    for i, row in enumerate(rows):
        if str(row.get("record_id") or "") == record_id and str(row.get("user_email") or "").strip().lower() == email:
            rows[i] = payload
            replaced = True
            break
    if not replaced:
        rows.append(payload)
    _write_json_list(EXPENSE_DRAFTS_FILE, rows)
    return record_id


def remove_local_expense_draft(email: str, record_id: str, mark_deleted: bool = False) -> None:
    rows = _read_json_list(EXPENSE_DRAFTS_FILE)
    email = str(email or "").strip().lower()
    out = []
    for row in rows:
        same = str(row.get("record_id") or "") == str(record_id or "") and str(row.get("user_email") or "").strip().lower() == email
        if same:
            if mark_deleted:
                row = dict(row)
                row["status"] = "deleted"
                row["deleted_at"] = datetime.now().isoformat(timespec="seconds")
                out.append(row)
            if not mark_deleted:
                continue
        else:
            out.append(row)
    _write_json_list(EXPENSE_DRAFTS_FILE, out)


def load_local_travel_records(email: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _read_json_list(TRAVEL_RECORDS_FILE)
    email = str(email or "").strip().lower()
    if not email:
        return rows
    return [r for r in rows if str(r.get("user_email") or "").strip().lower() == email]


def upsert_local_travel_record(email: str, payload: Dict[str, Any]) -> str:
    rows = _read_json_list(TRAVEL_RECORDS_FILE)
    email = str(email or payload.get("user_email") or "").strip().lower()
    payload = dict(payload)
    record_id = str(payload.get("record_id") or "").strip()
    if not record_id:
        existing_ids = [str(r.get("record_id") or "") for r in rows]
        record_id = _next_prefixed_id("TR", payload.get("employee_no") or email, payload.get("form_date"), existing_ids)
    payload["record_id"] = record_id
    payload["user_email"] = email
    payload["updated_at"] = datetime.now().isoformat(timespec="seconds")
    replaced = False
    for i, row in enumerate(rows):
        if str(row.get("record_id") or "") == record_id and str(row.get("user_email") or "").strip().lower() == email:
            rows[i] = payload
            replaced = True
            break
    if not replaced:
        rows.append(payload)
    _write_json_list(TRAVEL_RECORDS_FILE, rows)
    return record_id




def delete_local_travel_record(email: str, record_id: str) -> None:
    rows = _read_json_list(TRAVEL_RECORDS_FILE)
    email = str(email or "").strip().lower()
    out = []
    for row in rows:
        same = str(row.get("record_id") or "") == str(record_id or "") and str(row.get("user_email") or "").strip().lower() == email
        if not same:
            out.append(row)
    _write_json_list(TRAVEL_RECORDS_FILE, out)


def mark_local_travel_status(email: str, record_id: str, status: str) -> None:
    rows = _read_json_list(TRAVEL_RECORDS_FILE)
    email = str(email or "").strip().lower()
    for i, row in enumerate(rows):
        if str(row.get("record_id") or "") == str(record_id or "") and str(row.get("user_email") or "").strip().lower() == email:
            row = dict(row)
            row["status"] = status
            row["updated_at"] = datetime.now().isoformat(timespec="seconds")
            if status == "deleted":
                row["deleted_at"] = datetime.now().isoformat(timespec="seconds")
            if status == "void":
                row["voided_at"] = datetime.now().isoformat(timespec="seconds")
            rows[i] = row
            break
    _write_json_list(TRAVEL_RECORDS_FILE, rows)



def load_pending_sync(owner_email: str) -> list[dict]:
    rows = load_pending_sync_queue(owner_email)
    return rows if isinstance(rows, list) else []


def _queue_item_to_record(item: dict) -> dict:
    if not isinstance(item, dict):
        return {}
    payload = dict(item.get('payload') or {})
    payload.setdefault('queue_owner_email', item.get('queue_owner_email') or (item.get('actor') or {}).get('email') or payload.get('user_email') or '')
    payload.setdefault('system_type', payload.get('system_type') or ('travel' if 'travel' in str(item.get('operation','')).lower() else 'expense'))
    payload.setdefault('needs_sync', True)
    payload.setdefault('sync_status', 'pending')
    payload.setdefault('sync_message', '')
    return payload


def count_pending_sync(owner_email: str, system_type: Optional[str] = None) -> int:
    rows = []
    for item in load_pending_sync_queue(owner_email):
        row = _queue_item_to_record(item)
        if system_type and row.get('system_type') != system_type:
            continue
        if row.get('needs_sync', True) and row.get('sync_status', 'pending') in {'pending', 'failed', 'conflict'}:
            rows.append(row)
    return len(rows)


def mark_sync_success(owner_email: str, system_type: str, record_id: str) -> None:
    queue = load_pending_sync_queue(owner_email)
    rid = str(record_id or '').strip()
    newq = []
    for item in queue:
        payload = dict(item.get('payload') or {})
        item_system = payload.get('system_type') or ('travel' if 'travel' in str(item.get('operation','')).lower() else 'expense')
        item_rid = str(payload.get('record_id') or '').strip()
        if item_system == system_type and item_rid == rid:
            append_sync_audit({
                'event_type': 'sync_success',
                'record_id': rid,
                'system_type': system_type,
                'queue_owner_email': owner_email,
                'event_id': item.get('event_id', ''),
            })
            continue
        newq.append(item)
    save_pending_sync_queue(newq, owner_email)


def mark_sync_failed(owner_email: str, system_type: str, record_id: str, message: str = '') -> None:
    queue = load_pending_sync_queue(owner_email)
    rid = str(record_id or '').strip()
    changed = False
    for item in queue:
        payload = dict(item.get('payload') or {})
        item_system = payload.get('system_type') or ('travel' if 'travel' in str(item.get('operation','')).lower() else 'expense')
        item_rid = str(payload.get('record_id') or '').strip()
        if item_system == system_type and item_rid == rid:
            payload['needs_sync'] = True
            payload['sync_status'] = 'failed'
            payload['sync_message'] = message or '同步失敗'
            item['payload'] = payload
            item['retry_count'] = int(item.get('retry_count') or 0) + 1
            item['last_error'] = message or '同步失敗'
            changed = True
            append_sync_audit({
                'event_type': 'sync_failed',
                'record_id': rid,
                'system_type': system_type,
                'queue_owner_email': owner_email,
                'event_id': item.get('event_id', ''),
                'message': message or '同步失敗',
            })
    if changed:
        save_pending_sync_queue(queue, owner_email)


def get_sync_status_label(row: Dict[str, Any]) -> str:
    sync_status = str((row or {}).get('sync_status') or '').strip().lower()
    needs_sync = bool((row or {}).get('needs_sync', False))
    if sync_status == 'failed':
        return '同步失敗'
    if sync_status == 'conflict':
        return '版本衝突'
    if sync_status == 'synced' and not needs_sync:
        return '已同步'
    if sync_status == 'pending' or needs_sync:
        return '待同步'
    return ''


def clear_global_cache_files() -> None:
    for filename in ["users_cache.json", "user_defaults_cache.json", "options_cache.json", "cloud_backup.xlsx"]:
        path = _cache_path(filename)
        try:
            if path.exists():
                path.unlink()
        except Exception:
            pass
