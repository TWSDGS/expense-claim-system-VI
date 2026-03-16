from __future__ import annotations

import json
import re
import shutil
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
    record_id = str(payload.get('record_id') or '').strip()
    queued_at = datetime.now().isoformat(timespec='seconds')
    item = {
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
    record_id = str(payload.get("record_id") or "").strip() or f"LCL-EXP-{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
    record_id = str(payload.get("record_id") or "").strip() or f"LCL-TRV-{datetime.now().strftime('%Y%m%d%H%M%S')}"
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
