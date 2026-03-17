from __future__ import annotations

import json
from datetime import date, datetime
from tempfile import gettempdir
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st

from storage_apps_script import Actor, AppsScriptStorage
from cache_utils import (
    delete_saved_file,
    load_local_travel_records,
    upsert_local_travel_record,
    mark_local_travel_status,
    count_pending_sync,
    load_pending_sync,
    mark_sync_success,
    mark_sync_failed,
    get_sync_status_label,
    queue_pending_sync,
    load_options_cache,
    load_users_cache,
    save_cloud_backup_excel,
    archive_deleted_record,
    delete_local_travel_record,
    load_deleted_archive_rows,
    mark_deleted_archive_restored,
    remove_pending_sync_item,
)
import pdf_gen_travel
from shared_plan_options import get_shared_plan_code_options
from sync_engine import build_master_dataframe, sync_pending_events

BASE_DIR = Path(__file__).resolve().parents[1]
TRAVEL_CONFIG_PATH = BASE_DIR / "data" / "travel_config.json"
TRAVEL_ATTACHMENTS_ROOT_URL = "https://drive.google.com/drive/folders/1Hh6JFu62PPVU6rCcQ5bV6NEh0VsGaEcv?usp=sharing"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}



def _get_web_app_url() -> str:
    cfg = _read_json(TRAVEL_CONFIG_PATH)
    secrets = st.secrets if hasattr(st, "secrets") else {}
    return (
        cfg.get("google", {}).get("apps_script_url")
        or secrets.get("APPS_SCRIPT_WEB_APP_URL", "")
    ).strip()


def _get_cloud_excel_url() -> str:
    cfg = _read_json(TRAVEL_CONFIG_PATH)
    return str(cfg.get("ui", {}).get("cloud_excel_url", "")).strip()


@st.cache_resource(show_spinner=False)
def get_api() -> AppsScriptStorage:
    return AppsScriptStorage(web_app_url=_get_web_app_url(), system="travel", timeout=20)


def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "travel") -> bytes:
    from io import BytesIO
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        (df.copy() if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sheet_name[:31], index=False)
    bio.seek(0)
    return bio.getvalue()


def _split_travel_export_frames(actor: Actor) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_df = list_records(actor)
    if not isinstance(all_df, pd.DataFrame) or all_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    df = all_df.copy().fillna("")
    if "status" not in df.columns:
        df["status"] = "draft"
    status_series = df["status"].astype(str).str.lower()
    draft_df = df[status_series.isin(["draft", "deleted"])].copy()
    submitted_df = df[status_series.isin(["submitted", "void"])].copy()
    return draft_df, submitted_df


def _build_travel_workbook_bytes(actor: Actor) -> bytes:
    from io import BytesIO
    draft_df, submitted_df = _split_travel_export_frames(actor)
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        submitted_df.to_excel(writer, sheet_name="申請列表", index=False)
        draft_df.to_excel(writer, sheet_name="草稿列表", index=False)
    bio.seek(0)
    return bio.getvalue()


def _group_option_rows() -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for row in load_options_cache() or []:
        k = str(row.get("option_type", "")).strip()
        v = str(row.get("option_value", "")).strip()
        if not k or not v:
            continue
        grouped.setdefault(k, [])
        if v not in grouped[k]:
            grouped[k].append(v)
    return grouped


def _option_candidates(grouped: dict[str, list[str]], *keys: str) -> list[str]:
    out: list[str] = []
    for key in keys:
        for v in grouped.get(key, []):
            if v not in out:
                out.append(v)
    if any(key in {"plan_code", "project_id"} for key in keys):
        return get_shared_plan_code_options(out, include_other=True)
    return out


def get_current_actor() -> Actor | None:
    name = str(st.session_state.get("actor_name", "")).strip()
    email = str(st.session_state.get("actor_email", "")).strip().lower()
    role = str(st.session_state.get("actor_role", "user")).strip() or "user"
    employee_no = str(st.session_state.get("actor_employee_no", "")).strip()
    department = str(st.session_state.get("actor_department", "")).strip()
    if not name or not email:
        return None
    return Actor(name=name, email=email, role=role, employee_no=employee_no, department=department)


def require_actor() -> Actor:
    actor = get_current_actor()
    if not actor:
        st.warning("請先回入口頁選擇身份。")
        if st.button("回到入口頁", type="primary"):
            st.switch_page("pages/home.py")
        st.stop()
    return actor


def safe_int(v: Any) -> int:
    try:
        return int(round(float(v or 0)))
    except Exception:
        return 0


def normalize_attachment_paths(value: Any) -> list[str]:
    if not value:
        return []
    out: list[str] = []
    for x in value:
        if isinstance(x, dict):
            p = str(x.get("path", "")).strip()
            if p:
                out.append(p)
        elif isinstance(x, str):
            p = x.strip()
            if p:
                out.append(p)
    return out


def default_form(actor: Actor) -> Dict[str, Any]:
    return {
        "record_id": "",
        "status": "draft",
        "form_date": date.today().isoformat(),
        "traveler": actor.name,
        "employee_no": actor.employee_no,
        "project_id": "",
        "budget_source": "",
        "purpose": "",
        "departure_location": "台南",
        "destination_location": "台北",
        "start_date": date.today().isoformat(),
        "end_date": date.today().isoformat(),
        "transport_options": [],
        "private_car_km": 0,
        "private_car_plate": "",
        "official_car_plate": "",
        "other_transport": "",
        "details": [{"日期": date.today().isoformat(), "起訖地點": "", "車別": "", "交通費": 0, "膳雜費": 0, "住宿費": 0, "其它": 0, "單據編號": ""}],
        "attachment_files": [],
        "signature_file": {},
        "user_email": actor.email,
        "owner_name": actor.name,
    }


def form_key(actor: Actor) -> str:
    return f"travel_form::{actor.email}"


def get_form(actor: Actor) -> Dict[str, Any]:
    if form_key(actor) not in st.session_state:
        st.session_state[form_key(actor)] = default_form(actor)
    return st.session_state[form_key(actor)]


def set_form(actor: Actor, data: Dict[str, Any]) -> None:
    st.session_state[form_key(actor)] = data

def _coerce_meta_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return {}
        try:
            parsed = json.loads(s)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _coerce_meta_list(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [x for x in value if isinstance(x, dict)]
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
            return [x for x in parsed if isinstance(x, dict)] if isinstance(parsed, list) else []
        except Exception:
            return []
    return []


def _normalize_loaded_travel_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    r = dict(rec or {})

    if not r.get("project_id") and r.get("plan_code"):
        r["project_id"] = r.get("plan_code")

    if not r.get("purpose") and r.get("trip_purpose"):
        r["purpose"] = r.get("trip_purpose")
    if not r.get("purpose") and r.get("purpose_desc"):
        r["purpose"] = r.get("purpose_desc")

    if not r.get("start_date") and r.get("trip_date_start"):
        r["start_date"] = r.get("trip_date_start")
    if not r.get("end_date") and r.get("trip_date_end"):
        r["end_date"] = r.get("trip_date_end")

    if not r.get("departure_location") and r.get("from_location"):
        r["departure_location"] = r.get("from_location")
    if not r.get("destination_location") and r.get("to_location"):
        r["destination_location"] = r.get("to_location")

    details = r.get("details")
    if not details and r.get("expense_rows"):
        details = r.get("expense_rows")
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except Exception:
            details = []
    if not isinstance(details, list) or not details:
        details = [{"日期": r.get("start_date", date.today().isoformat()), "起訖地點": "", "車別": "", "交通費": 0, "膳雜費": 0, "住宿費": 0, "其它": 0, "單據編號": ""}]
    r["details"] = details

    r["signature_file"] = _coerce_meta_dict(r.get("signature_file"))
    r["attachment_files"] = _coerce_meta_list(r.get("attachment_files"))

    return r



def _travel_local_rows(actor: Actor) -> List[Dict[str, Any]]:
    if str(actor.role).lower() == "admin":
        return load_local_travel_records() or []
    return load_local_travel_records(actor.email) or []


def _travel_cloud_rows(actor: Actor) -> List[Dict[str, Any]]:
    owner_only = str(actor.role).lower() != "admin"
    return get_api().record_list_all(actor=actor, status=None, owner_only=owner_only)


def _load_travel_master(actor: Actor, force_refresh: bool = False) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cache_key = f"travel_master_cache::{actor.email}::{actor.role}"
    if (not force_refresh) and cache_key in st.session_state:
        return st.session_state[cache_key]
    df, report = build_master_dataframe(
        'travel',
        actor.email,
        fetch_cloud_rows=lambda: _travel_cloud_rows(actor),
        local_rows=_travel_local_rows(actor),
    )
    if not df.empty:
        df = df.copy().fillna('')
        if 'status' not in df.columns:
            df['status'] = 'draft'
        if 'owner_name' not in df.columns:
            df['owner_name'] = df.get('traveler', '')
    st.session_state[cache_key] = (df, report)
    st.session_state['travel_sync_report'] = report
    st.session_state['cloud_online_travel'] = bool(report.get('cloud_online', False))
    return df, report


def _invalidate_travel_master(actor: Actor | None) -> None:
    if not actor:
        return
    for suffix in [f"travel_master_cache::{actor.email}::{actor.role}", f"travel_master_cache::{actor.email}::admin", f"travel_master_cache::{actor.email}::user"]:
        st.session_state.pop(suffix, None)


def _travel_pending_items(owner_email: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for item in load_pending_sync(owner_email) or []:
        payload = dict(item.get("payload") or {})
        system_type = str(payload.get("system_type") or ("travel" if "travel" in str(item.get("operation", "")).lower() else "expense")).lower()
        if system_type == "travel":
            items.append(item)
    return items


def _travel_raw_pending_count(owner_email: str) -> int:
    rows = []
    for item in _travel_pending_items(owner_email):
        payload = dict(item.get("payload") or {})
        sync_status = str(item.get("sync_status") or payload.get("sync_status") or "pending").lower()
        needs_sync = bool(payload.get("needs_sync", True))
        if needs_sync and sync_status in {"pending", "failed", "conflict"}:
            rows.append(item)
    return len(rows)


def _cleanup_stale_travel_pending(actor: Actor) -> int:
    report = st.session_state.get("travel_sync_report", {}) or {}
    raw_pending = _travel_raw_pending_count(actor.email)
    report_pending = int(report.get("pending_count", 0) or 0)
    cloud_online = bool(report.get("cloud_online", False))
    if not cloud_online or raw_pending <= report_pending:
        return 0
    removed = 0
    for item in _travel_pending_items(actor.email):
        payload = dict(item.get("payload") or {})
        sync_status = str(item.get("sync_status") or payload.get("sync_status") or "pending").lower()
        if sync_status == "conflict":
            continue
        event_id = str(item.get("event_id") or payload.get("event_id") or "").strip()
        record_id = str(payload.get("record_id") or "").strip()
        if event_id:
            removed += remove_pending_sync_item(actor.email, event_id=event_id, system_type="travel")
        elif record_id:
            removed += remove_pending_sync_item(actor.email, record_id=record_id, system_type="travel")
    return removed


def _queue_and_try_sync_travel(actor: Actor, operation: str, payload: Dict[str, Any]) -> tuple[bool, str]:
    payload = dict(payload or {})
    payload['system_type'] = 'travel'
    owner_email = str(payload.get('user_email') or actor.email or '').strip().lower()
    payload['user_email'] = owner_email
    queue_pending_sync(operation, {'email': actor.email, 'name': actor.name, 'role': actor.role}, payload, queue_owner_email=owner_email)
    try:
        result = sync_pending_events('travel', actor, get_api())
        _invalidate_travel_master(actor)
        _load_travel_master(actor, force_refresh=True)
        if result.get('failed', 0) == 0:
            st.session_state['cloud_online_travel'] = True
            return True, ''
        st.session_state['cloud_online_travel'] = False
        return False, f"仍有 {result.get('remaining', 0)} 筆待同步"
    except Exception as exc:
        st.session_state['cloud_online_travel'] = False
        return False, str(exc)


def list_records(actor: Actor) -> pd.DataFrame:
    df, report = _load_travel_master(actor)
    st.session_state["travel_sync_report"] = report
    return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()


def _travel_archive_restore_status(row: Dict[str, Any]) -> str:
    status = str((row or {}).get("status", "")).strip().lower()
    return "submitted" if status in {"submitted", "void"} else "draft"


def _travel_restore_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row or {})
    for k in [
        "archive_system_type", "archive_actor_email", "archived_at", "archive_id",
        "archive_restored", "restored_at", "restored_by", "restore_target_status",
    ]:
        payload.pop(k, None)
    payload["status"] = _travel_archive_restore_status(row)
    return payload


def _render_deleted_archive_restore_travel(actor: Actor) -> None:
    if str(actor.role).lower() != "admin":
        return
    rows = load_deleted_archive_rows(system_type="travel", include_restored=False)
    st.sidebar.markdown("---")
    st.sidebar.subheader("deleted archive 還原")
    if not rows:
        st.sidebar.info("目前沒有可還原的出差刪除備份。")
        return
    options: Dict[str, Dict[str, Any]] = {}
    labels: List[str] = []
    for row in rows[:100]:
        rid = str(row.get("record_id", "")).strip() or "(無編號)"
        owner = str(row.get("user_email", "")).strip().lower()
        archived_at = str(row.get("archived_at", "")).strip()
        raw_status = str(row.get("status", "")).strip().lower()
        restore_status = _travel_archive_restore_status(row)
        label = f"{rid}｜{owner or '-'}｜原狀態:{raw_status or '-'} → 還原為:{restore_status}｜{archived_at}"
        labels.append(label)
        options[label] = row
    selected = st.sidebar.selectbox("選擇要還原的出差紀錄", labels, key="travel_archive_restore_select")
    selected_row = options.get(selected) if selected else None
    if selected_row and st.sidebar.button("一鍵還原出差紀錄", key="travel_archive_restore_btn", use_container_width=True):
        payload = _travel_restore_payload(selected_row)
        target_status = str(payload.get("status", "draft")).strip().lower() or "draft"
        owner_email = str(payload.get("user_email") or actor.email or "").strip().lower()
        upsert_local_travel_record(owner_email, payload)
        ok, msg = _queue_and_try_sync_travel(actor, f'travel_restore_{target_status}', payload)
        mark_deleted_archive_restored(str(selected_row.get("archive_id", "")), restored_by=actor.email, restore_target_status=target_status)
        _invalidate_travel_master(actor)
        if ok:
            st.sidebar.success(f"已還原：{payload.get('record_id','')}")
        else:
            st.sidebar.warning(f"已加入待同步還原：{msg or '請稍後重新同步'}")
        st.rerun()


def render_sync_status_sidebar_travel(current_user_email: str) -> None:
    if not current_user_email:
        return
    st.sidebar.markdown("---")
    st.sidebar.subheader("雲端同步狀態")

    actor = get_current_actor() or Actor(name="", email=current_user_email, role="user")
    _, report = _load_travel_master(actor, force_refresh=False)
    raw_pending_count = _travel_raw_pending_count(current_user_email)
    report_pending_count = int(report.get("pending_count", 0) or 0)
    stale_queue_detected = raw_pending_count != report_pending_count

    cloud_online = bool(report.get("cloud_online", st.session_state.get("cloud_online_travel", True)))
    st.session_state["cloud_online_travel"] = cloud_online
    if cloud_online:
        st.sidebar.success("雲端：已連線")
    else:
        st.sidebar.error("雲端：未連線")

    if report_pending_count > 0:
        st.sidebar.warning(f"你有 {report_pending_count} 筆出差資料尚未同步到雲端")
    else:
        st.sidebar.success("你的出差資料皆已同步")

    if stale_queue_detected:
        st.sidebar.warning("偵測到本機待同步殘留，已建議清理")

    cloud_url = _get_cloud_excel_url()
    if cloud_url:
        st.sidebar.link_button("開啟雲端表單", cloud_url, use_container_width=True)
    st.sidebar.link_button("開啟附件雲端資料夾", TRAVEL_ATTACHMENTS_ROOT_URL, use_container_width=True)

    draft_cloud_df, submitted_cloud_df = _split_travel_export_frames(actor)
    save_cloud_backup_excel({"申請列表": submitted_cloud_df, "草稿列表": draft_cloud_df}, filename="travel_cloud_backup.xlsx")
    st.sidebar.download_button(
        "下載Excel",
        data=_build_travel_workbook_bytes(actor),
        file_name="出差報帳.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="travel_sidebar_download_excel",
    )

    _render_deleted_archive_restore_travel(actor)

    st.sidebar.caption(f"master={report.get('master_count', 0)}｜cloud={report.get('cloud_count', 0)}｜pending={report_pending_count}")
    if report.get('cloud_count', 0) != report.get('master_count', 0) and report_pending_count == 0:
        st.sidebar.warning('偵測到雲端與前端筆數不一致，建議重新同步或重新整理。')

    if st.sidebar.button("立即同步出差資料", key="sync_travel_now_btn", use_container_width=True):
        try:
            sync_actor = get_current_actor() or Actor(name='', email=current_user_email, role='user')
            result = sync_pending_events('travel', sync_actor, get_api())
            _invalidate_travel_master(sync_actor)
            _, report = _load_travel_master(sync_actor, force_refresh=True)
            st.session_state['cloud_online_travel'] = result.get('failed', 0) == 0
            cleanup_removed = 0
            if result.get('failed', 0) == 0 and result.get('conflicts', 0) == 0:
                cleanup_removed = _cleanup_stale_travel_pending(sync_actor)
                if cleanup_removed:
                    _invalidate_travel_master(sync_actor)
                    _, report = _load_travel_master(sync_actor, force_refresh=True)
            if result.get('synced', 0) == 0 and result.get('failed', 0) == 0 and report.get('pending_count', 0) == 0:
                msg = "目前沒有待同步的出差資料。"
                if cleanup_removed:
                    msg += f" 已清理 {cleanup_removed} 筆本機待同步殘留。"
                st.sidebar.info(msg)
            elif result.get('failed', 0) == 0:
                msg = f"同步完成：{result.get('synced', 0)} 筆"
                if cleanup_removed:
                    msg += f"；另已清理 {cleanup_removed} 筆本機待同步殘留"
                st.sidebar.success(msg)
            else:
                st.sidebar.warning(f"同步完成：成功 {result.get('synced', 0)} 筆，失敗 {result.get('failed', 0)} 筆")
        except Exception as e:
            st.session_state['cloud_online_travel'] = False
            st.sidebar.error(f"同步失敗：{e}")


def render_top_sync_notice_travel(current_user_email: str) -> None:
    if not current_user_email:
        return
    actor = get_current_actor() or Actor(name="", email=current_user_email, role="user")
    _, report = _load_travel_master(actor, force_refresh=False)
    pending_count = int(report.get("pending_count", 0) or 0)
    if pending_count > 0:
        st.info(f"提醒：你有 {pending_count} 筆出差資料尚未同步到雲端。")
    elif _travel_raw_pending_count(current_user_email) != pending_count:
        st.info("提醒：偵測到本機待同步殘留，已建議清理。")


def _upload_file_to_drive(actor: Actor, up, category: str, record_id: str = "") -> Dict[str, Any]:
    return get_api().upload_drive_file(
        actor,
        filename=getattr(up, "name", "file.bin"),
        file_bytes=up.getvalue(),
        mime_type=getattr(up, "type", "application/octet-stream") or "application/octet-stream",
        category=category,
        record_id=record_id,
        owner_email=actor.email,
    )


def _delete_attachment_meta(actor: Actor, meta: Dict[str, Any]) -> None:
    drive_file_id = str((meta or {}).get("drive_file_id", "")).strip()
    if drive_file_id:
        try:
            get_api().delete_drive_file(actor, drive_file_id)
        except Exception:
            pass
    else:
        delete_saved_file(meta)


def persist_uploads(actor: Actor, payload: Dict[str, Any], uploads: list | None, signature_upload) -> Dict[str, Any]:
    payload = dict(payload)
    existing = list(payload.get("attachment_files", []) or [])
    record_id = str(payload.get("record_id") or "").strip()
    for up in uploads or []:
        marker = (getattr(up, 'name', ''), len(up.getvalue()))
        if any((r.get("name"), int(r.get("size", 0))) == marker for r in existing if str(r.get("size", "")).isdigit()):
            continue
        existing.append(_upload_file_to_drive(actor, up, "travel_attachment", record_id))
    payload["attachment_files"] = existing
    if signature_upload is not None:
        payload["signature_file"] = _upload_file_to_drive(actor, signature_upload, "travel_signature", record_id)
    return payload


def remove_attachment(actor: Actor, idx: int) -> None:
    form = dict(get_form(actor))
    files = list(form.get("attachment_files", []) or [])
    if 0 <= idx < len(files):
        _delete_attachment_meta(actor, files[idx])
        files.pop(idx)
        form["attachment_files"] = files
        set_form(actor, form)


def remove_signature(actor: Actor) -> None:
    form = dict(get_form(actor))
    _delete_attachment_meta(actor, form.get("signature_file", {}))
    form["signature_file"] = {}
    set_form(actor, form)


def load_into_form(actor: Actor, rec: Dict[str, Any], as_copy: bool = False) -> None:
    data = _normalize_loaded_travel_record(rec)
    if as_copy:
        data["record_id"] = ""
        data["form_date"] = date.today().isoformat()
        data["status"] = "draft"
    set_form(actor, data)
    st.session_state["travel_page"] = "new"


def _travel_runtime_attachment_dir() -> Path:
    path = Path(gettempdir()) / 'travel_drive_runtime'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _resolve_attachment_paths(actor: Actor, payload: Dict[str, Any]) -> list[str]:
    out: list[str] = []
    for meta in list(payload.get('attachment_files', []) or []):
        if isinstance(meta, str):
            p = meta.strip()
            if p and Path(p).exists():
                out.append(p)
            continue
        if not isinstance(meta, dict):
            continue
        p = str(meta.get('path', '')).strip()
        if p and Path(p).exists():
            out.append(p)
            continue
        drive_file_id = str(meta.get('drive_file_id', '')).strip()
        if not drive_file_id:
            continue
        try:
            file_meta = get_api().download_drive_file(actor, drive_file_id)
            file_bytes = file_meta.get('file_bytes', b'')
            if not file_bytes:
                continue
            filename = str(file_meta.get('name') or meta.get('name') or f'{drive_file_id}.bin').strip()
            target = _travel_runtime_attachment_dir() / filename
            target.write_bytes(file_bytes)
            out.append(str(target))
        except Exception:
            continue
    return out


def _build_pdf(actor: Actor, payload: Dict[str, Any]) -> bytes:
    attachment_paths = _resolve_attachment_paths(actor, payload)
    return pdf_gen_travel.build_pdf_bytes(payload, attachment_paths=attachment_paths)


def render_form(actor: Actor) -> None:
    form = get_form(actor)
    st.title("出差報帳")

    grouped = _group_option_rows()
    users_rows = load_users_cache() or []
    traveler_options = [str(r.get("name", "")).strip() for r in users_rows if str(r.get("name", "")).strip()] or [actor.name]
    employee_options = [str(r.get("employee_no", "")).strip() for r in users_rows if str(r.get("employee_no", "")).strip()] or [actor.employee_no]
    project_options = _option_candidates(grouped, "plan_code", "project_id") or [""]
    budget_options = _option_candidates(grouped, "budget_source") or [""]
    departure_options = ["台南", "台中", "其他"]
    destination_options = ["台北", "新北", "新竹", "台中", "台南", "高雄", "其他"]
    transport_opts = ["公務車", "計程車", "私車公用", "高鐵", "台鐵","飛機", "派車", "其他"]

    details_rows = form.get("details") or []
    if not isinstance(details_rows, list) or not details_rows:
        details_rows = [{"日期": form.get("start_date", date.today().isoformat()), "起訖地點": "", "車別": "", "交通費": 0, "膳雜費": 0, "住宿費": 0, "其它": 0, "單據編號": ""}]
    details_df = pd.DataFrame(details_rows).fillna("")

    pdf_bytes: bytes | None = None

    with st.form("travel_main_form", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        form_date_val = c1.date_input("填寫日期", value=datetime.fromisoformat(str(form.get("form_date", date.today().isoformat()))).date())
        traveler_val = c2.selectbox("出差人", traveler_options, index=traveler_options.index(form.get("traveler", actor.name)) if form.get("traveler", actor.name) in traveler_options else 0)
        employee_val = c3.selectbox("工號", employee_options, index=employee_options.index(form.get("employee_no", actor.employee_no)) if form.get("employee_no", actor.employee_no) in employee_options else 0)

        c4, c5, c6, c7 = st.columns(4)
        current_project = str(form.get("project_id", "")).strip()
        project_select_options = list(project_options) if list(project_options) else [""]
        if "其他" not in project_select_options:
            project_select_options.append("其他")
        project_select_default = current_project if current_project in project_select_options else ("其他" if current_project else project_select_options[0])
        project_choice = c4.selectbox("計畫編號", project_select_options, index=project_select_options.index(project_select_default))
        budget_val = c5.selectbox("預算來源", budget_options, index=budget_options.index(form.get("budget_source", "")) if form.get("budget_source", "") in budget_options else 0)
        project_other_val = ""
        if project_choice == "其他":
            project_other_val = st.text_input("計畫編號（其他）", value=current_project if current_project not in project_options else "")

        dep_default = form.get("departure_location", "台南") if form.get("departure_location", "台南") in departure_options else "其他"
        dest_default = form.get("destination_location", "台北") if form.get("destination_location", "台北") in destination_options else "其他"
        dep_choice = c6.selectbox("出發地", departure_options, index=departure_options.index(dep_default))
        dest_choice = c7.selectbox("目的地", destination_options, index=destination_options.index(dest_default))

        dep_other = ""
        dest_other = ""
        if dep_choice == "其他":
            dep_other = st.text_input("其他出發地", value=form.get("departure_location", "") if form.get("departure_location", "") not in departure_options else "")
        if dest_choice == "其他":
            dest_other = st.text_input("其他目的地", value=form.get("destination_location", "") if form.get("destination_location", "") not in destination_options else "")

        purpose_val = st.text_input("出差事由", value=str(form.get("purpose", "")))
        d1, d2 = st.columns(2)
        start_val = d1.date_input("起始日期", value=datetime.fromisoformat(str(form.get("start_date", date.today().isoformat()))).date())
        end_val = d2.date_input("結束日期", value=datetime.fromisoformat(str(form.get("end_date", date.today().isoformat()))).date())

        transport_val = st.multiselect("交通方式", transport_opts, default=[x for x in form.get("transport_options", []) if x in transport_opts])
        tf1, tf2, tf3, tf4 = st.columns(4)
        private_km_val = tf1.number_input("私車公用里程數", min_value=0, step=1, value=safe_int(form.get("private_car_km", 0)))
        private_plate_val = tf2.text_input("私車公用車號", value=str(form.get("private_car_plate", "")))
        official_plate_val = tf3.text_input("公務車車號", value=str(form.get("official_car_plate", "")))
        other_transport_val = tf4.text_input("其他交通方式", value=str(form.get("other_transport", "")))

        edited_df = st.data_editor(
            details_df,
            hide_index=True,
            num_rows="dynamic",
            use_container_width=True,
            key="travel_details_editor",
            column_config={
                "日期": st.column_config.TextColumn("日期", help="YYYY-MM-DD"),
                "起訖地點": st.column_config.TextColumn("起訖地點"),
                "車別": st.column_config.SelectboxColumn(
                    "車別",
                    options=["", "高鐵", "台鐵", "客運", "捷運", "公車", "計程車", "私車公用", "公務車", "飛機", "船舶", "其他", "停車費", "過路費"],
                    required=False,
                ),
                "交通費": st.column_config.NumberColumn("交通費", min_value=0, step=1),
                "膳雜費": st.column_config.NumberColumn("膳雜費", min_value=0, step=1),
                "住宿費": st.column_config.NumberColumn("住宿費", min_value=0, step=1),
                "其它": st.column_config.NumberColumn("其它", min_value=0, step=1),
                "單據編號": st.column_config.TextColumn("單據編號"),
            },
        )

        attach_uploads = st.file_uploader("上傳附件", type=["pdf", "png", "jpg", "jpeg", "webp", "bmp"], accept_multiple_files=True)
        signature_upload = st.file_uploader("上傳數位簽名檔", type=["png", "jpg", "jpeg", "webp", "bmp"], accept_multiple_files=False)

        t1, t2, t3, t4, t5 = st.columns(5)
        save_draft = t1.form_submit_button("儲存草稿", use_container_width=True)
        submit_final = t2.form_submit_button("確認無誤並送出", use_container_width=True, type="primary")
        make_pdf = t3.form_submit_button("下載PDF", use_container_width=True)
        copy_form = t4.form_submit_button("複製本表單", use_container_width=True)
        back_list = t5.form_submit_button("返回列表", use_container_width=True)

        x1, x2 = st.columns(2)
        delete_or_void = x1.form_submit_button("作廢此筆" if str(form.get("status", "draft")).lower() in {"submitted", "void"} else "刪除此筆", use_container_width=True)
        clear_new = x2.form_submit_button("清空新增", use_container_width=True)

        payload = {
            "record_id": form.get("record_id", ""),
            "status": form.get("status", "draft"),
            "form_date": form_date_val.isoformat(),
            "traveler": traveler_val,
            "employee_no": employee_val,
            "project_id": project_other_val.strip() if project_choice == "其他" else project_choice,
            "budget_source": budget_val,
            "purpose": purpose_val,
            "departure_location": dep_other if dep_choice == "其他" else dep_choice,
            "destination_location": dest_other if dest_choice == "其他" else dest_choice,
            "location": " → ".join([x for x in [(dep_other if dep_choice == "其他" else dep_choice), (dest_other if dest_choice == "其他" else dest_choice)] if x]),
            "start_date": start_val.isoformat(),
            "end_date": end_val.isoformat(),
            "transport_options": list(transport_val),
            "private_car_km": safe_int(private_km_val) if "私車公用" in transport_val else 0,
            "private_car_plate": private_plate_val if "私車公用" in transport_val else "",
            "official_car_plate": official_plate_val if "公務車" in transport_val else "",
            "other_transport": other_transport_val if "其他" in transport_val else "",
            "details": edited_df.fillna("").to_dict(orient="records"),
            "attachment_files": _coerce_meta_list(form.get("attachment_files")),
            "signature_file": _coerce_meta_dict(form.get("signature_file")),
            "user_email": actor.email,
            "owner_name": actor.name,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
        payload["transport_fee_total"] = int(pd.Series([safe_int(x.get("交通費", 0)) for x in payload["details"]]).sum()) if payload["details"] else 0
        payload["misc_fee_total"] = int(pd.Series([safe_int(x.get("膳雜費", 0)) for x in payload["details"]]).sum()) if payload["details"] else 0
        payload["lodging_fee_total"] = int(pd.Series([safe_int(x.get("住宿費", 0)) for x in payload["details"]]).sum()) if payload["details"] else 0
        payload["other_fee_total"] = int(pd.Series([safe_int(x.get("其它", 0)) for x in payload["details"]]).sum()) if payload["details"] else 0
        payload["amount_total"] = payload["transport_fee_total"] + payload["misc_fee_total"] + payload["lodging_fee_total"] + payload["other_fee_total"]

        if save_draft or submit_final or make_pdf:
            payload = persist_uploads(actor, payload, attach_uploads, signature_upload)

        if save_draft:
            payload["status"] = "draft"
            rid = upsert_local_travel_record(actor.email, payload)
            payload["record_id"] = rid
            set_form(actor, payload)
            ok, msg = _queue_and_try_sync_travel(actor, 'travel_draft', payload)
            st.session_state["travel_page"] = "drafts"
            if ok:
                st.success('草稿已儲存並同步。')
            else:
                st.warning(f"草稿已先保存在本機待同步：{msg or '請稍後重新同步'}")
            st.rerun()

        if submit_final:
            payload["status"] = "submitted"
            rid = upsert_local_travel_record(actor.email, payload)
            payload["record_id"] = rid
            set_form(actor, payload)
            ok, msg = _queue_and_try_sync_travel(actor, 'travel_submit', payload)
            st.session_state["travel_page"] = "submitted"
            if ok:
                st.success('表單已送出並同步。')
            else:
                st.error(f"送出已保存在本機待同步：{msg or '請稍後重新同步'}")
            st.rerun()

        if make_pdf:
            set_form(actor, payload)
            pdf_bytes = _build_pdf(actor, payload)

        if copy_form:
            set_form(actor, {**payload, "record_id": "", "form_date": date.today().isoformat(), "status": "draft"})
            st.rerun()

        if back_list:
            set_form(actor, payload)
            st.session_state["travel_page"] = "all"
            st.rerun()

        if delete_or_void:
            rid = str(form.get("record_id") or "")
            target_status = "void" if str(form.get("status", "draft")).lower() in {"submitted", "void"} else "deleted"
            if rid:
                mark_local_travel_status(actor.email, rid, target_status)
                payload['record_id'] = rid
                payload['status'] = target_status
                _queue_and_try_sync_travel(actor, 'travel_soft_delete', payload)
            st.session_state["travel_page"] = "submitted" if str(form.get("status", "draft")).lower() in {"submitted", "void"} else "drafts"
            st.rerun()

        if clear_new:
            set_form(actor, default_form(actor))
            st.rerun()

    current = get_form(actor)
    st.session_state["travel_sidebar_export_df"] = pd.DataFrame([current])

    st.subheader("已附附件")
    if current.get("attachment_files"):
        for i, att in enumerate(current["attachment_files"]):
            name = att.get("name") if isinstance(att, dict) else str(att)
            drive_url = att.get("drive_url", "") if isinstance(att, dict) else ""
            a1, a2, a3, a4 = st.columns([5, 1.2, 1.2, 1])
            a1.write(name or "")
            if drive_url:
                a2.link_button("預覽", drive_url, use_container_width=True)
            else:
                a2.write("")
            if isinstance(att, dict) and str(att.get('drive_file_id', '')).strip():
                try:
                    dl = get_api().download_drive_file(actor, str(att.get('drive_file_id')))
                    a3.download_button(
                        "下載",
                        data=dl.get('file_bytes', b''),
                        file_name=dl.get('name') or (name or 'attachment.bin'),
                        mime=dl.get('mime_type') or 'application/octet-stream',
                        key=f"trv_att_dl_{i}",
                        use_container_width=True,
                    )
                except Exception:
                    a3.write("")
            else:
                a3.write("")
            if a4.button("移除", key=f"trv_att_rm_{i}"):
                remove_attachment(actor, i)
                st.rerun()
    else:
        st.caption("目前沒有已附附件。")

    if current.get("signature_file"):
        s1, s2 = st.columns([6, 1])
        sig_name = current["signature_file"].get("name", "") if isinstance(current["signature_file"], dict) else str(current["signature_file"])
        s1.write(f"數位簽名檔：{sig_name}")
        if s2.button("移除", key="trv_sig_rm"):
            remove_signature(actor)
            st.rerun()

    if pdf_bytes:
        st.download_button(
            "點此下載PDF",
            data=pdf_bytes,
            file_name=f"出差報帳_{current.get('record_id') or 'preview'}.pdf",
            mime="application/pdf",
            use_container_width=True,
            key="travel_pdf_download_final",
        )

    t, m, l, o = safe_int(current.get("transport_fee_total", 0)), safe_int(current.get("misc_fee_total", 0)), safe_int(current.get("lodging_fee_total", 0)), safe_int(current.get("other_fee_total", 0))
    cols = st.columns(5)
    cols[0].metric("交通費合計", f"NT$ {t:,}")
    cols[1].metric("膳雜費合計", f"NT$ {m:,}")
    cols[2].metric("住宿費合計", f"NT$ {l:,}")
    cols[3].metric("其他費合計", f"NT$ {o:,}")
    cols[4].metric("總金額總計", f"NT$ {safe_int(current.get('amount_total', 0)):,}")


def render_list(actor: Actor, title: str, statuses: List[str], key_prefix: str) -> None:
    st.title(title)
    df = list_records(actor)
    st.session_state["travel_sidebar_export_df"] = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    if not df.empty:
        df["status"] = df["status"].astype(str).str.lower()
        df = df[df["status"].isin(statuses)].copy()

    reset_col, _ = st.columns([1, 5])
    if reset_col.button("重設篩選", key=f"{key_prefix}_reset"):
        for s in ["status", "owner", "plan", "record", "start", "end", "page_size", "page_no"]:
            st.session_state.pop(f"{key_prefix}_{s}", None)
        st.rerun()

    r1 = st.columns(4)
    opts = ["all"] + statuses
    cur = st.session_state.get(f"{key_prefix}_status", statuses[0] if len(statuses) == 1 else "all")
    if cur not in opts:
        cur = opts[0]
    status_filter = r1[0].selectbox("狀態", opts, index=opts.index(cur), key=f"{key_prefix}_status")
    owner = r1[1].text_input("填表人包含", value=st.session_state.get(f"{key_prefix}_owner", ""), key=f"{key_prefix}_owner")
    plan = r1[2].text_input("計畫編號包含", value=st.session_state.get(f"{key_prefix}_plan", ""), key=f"{key_prefix}_plan")
    record = r1[3].text_input("表單ID", value=st.session_state.get(f"{key_prefix}_record", ""), key=f"{key_prefix}_record")

    r2 = st.columns(2)
    start_month = r2[0].text_input("起始年月(YYYY-MM)", value=st.session_state.get(f"{key_prefix}_start", ""), key=f"{key_prefix}_start")
    end_month = r2[1].text_input("結束年月(YYYY-MM)", value=st.session_state.get(f"{key_prefix}_end", ""), key=f"{key_prefix}_end")

    r3 = st.columns(2)
    page_size_options = [10, 20, 50, 100]
    current_page_size = int(st.session_state.get(f"{key_prefix}_page_size", 20) or 20)
    if current_page_size not in page_size_options:
        current_page_size = 20
    page_size = r3[0].selectbox("每頁筆數", page_size_options, index=page_size_options.index(current_page_size), key=f"{key_prefix}_page_size")

    if df.empty:
        r3[1].number_input("頁碼", min_value=1, value=1, disabled=True, key=f"{key_prefix}_page_no")
        st.info("目前沒有符合篩選條件的資料。")
        return

    filtered = df.copy().fillna("")
    if "owner_name" not in filtered.columns:
        filtered["owner_name"] = filtered.get("traveler", "")
    filtered["project_id_text"] = filtered.get("project_id", "").astype(str)
    filtered["record_id_text"] = filtered.get("record_id", "").astype(str)
    filtered["month_text"] = filtered.get("form_date", "").astype(str).str.slice(0, 7)

    if status_filter != "all":
        filtered = filtered[filtered["status"] == status_filter]
    if owner.strip():
        filtered = filtered[filtered["owner_name"].astype(str).str.contains(owner.strip(), case=False, na=False)]
    if plan.strip():
        filtered = filtered[filtered["project_id_text"].str.contains(plan.strip(), case=False, na=False)]
    if record.strip():
        filtered = filtered[filtered["record_id_text"].str.contains(record.strip(), case=False, na=False)]
    if start_month.strip():
        filtered = filtered[filtered["month_text"] >= start_month.strip()]
    if end_month.strip():
        filtered = filtered[filtered["month_text"] <= end_month.strip()]

    if filtered.empty:
        r3[1].number_input("頁碼", min_value=1, value=1, disabled=True, key=f"{key_prefix}_page_no")
        st.info("目前沒有符合篩選條件的資料。")
        return

    total_pages = max(1, (len(filtered) + page_size - 1) // page_size)
    current_page_no = int(st.session_state.get(f"{key_prefix}_page_no", 1) or 1)
    if current_page_no > total_pages:
        current_page_no = total_pages
    page_no = r3[1].number_input("頁碼", min_value=1, max_value=total_pages, value=current_page_no, step=1, key=f"{key_prefix}_page_no")

    page_df = filtered.iloc[(page_no - 1) * page_size : page_no * page_size].copy()

    totals = {
        "交通費合計": int(page_df.get("transport_fee_total", 0).apply(safe_int).sum()) if "transport_fee_total" in page_df.columns else 0,
        "膳雜費合計": int(page_df.get("misc_fee_total", 0).apply(safe_int).sum()) if "misc_fee_total" in page_df.columns else 0,
        "住宿費合計": int(page_df.get("lodging_fee_total", 0).apply(safe_int).sum()) if "lodging_fee_total" in page_df.columns else 0,
        "其他費合計": int(page_df.get("other_fee_total", 0).apply(safe_int).sum()) if "other_fee_total" in page_df.columns else 0,
    }
    total_all = sum(totals.values())

    h = st.columns([1.2, 0.8, 0.95, 1, 1, 1, 0.9, 1.2, 2.5])
    for c, t in zip(h, ["表單ID", "狀態", "同步狀態", "日期", "填表人", "計畫編號", "總金額", "更新時間", "操作"]):
        c.markdown(f"**{t}**")
    for _, row in page_df.iterrows():
        rec = row.to_dict()
        cols = st.columns([1.2, 0.8, 0.95, 1, 1, 1, 0.9, 1.2, 2.5])
        cols[0].write(rec.get("record_id", ""))
        cols[1].write(rec.get("status", ""))
        cols[2].write(get_sync_status_label(rec))
        cols[3].write(str(rec.get("form_date", ""))[:10])
        cols[4].write(rec.get("owner_name", "") or rec.get("traveler", ""))
        cols[5].write(rec.get("project_id", ""))
        cols[6].write(f"{safe_int(rec.get('amount_total')):,}")
        cols[7].write(str(rec.get("updated_at", ""))[:19])
        actions = cols[8].columns(6)
        record_id = str(rec.get("record_id") or "")
        owner_email = str(rec.get("user_email") or actor.email or "").strip().lower()
        if actions[0].button("編輯", key=f"{key_prefix}_edit_{rec.get('record_id')}"):
            load_into_form(actor, rec, as_copy=False)
            st.rerun()
        if actions[1].button("複製", key=f"{key_prefix}_copy_{rec.get('record_id')}"):
            load_into_form(actor, rec, as_copy=True)
            st.rerun()
        pdf_bytes = _build_pdf(actor, rec)
        actions[2].download_button("下載", data=pdf_bytes, file_name=f"出差報帳_{rec.get('record_id') or 'preview'}.pdf", mime="application/pdf", key=f"{key_prefix}_dl_{rec.get('record_id')}")
        if actions[3].button("送出", key=f"{key_prefix}_submit_{rec.get('record_id')}", disabled=str(rec.get("status")) in {"submitted", "void"}):
            rec["status"] = "submitted"
            upsert_local_travel_record(owner_email, rec)
            ok, msg = _queue_and_try_sync_travel(actor, 'travel_submit', rec)
            _invalidate_travel_master(actor)
            st.rerun()
        action_label = "作廢" if str(rec.get("status")) in {"submitted", "void"} else "刪除"
        if actions[4].button(action_label, key=f"{key_prefix}_del_{rec.get('record_id')}"):
            target_status = "void" if action_label == "作廢" else "deleted"
            mark_local_travel_status(owner_email, record_id, target_status)
            rec['status'] = target_status
            ok, msg = _queue_and_try_sync_travel(actor, 'travel_soft_delete', rec)
            _invalidate_travel_master(actor)
            st.rerun()
        confirm_key = f"travel_hard_delete_confirm::{record_id}"
        if st.session_state.get(confirm_key):
            if actions[5].button("確認移除", key=f"{key_prefix}_hard_yes_{record_id}"):
                archive_deleted_record(rec, system_type="travel", actor_email=actor.email)
                delete_local_travel_record(owner_email, record_id)
                ok, msg = _queue_and_try_sync_travel(actor, 'travel_hard_delete', {'record_id': record_id, 'user_email': owner_email, 'system_type': 'travel'})
                _invalidate_travel_master(actor)
                st.session_state.pop(confirm_key, None)
                st.success(f"{record_id} 已永久移除。")
                st.rerun()
        elif str(actor.role).lower() == "admin" and actions[5].button("移除", key=f"{key_prefix}_hard_del_{record_id}"):
            st.session_state[confirm_key] = True
            st.warning("此操作會永久移除資料，且已先備份到 deleted archive。")
            st.rerun()
        if st.session_state.get(confirm_key):
            c1, c2 = st.columns(2)
            if c1.button("取消移除", key=f"{key_prefix}_hard_no_{record_id}"):
                st.session_state.pop(confirm_key, None)
                st.rerun()

    m = st.columns(5)
    for col, (label, value) in zip(m, list(totals.items()) + [("總金額總計", total_all)]):
        col.metric(label, f"NT$ {value:,}")


def main() -> None:
    st.set_page_config(page_title="出差報帳", page_icon="🚆", layout="wide")
    st.session_state.setdefault("travel_sidebar_export_df", pd.DataFrame())
    actor = require_actor()
    with st.sidebar:
        st.write(f"姓名：{actor.name}")
        st.write(f"Email：{actor.email}")
        st.write(f"角色：{actor.role}")
        page_options = ["new", "drafts", "submitted", "all"]
        current = st.session_state.get("travel_page", "new")
        choice = st.radio("功能選單", page_options, index=page_options.index(current) if current in page_options else 0, format_func=lambda x: {"new":"📝 新增 / 編輯","drafts":"📄 草稿列表","submitted":"📤 已送出列表","all":"📚 全部資料"}[x])
        if choice != current:
            st.session_state["travel_page"] = choice
            if choice == "new":
                set_form(actor, default_form(actor))
            st.rerun()
        render_sync_status_sidebar_travel(actor.email)
    render_top_sync_notice_travel(actor.email)
    page = st.session_state.get("travel_page", "new")
    if page == "drafts":
        render_list(actor, "草稿列表", ["draft", "deleted"], "trv_drafts")
    elif page == "submitted":
        render_list(actor, "已送出表單列表", ["submitted", "void"], "trv_submitted")
    elif page == "all":
        render_list(actor, "全部表單列表", ["draft", "deleted", "submitted", "void"], "trv_all")
    else:
        render_form(actor)


if __name__ == "__main__":
    main()
