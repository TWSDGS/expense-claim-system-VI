from __future__ import annotations

import json
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from storage_apps_script import AppsScriptAPIError, AppsScriptStorage, Actor
from cache_utils import (
    load_backup_sheet_df,
    load_local_expense_drafts,
    load_options_cache,
    load_user_defaults_cache,
    save_cloud_backup_excel,
    save_options_cache,
    save_signature_file,
    save_uploaded_attachment,
    save_user_defaults_cache,
    upsert_local_expense_draft,
    remove_local_expense_draft,
    delete_saved_file,
    count_pending_sync,
    load_pending_sync,
    mark_sync_success,
    mark_sync_failed,
    get_sync_status_label,
    queue_pending_sync,
    archive_deleted_record,
    load_deleted_archive_rows,
    mark_deleted_archive_restored,
    remove_pending_sync_item,
)
from pdf_gen import build_pdf_bytes, merge_expense_pdf_with_attachments
from shared_plan_options import get_shared_plan_code_options
from sync_engine import build_master_dataframe, sync_pending_events

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CONFIG_PATH = DATA_DIR / "config.json"


EXPENSE_WIDGET_KEYS = {
    "form_date": "exp_form_date",
    "plan_code": "exp_plan_code",
    "plan_code_other": "exp_plan_code_other",
    "purpose_desc": "exp_purpose_desc",
    "payment_target": "exp_payment_target",
    "employee_name": "exp_employee_name",
    "employee_name_other": "exp_employee_name_other",
    "employee_no": "exp_employee_no",
    "employee_no_other": "exp_employee_no_other",
    "advance_amount": "exp_advance_amount",
    "offset_amount": "exp_offset_amount",
    "balance_refund_amount": "exp_balance_refund_amount",
    "supplement_amount": "exp_supplement_amount",
    "vendor_name": "exp_vendor_name",
    "vendor_address": "exp_vendor_address",
    "vendor_payee_name": "exp_vendor_payee_name",
    "receipt_count": "exp_receipt_count",
    "amount_untaxed": "exp_amount_untaxed",
    "tax_mode": "exp_tax_mode",
    "department": "exp_department",
    "department_other": "exp_department_other",
    "note_public": "exp_note_public",
    "remarks_internal": "exp_remarks_internal",
    "attachments": "exp_attachments",
    "signature": "exp_signature",
}


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _get_web_app_url() -> str:
    cfg = _read_json(CONFIG_PATH)
    secrets = st.secrets if hasattr(st, "secrets") else {}
    return (
        cfg.get("google", {}).get("apps_script_url")
        or secrets.get("APPS_SCRIPT_WEB_APP_URL", "")
    ).strip()


def _get_cloud_excel_url() -> str:
    cfg = _read_json(CONFIG_PATH)
    return str(cfg.get("ui", {}).get("cloud_excel_url", "")).strip()



EXPENSE_EXPORT_SCHEMA = [
    ("record_id", "表單編號"),
    ("status", "狀態"),
    ("form_type", "表單類型"),
    ("form_date", "填寫日期"),
    ("plan_code", "計畫代號"),
    ("purpose_desc", "用途說明"),
    ("employee_enabled", "員工姓名_是否勾選"),
    ("employee_name", "員工姓名"),
    ("employee_no", "工號"),
    ("advance_offset_enabled", "借支沖抵_是否勾選"),
    ("advance_amount", "借支金額"),
    ("offset_amount", "沖銷金額"),
    ("balance_refund_amount", "餘額退回"),
    ("supplement_amount", "應補差額"),
    ("vendor_enabled", "逕付廠商_是否勾選"),
    ("vendor_name", "逕付廠商"),
    ("vendor_address", "地址"),
    ("vendor_payee_name", "收款人"),
    ("receipt_count", "憑證編號"),
    ("amount_untaxed", "未稅金額"),
    ("tax_mode", "稅額方式"),
    ("tax_amount", "稅額"),
    ("amount_total", "金額"),
    ("handler_name", "經辦人"),
    ("project_manager_name", "計畫主管"),
    ("department_manager_name", "部門主管"),
    ("accountant_name", "會計"),
    ("department", "部門"),
    ("note_public", "備註"),
    ("remarks_internal", "內部備註"),
    ("owner_name", "擁有人"),
    ("user_email", "使用者Email"),
    ("actor_role", "角色"),
    ("source_system", "來源系統"),
    ("created_at", "建立時間"),
    ("created_by", "建立者"),
    ("updated_at", "更新時間"),
    ("updated_by", "更新者"),
    ("submitted_at", "送出時間"),
    ("submitted_by", "送出者"),
    ("is_deleted", "是否刪除"),
    ("deleted_at", "刪除時間"),
    ("deleted_by", "刪除者"),
]


def _build_schema_export_df(df: pd.DataFrame, schema: list[tuple[str, str]]) -> pd.DataFrame:
    src = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    data_rows = []
    for _, row in src.iterrows():
        data_rows.append({eng: row.get(eng, "") for eng, _ in schema})
    export_df = pd.DataFrame(data_rows, columns=[eng for eng, _ in schema])
    header_en = pd.DataFrame([[eng for eng, _ in schema]], columns=[eng for eng, _ in schema])
    header_zh = pd.DataFrame([[zh for _, zh in schema]], columns=[eng for eng, _ in schema])
    return pd.concat([header_en, header_zh, export_df], ignore_index=True)


def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "data") -> bytes:
    from io import BytesIO
    bio = BytesIO()
    export_df = _build_schema_export_df(df, EXPENSE_EXPORT_SCHEMA)
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        export_df.to_excel(writer, sheet_name=sheet_name[:31], index=False, header=False)
    bio.seek(0)
    return bio.getvalue()


def _split_expense_export_frames(actor: Actor) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_df, _ = load_records_cloud_or_backup(actor, status=None)
    if not isinstance(all_df, pd.DataFrame) or all_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    df = all_df.copy().fillna("")
    if "status" not in df.columns:
        df["status"] = "draft"
    status_series = df["status"].astype(str).str.lower()
    draft_df = df[status_series.isin(["draft", "deleted"])].copy()
    submitted_df = df[status_series.isin(["submitted", "void"])].copy()
    return draft_df, submitted_df


def _build_expense_workbook_bytes(actor: Actor) -> bytes:
    from io import BytesIO

    draft_df, submitted_df = _split_expense_export_frames(actor)

    draft_export = _build_schema_export_df(draft_df, EXPENSE_EXPORT_SCHEMA)
    submitted_export = _build_schema_export_df(submitted_df, EXPENSE_EXPORT_SCHEMA)

    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        submitted_export.to_excel(writer, sheet_name="申請列表", index=False, header=False)
        draft_export.to_excel(writer, sheet_name="草稿列表", index=False, header=False)
    bio.seek(0)
    return bio.getvalue()


@st.cache_resource(show_spinner=False)
def get_api() -> AppsScriptStorage:
    return AppsScriptStorage(web_app_url=_get_web_app_url(), system="expense", timeout=20)


def get_current_actor() -> Optional[Actor]:
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


def safe_float(v: Any) -> float:
    try:
        return float(v or 0)
    except Exception:
        return 0.0


def safe_int(v: Any) -> int:
    try:
        return int(round(float(v or 0)))
    except Exception:
        return 0


def normalize_date_value(v: Any) -> date:
    if isinstance(v, date):
        return v
    s = str(v or "").strip()
    if not s:
        return date.today()
    try:
        return datetime.fromisoformat(s.replace("/", "-")).date()
    except Exception:
        return date.today()


def to_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    if s in ["true", "1", "yes", "y"]:
        return True
    if s in ["false", "0", "no", "n"]:
        return False
    return default


def is_admin(actor: Actor) -> bool:
    return actor.role.strip().lower() == "admin"


def can_edit_record(actor: Actor, record: Dict[str, Any]) -> bool:
    return is_admin(actor) or str(record.get("user_email", "")).strip().lower() == actor.email


def can_delete_record(actor: Actor, record: Dict[str, Any]) -> bool:
    return is_admin(actor) or str(record.get("user_email", "")).strip().lower() == actor.email


def can_hard_delete(actor: Actor) -> bool:
    return is_admin(actor)


def load_options_with_fallback() -> Tuple[Dict[str, List[str]], str]:
    if "expense_options_grouped" in st.session_state:
        return st.session_state["expense_options_grouped"], st.session_state.get("expense_options_source", "session")
    try:
        grouped = get_api().get_all_options_grouped()
        if grouped:
            flat = []
            for k, vals in grouped.items():
                for v in vals:
                    flat.append({"option_type": k, "option_value": v})
            save_options_cache(flat)
            st.session_state["expense_options_grouped"] = grouped
            st.session_state["expense_options_source"] = "cloud"
            return grouped, "cloud"
    except Exception:
        pass
    cached = load_options_cache()
    grouped: Dict[str, List[str]] = {}
    for row in cached:
        k = str(row.get("option_type", "")).strip()
        v = str(row.get("option_value", "")).strip()
        if not k or not v:
            continue
        grouped.setdefault(k, [])
        if v not in grouped[k]:
            grouped[k].append(v)
    st.session_state["expense_options_grouped"] = grouped
    st.session_state["expense_options_source"] = "cache" if grouped else "empty"
    return grouped, ("cache" if grouped else "empty")


def load_defaults_with_fallback(email: str) -> Tuple[Dict[str, Any], str]:
    cache_key = f"expense_defaults_{email.lower()}"
    if cache_key in st.session_state:
        return st.session_state[cache_key], st.session_state.get(f"{cache_key}_source", "session")
    try:
        rows = get_api().user_defaults_list(email=email)
        if rows:
            all_rows = load_user_defaults_cache()
            merged = [r for r in all_rows if str(r.get("email", "")).strip().lower() != email.lower()]
            merged.extend(rows)
            save_user_defaults_cache(merged)
            st.session_state[cache_key] = rows[0]
            st.session_state[f"{cache_key}_source"] = "cloud"
            return rows[0], "cloud"
    except Exception:
        pass
    cached = load_user_defaults_cache()
    for row in cached:
        if str(row.get("email", "")).strip().lower() == email.lower():
            st.session_state[cache_key] = row
            st.session_state[f"{cache_key}_source"] = "cache"
            return row, "cache"
    st.session_state[cache_key] = {}
    st.session_state[f"{cache_key}_source"] = "empty"
    return {}, "empty"




def _expense_local_rows(actor: Actor) -> List[Dict[str, Any]]:
    if is_admin(actor):
        return load_local_expense_drafts() or []
    return load_local_expense_drafts(actor.email) or []


def _expense_cloud_rows(actor: Actor) -> List[Dict[str, Any]]:
    owner_only = not is_admin(actor)
    return get_api().record_list_all(actor=actor, status=None, owner_only=owner_only)


def _load_expense_master(actor: Actor, force_refresh: bool = False) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    cache_key = f"expense_master_cache::{actor.email}::{actor.role}"
    if (not force_refresh) and cache_key in st.session_state:
        return st.session_state[cache_key]
    df, report = build_master_dataframe(
        'expense',
        actor.email,
        fetch_cloud_rows=lambda: _expense_cloud_rows(actor),
        local_rows=_expense_local_rows(actor),
    )
    if not df.empty:
        df = df.copy().fillna('')
        if 'status' not in df.columns:
            df['status'] = 'draft'
        if 'owner_name' not in df.columns:
            df['owner_name'] = df.get('employee_name', '')
    st.session_state[cache_key] = (df, report)
    st.session_state['expense_sync_report'] = report
    st.session_state['cloud_online_expense'] = bool(report.get('cloud_online', False))
    return df, report


def _invalidate_expense_master(actor: Optional[Actor]) -> None:
    if not actor:
        return
    for suffix in [f"expense_master_cache::{actor.email}::{actor.role}", f"expense_master_cache::{actor.email}::admin", f"expense_master_cache::{actor.email}::user"]:
        st.session_state.pop(suffix, None)


def _queue_and_try_sync_expense(actor: Actor, operation: str, payload: Dict[str, Any]) -> Tuple[bool, str]:
    payload = dict(payload or {})
    payload['system_type'] = 'expense'
    payload['user_email'] = str(payload.get('user_email') or actor.email or '').strip().lower()
    queue_pending_sync(operation, {'email': actor.email, 'name': actor.name, 'role': actor.role}, payload, queue_owner_email=payload['user_email'])
    try:
        result = sync_pending_events('expense', actor, get_api())
        _invalidate_expense_master(actor)
        _load_expense_master(actor, force_refresh=True)
        if result.get('failed', 0) == 0:
            st.session_state['cloud_online_expense'] = True
            return True, ''
        st.session_state['cloud_online_expense'] = False
        return False, f"仍有 {result.get('remaining', 0)} 筆待同步"
    except Exception as exc:
        st.session_state['cloud_online_expense'] = False
        return False, str(exc)

def load_records_cloud_or_backup(actor: Actor, status: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
    try:
        df = get_api().records_df(actor=actor, status=status, owner_only=False).fillna("")
        local_rows = load_local_expense_drafts(actor.email)
        if local_rows:
            local_df = pd.DataFrame(local_rows).fillna("")
            if not df.empty:
                if status:
                    local_df = local_df[local_df.get("status", "").astype(str).str.lower() == status]
                df = pd.concat([df, local_df], ignore_index=True).fillna("")
                df = df.drop_duplicates(subset=["record_id"], keep="last") if "record_id" in df.columns else df
            elif status == "draft":
                df = local_df
                return df.fillna(""), "local"
        if status is None:
            df_copy = df.copy().fillna("")
            if not df_copy.empty:
                if "status" not in df_copy.columns:
                    df_copy["status"] = "draft"
                status_series = df_copy["status"].astype(str).str.lower()
                draft_cloud = df_copy[status_series.isin(["draft", "deleted"])].copy()
                submitted_cloud = df_copy[status_series.isin(["submitted", "void"])].copy()
            else:
                draft_cloud = pd.DataFrame()
                submitted_cloud = pd.DataFrame()
            save_cloud_backup_excel({"申請列表": submitted_cloud, "草稿列表": draft_cloud})
        elif status == "submitted":
            save_cloud_backup_excel({"申請列表": df})
        elif status == "draft":
            save_cloud_backup_excel({"草稿列表": df})
        return df, "cloud"
    except Exception:
        if status in {"draft", None}:
            local_rows = load_local_expense_drafts(actor.email)
            if local_rows:
                df_local = pd.DataFrame(local_rows).fillna("")
                if status:
                    status_series = df_local.get("status", pd.Series(dtype=str)).astype(str).str.lower()
                    df_local = df_local[status_series == status]
                return df_local, "local"
        if status == "submitted":
            df = load_backup_sheet_df("申請列表")
            if df.empty:
                df = load_backup_sheet_df("申請表單")
        else:
            df = load_backup_sheet_df("草稿列表")
        if not df.empty:
            return df.fillna(""), "backup"
        return pd.DataFrame(), "empty"


def refresh_runtime_cache(actor: Actor) -> None:
    st.session_state.pop("expense_options_grouped", None)
    st.session_state.pop("expense_options_source", None)
    cache_key = f"expense_defaults_{actor.email.lower()}"
    st.session_state.pop(cache_key, None)
    st.session_state.pop(f"{cache_key}_source", None)
    _invalidate_expense_master(actor)


def _expense_archive_restore_status(row: Dict[str, Any]) -> str:
    status = str((row or {}).get("status", "")).strip().lower()
    return "submitted" if status in {"submitted", "void"} else "draft"


def _expense_restore_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(row or {})
    for k in [
        "archive_system_type", "archive_actor_email", "archived_at", "archive_id",
        "archive_restored", "restored_at", "restored_by", "restore_target_status",
    ]:
        payload.pop(k, None)
    payload["status"] = _expense_archive_restore_status(row)
    return payload



def _expense_pending_items(owner_email: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for item in load_pending_sync(owner_email) or []:
        payload = dict(item.get("payload") or {})
        system_type = str(payload.get("system_type") or ("travel" if "travel" in str(item.get("operation", "")).lower() else "expense")).lower()
        if system_type == "expense":
            items.append(item)
    return items



def _expense_raw_pending_count(owner_email: str) -> int:
    rows = []
    for item in _expense_pending_items(owner_email):
        payload = dict(item.get("payload") or {})
        sync_status = str(item.get("sync_status") or payload.get("sync_status") or "pending").lower()
        needs_sync = bool(payload.get("needs_sync", True))
        if needs_sync and sync_status in {"pending", "failed", "conflict"}:
            rows.append(item)
    return len(rows)



def _cleanup_stale_expense_pending(actor: Actor) -> int:
    report = st.session_state.get("expense_sync_report", {}) or {}
    raw_pending = _expense_raw_pending_count(actor.email)
    report_pending = int(report.get("pending_count", 0) or 0)
    cloud_online = bool(report.get("cloud_online", False))
    if not cloud_online or raw_pending <= report_pending:
        return 0
    removed = 0
    for item in _expense_pending_items(actor.email):
        payload = dict(item.get("payload") or {})
        sync_status = str(item.get("sync_status") or payload.get("sync_status") or "pending").lower()
        if sync_status == "conflict":
            continue
        event_id = str(item.get("event_id") or payload.get("event_id") or "").strip()
        record_id = str(payload.get("record_id") or "").strip()
        if event_id:
            removed += remove_pending_sync_item(actor.email, event_id=event_id, system_type="expense")
        elif record_id:
            removed += remove_pending_sync_item(actor.email, record_id=record_id, system_type="expense")
    return removed



def _render_deleted_archive_restore_expense(actor: Actor) -> None:
    if str(actor.role).lower() != "admin":
        return
    rows = load_deleted_archive_rows(system_type="expense", include_restored=False)
    st.sidebar.markdown("---")
    st.sidebar.subheader("deleted archive 還原")
    if not rows:
        st.sidebar.info("目前沒有可還原的支出刪除備份。")
        return
    options: Dict[str, Dict[str, Any]] = {}
    labels: List[str] = []
    for row in rows[:100]:
        rid = str(row.get("record_id", "")).strip() or "(無編號)"
        owner = str(row.get("user_email", "")).strip().lower()
        archived_at = str(row.get("archived_at", "")).strip()
        raw_status = str(row.get("status", "")).strip().lower()
        restore_status = _expense_archive_restore_status(row)
        label = f"{rid}｜{owner or '-'}｜原狀態:{raw_status or '-'} → 還原為:{restore_status}｜{archived_at}"
        labels.append(label)
        options[label] = row
    selected = st.sidebar.selectbox("選擇要還原的支出紀錄", labels, key="expense_archive_restore_select")
    selected_row = options.get(selected) if selected else None
    if selected_row and st.sidebar.button("一鍵還原支出紀錄", key="expense_archive_restore_btn", use_container_width=True):
        payload = _expense_restore_payload(selected_row)
        target_status = str(payload.get("status", "draft")).strip().lower() or "draft"
        owner_email = str(payload.get("user_email") or actor.email or "").strip().lower()
        upsert_local_expense_draft(owner_email, payload)
        ok, msg = _queue_and_try_sync_expense(actor, f"expense_restore_{target_status}", payload)
        mark_deleted_archive_restored(str(selected_row.get("archive_id", "")), restored_by=actor.email, restore_target_status=target_status)
        refresh_runtime_cache(actor)
        if ok:
            st.sidebar.success(f"已還原：{payload.get('record_id','')}")
        else:
            st.sidebar.warning(f"已加入待同步還原：{msg or '請稍後重新同步'}")
        st.rerun()


def render_sync_status_sidebar_expense(current_user_email: str) -> None:
    if not current_user_email:
        return
    actor = get_current_actor() or Actor(name="", email=current_user_email, role="user")
    st.sidebar.markdown("---")
    st.sidebar.subheader("雲端同步狀態")

    _, report = _load_expense_master(actor, force_refresh=False)
    raw_pending_count = _expense_raw_pending_count(current_user_email)
    report_pending_count = int(report.get("pending_count", 0) or 0)
    stale_queue_detected = raw_pending_count != report_pending_count

    cloud_online = bool(report.get("cloud_online", st.session_state.get("cloud_online_expense", True)))
    st.session_state["cloud_online_expense"] = cloud_online
    if cloud_online:
        st.sidebar.success("雲端：已連線")
    else:
        st.sidebar.error("雲端：未連線")

    if report_pending_count > 0:
        st.sidebar.warning(f"你有 {report_pending_count} 筆支出資料尚未同步到雲端")
    else:
        st.sidebar.success("你的支出資料皆已同步")

    if stale_queue_detected:
        st.sidebar.warning("偵測到本機待同步殘留，已建議清理")

    cloud_url = _get_cloud_excel_url()
    if cloud_url:
        st.sidebar.link_button("開啟雲端表單", cloud_url, use_container_width=True)
    st.sidebar.link_button("開啟附件雲端資料夾", EXPENSE_ATTACHMENTS_ROOT_URL, use_container_width=True)

    _load_expense_master(actor, force_refresh=False)
    draft_cloud_df, submitted_cloud_df = _split_expense_export_frames(actor)
    save_cloud_backup_excel({"申請列表": submitted_cloud_df, "草稿列表": draft_cloud_df})

    st.sidebar.download_button(
        "下載Excel",
        data=_build_expense_workbook_bytes(actor),
        file_name="支出報帳.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key="expense_sidebar_download_excel",
    )

    _render_deleted_archive_restore_expense(actor)

    st.sidebar.caption(f"master={report.get('master_count', 0)}｜cloud={report.get('cloud_count', 0)}｜pending={report_pending_count}")
    if report.get('cloud_count', 0) != report.get('master_count', 0) and report_pending_count == 0:
        st.sidebar.warning('偵測到雲端與前端筆數不一致，建議重新同步或重新整理。')

    if st.sidebar.button("立即同步支出資料", key="sync_expense_now_btn", use_container_width=True):
        try:
            sync_actor = get_current_actor() or Actor(name='', email=current_user_email, role='user')
            result = sync_pending_events('expense', sync_actor, get_api())
            _invalidate_expense_master(sync_actor)
            _, report = _load_expense_master(sync_actor, force_refresh=True)
            st.session_state['cloud_online_expense'] = result.get('failed', 0) == 0
            cleanup_removed = 0
            if result.get('failed', 0) == 0 and result.get('conflicts', 0) == 0:
                cleanup_removed = _cleanup_stale_expense_pending(sync_actor)
                if cleanup_removed:
                    _invalidate_expense_master(sync_actor)
                    _, report = _load_expense_master(sync_actor, force_refresh=True)
            if result.get('synced', 0) == 0 and result.get('failed', 0) == 0 and report.get('pending_count', 0) == 0:
                msg = "目前沒有待同步的支出資料。"
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
            st.session_state['cloud_online_expense'] = False
            st.sidebar.error(f"同步失敗：{e}")




def render_top_sync_notice_expense(current_user_email: str) -> None:
    if not current_user_email:
        return
    actor = get_current_actor() or Actor(name="", email=current_user_email, role="user")
    _, report = _load_expense_master(actor, force_refresh=False)
    pending_count = int(report.get("pending_count", 0) or 0)
    if pending_count > 0:
        st.info(f"提醒：你有 {pending_count} 筆支出資料尚未同步到雲端。")
    elif _expense_raw_pending_count(current_user_email) != pending_count:
        st.info("提醒：偵測到本機待同步殘留，已建議清理。")



def option_values(grouped: Dict[str, List[str]], option_type: str, include_other: bool = True) -> List[str]:
    values = grouped.get(option_type, []).copy()
    if option_type == "plan_code":
        return get_shared_plan_code_options(values, include_other=include_other)
    if include_other and "其他" not in values:
        values.append("其他")
    return values


def _form_key(actor: Actor) -> str:
    return f"expense_form_data::{actor.email}"


def _edit_key(actor: Actor) -> str:
    return f"expense_editing_record_id::{actor.email}"


def default_form(actor: Actor, defaults: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "record_id": "",
        "form_date": date.today(),
        "plan_code": defaults.get("default_plan_code", ""),
        "purpose_desc": "",
        "payment_target": "employee",
        "employee_name": "",
        "employee_no": "",
        "advance_amount": 0,
        "offset_amount": 0,
        "balance_refund_amount": 0,
        "supplement_amount": 0,
        "vendor_name": "",
        "vendor_address": "",
        "vendor_payee_name": "",
        "receipt_count": 0,
        "amount_untaxed": 0,
        "tax_mode": "5%",
        "tax_amount": 0,
        "amount_total": 0,
        "department": defaults.get("default_department") or actor.department or "化安處",
        "handler_name": "",
        "project_manager_name": "",
        "department_manager_name": "",
        "accountant_name": "",
        "note_public": defaults.get("default_note_public", "憑證正本請黏貼於此頁下方；會議請填寫出席人員於用途說明"),
        "remarks_internal": "",
        "owner_name": actor.name,
        "user_email": actor.email,
        "attachment_files": [],
        "signature_file": {},
    }


def set_form_data(actor: Actor, form_data: Dict[str, Any]) -> None:
    st.session_state[_form_key(actor)] = form_data


def get_form_data(actor: Actor, defaults: Dict[str, Any]) -> Dict[str, Any]:
    key = _form_key(actor)
    if key not in st.session_state:
        st.session_state[key] = default_form(actor, defaults)
    return st.session_state[key]


def _set_widget_defaults(form_data: Dict[str, Any], grouped_options: Dict[str, List[str]]) -> None:
    d = form_data
    keys = EXPENSE_WIDGET_KEYS
    st.session_state.setdefault(keys["form_date"], normalize_date_value(d.get("form_date")))
    st.session_state.setdefault(keys["purpose_desc"], str(d.get("purpose_desc", "")))
    st.session_state.setdefault(keys["payment_target"], str(d.get("payment_target", "employee") or "employee"))
    st.session_state.setdefault(keys["advance_amount"], safe_int(d.get("advance_amount")))
    st.session_state.setdefault(keys["offset_amount"], safe_int(d.get("offset_amount")))
    st.session_state.setdefault(keys["balance_refund_amount"], safe_int(d.get("balance_refund_amount")))
    st.session_state.setdefault(keys["supplement_amount"], safe_int(d.get("supplement_amount")))
    st.session_state.setdefault(keys["vendor_name"], str(d.get("vendor_name", "")))
    st.session_state.setdefault(keys["vendor_address"], str(d.get("vendor_address", "")))
    st.session_state.setdefault(keys["vendor_payee_name"], str(d.get("vendor_payee_name", "")))
    st.session_state.setdefault(keys["receipt_count"], safe_int(d.get("receipt_count")))
    st.session_state.setdefault(keys["amount_untaxed"], safe_int(d.get("amount_untaxed")))
    st.session_state.setdefault(keys["tax_mode"], str(d.get("tax_mode", "5%") or "5%"))
    st.session_state.setdefault(keys["department"], str(d.get("department", "化安處")))
    st.session_state.setdefault(keys["note_public"], str(d.get("note_public", "")))
    st.session_state.setdefault(keys["remarks_internal"], str(d.get("remarks_internal", "")))
    plan_opts = option_values(grouped_options, "plan_code")
    plan_val = str(d.get("plan_code", "")).strip()
    st.session_state.setdefault(keys["plan_code"], plan_val if plan_val in plan_opts else "其他")
    st.session_state.setdefault(keys["plan_code_other"], "" if plan_val in plan_opts else plan_val)
    emp_name_opts = option_values(grouped_options, "employee_name")
    emp_no_opts = option_values(grouped_options, "employee_no")
    emp_name = str(d.get("employee_name", "")).strip()
    emp_no = str(d.get("employee_no", "")).strip()
    first_emp_name = emp_name_opts[0] if emp_name_opts else ""
    first_emp_no = emp_no_opts[0] if emp_no_opts else ""
    st.session_state.setdefault(keys["employee_name"], emp_name if emp_name in emp_name_opts else (first_emp_name if emp_name == "" else "其他"))
    st.session_state.setdefault(keys["employee_name_other"], "" if emp_name in emp_name_opts else emp_name)
    st.session_state.setdefault(keys["employee_no"], emp_no if emp_no in emp_no_opts else (first_emp_no if emp_no == "" else "其他"))
    st.session_state.setdefault(keys["employee_no_other"], "" if emp_no in emp_no_opts else emp_no)


def _reset_widget_defaults(form_data: Dict[str, Any], grouped_options: Dict[str, List[str]]) -> None:
    for k in EXPENSE_WIDGET_KEYS.values():
        st.session_state.pop(k, None)
    _set_widget_defaults(form_data, grouped_options)


def clear_form(actor: Actor, defaults: Dict[str, Any], grouped_options: Dict[str, List[str]]) -> None:
    form = default_form(actor, defaults)
    set_form_data(actor, form)
    st.session_state[_edit_key(actor)] = ""
    _reset_widget_defaults(form, grouped_options)


def load_record_into_form(record: Dict[str, Any], actor: Actor, grouped_options: Dict[str, List[str]]) -> None:
    payment_target = "employee"
    if to_bool(record.get("advance_offset_enabled"), False):
        payment_target = "advance"
    elif to_bool(record.get("vendor_enabled"), False):
        payment_target = "vendor"
    elif str(record.get("payment_target", "")).strip() in {"employee", "advance", "vendor"}:
        payment_target = str(record.get("payment_target", "")).strip()
    form_data = {
        "record_id": record.get("record_id", ""),
        "form_date": normalize_date_value(record.get("form_date", "")),
        "plan_code": record.get("plan_code", ""),
        "purpose_desc": record.get("purpose_desc", ""),
        "payment_target": payment_target,
        "employee_name": record.get("employee_name", ""),
        "employee_no": str(record.get("employee_no", "")),
        "advance_amount": safe_int(record.get("advance_amount")),
        "offset_amount": safe_int(record.get("offset_amount")),
        "balance_refund_amount": safe_int(record.get("balance_refund_amount")),
        "supplement_amount": safe_int(record.get("supplement_amount")),
        "vendor_name": record.get("vendor_name", ""),
        "vendor_address": record.get("vendor_address", ""),
        "vendor_payee_name": record.get("vendor_payee_name", ""),
        "receipt_count": safe_int(record.get("receipt_count")),
        "amount_untaxed": safe_int(record.get("amount_untaxed")),
        "tax_mode": str(record.get("tax_mode", "5%")) if str(record.get("tax_mode", "")) else ("免稅" if safe_float(record.get("tax_amount")) == 0 else "5%"),
        "tax_amount": safe_int(record.get("tax_amount")),
        "amount_total": safe_int(record.get("amount_total")),
        "department": record.get("department", "化安處"),
        "handler_name": "",
        "project_manager_name": "",
        "department_manager_name": "",
        "accountant_name": "",
        "note_public": record.get("note_public", ""),
        "remarks_internal": record.get("remarks_internal", ""),
        "owner_name": record.get("owner_name", actor.name),
        "user_email": record.get("user_email", actor.email),
        "attachment_files": record.get("attachment_files", []),
        "signature_file": record.get("signature_file", {}),
    }
    set_form_data(actor, form_data)
    st.session_state[_edit_key(actor)] = record.get("record_id", "")
    _reset_widget_defaults(form_data, grouped_options)
    st.session_state["expense_page"] = "new"


def copy_record_into_form(record: Dict[str, Any], actor: Actor, grouped_options: Dict[str, List[str]]) -> None:
    copied = dict(record)
    copied["record_id"] = ""
    copied["form_date"] = date.today().isoformat()
    copied["owner_name"] = actor.name
    copied["user_email"] = actor.email
    for k in ["status", "created_at", "updated_at", "modified_at", "submitted_at", "deleted_at", "voided_at"]:
        copied.pop(k, None)
    load_record_into_form(copied, actor, grouped_options)
    st.session_state[_edit_key(actor)] = ""
    st.session_state["expense_page"] = "new"


def _upload_file_to_drive(actor: Actor, up, category: str, record_id: str = '') -> Dict[str, Any]:
    if hasattr(get_api(), 'upload_drive_file'):
        return get_api().upload_drive_file(
            actor,
            filename=getattr(up, 'name', 'file.bin'),
            file_bytes=up.getvalue(),
            mime_type=getattr(up, 'type', 'application/octet-stream') or 'application/octet-stream',
            category=category,
            record_id=record_id,
            owner_email=actor.email,
        )
    return save_uploaded_attachment(actor.email, up, category)


def _delete_attachment_meta(actor: Actor, meta: Dict[str, Any]) -> None:
    drive_file_id = str((meta or {}).get('drive_file_id', '')).strip()
    if drive_file_id and hasattr(get_api(), 'delete_drive_file'):
        try:
            get_api().delete_drive_file(actor, drive_file_id)
            return
        except Exception:
            pass
    delete_saved_file(meta)


def _download_attachment_bytes(actor: Actor, meta: Dict[str, Any]) -> Tuple[bytes, str, str]:
    drive_file_id = str((meta or {}).get('drive_file_id', '')).strip()
    if drive_file_id and hasattr(get_api(), 'download_drive_file'):
        data = get_api().download_drive_file(actor, drive_file_id)
        return data.get('file_bytes', b''), str(data.get('name') or meta.get('name') or 'attachment.bin'), str(data.get('mime_type') or meta.get('mime_type') or 'application/octet-stream')
    path = str((meta or {}).get('path', '')).strip()
    if path and Path(path).exists():
        return Path(path).read_bytes(), Path(path).name, str(meta.get('mime_type') or 'application/octet-stream')
    return b'', str(meta.get('name') or 'attachment.bin'), str(meta.get('mime_type') or 'application/octet-stream')


def remove_attachment_from_form(actor: Actor, idx: int) -> None:
    form_data = dict(st.session_state.get(_form_key(actor), {}) or {})
    files = list(form_data.get("attachment_files", []) or [])
    if 0 <= idx < len(files):
        meta = files.pop(idx)
        _delete_attachment_meta(actor, meta)
        form_data["attachment_files"] = files
        set_form_data(actor, form_data)


def remove_signature_from_form(actor: Actor) -> None:
    form_data = dict(st.session_state.get(_form_key(actor), {}) or {})
    _delete_attachment_meta(actor, form_data.get("signature_file", {}))
    form_data["signature_file"] = {}
    set_form_data(actor, form_data)


def render_header() -> None:
    st.markdown(
        """
        <style>
        .exp-hero-title{font-size:42px;font-weight:800;line-height:1.1;margin:0 0 10px 0;color:#1f2937;}
        .exp-hero-sub{font-size:14px;color:#6b7280;margin-bottom:18px;}
        .exp-card{border:1px solid rgba(15,23,42,.12);border-radius:16px;padding:18px 18px 6px 18px;margin-bottom:18px;background:#ffffff;box-shadow:0 1px 2px rgba(15,23,42,.03);}
        .exp-card-title{font-size:28px;font-weight:800;color:#1f2a44;margin-bottom:8px;}
        .exp-card-desc{color:#6b7280;margin-bottom:14px;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="exp-hero-title">💰 支出報帳</div>', unsafe_allow_html=True)
    st.markdown('<div class="exp-hero-sub">支援草稿、附件、數位簽名檔與 PDF 匯出。</div>', unsafe_allow_html=True)


def card_open(title: str, desc: str = "") -> None:
    st.markdown('<div class="exp-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="exp-card-title">{title}</div>', unsafe_allow_html=True)
    if desc:
        st.markdown(f'<div class="exp-card-desc">{desc}</div>', unsafe_allow_html=True)


def card_close() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def _select_or_other_live(label: str, options: List[str], select_key: str, other_key: str) -> str:
    opts = options or ["其他"]
    if "其他" not in opts:
        opts = opts + ["其他"]
    selected = st.selectbox(label, opts, key=select_key)
    if selected == "其他":
        return st.text_input(f"{label}（其他）", key=other_key, placeholder="請輸入其他內容").strip()
    return str(selected).strip()


def _persist_uploaded_files(actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(payload)
    existing_attachments = payload.get("attachment_files", []) or []
    sig_meta = payload.get("signature_file", {}) or {}
    uploaded_attachments = st.session_state.get(EXPENSE_WIDGET_KEYS["attachments"], []) or []
    for up in uploaded_attachments:
        marker = (up.name, len(up.getvalue()))
        if any((r.get("name"), int(r.get("size", 0))) == marker for r in existing_attachments if str(r.get("size", "")).isdigit()):
            continue
        existing_attachments.append(_upload_file_to_drive(actor, up, 'attachment', record_id=str(payload.get('record_id') or '')))
    uploaded_sig = st.session_state.get(EXPENSE_WIDGET_KEYS['signature'])
    if uploaded_sig is not None:
        sig_meta = _upload_file_to_drive(actor, uploaded_sig, 'signature', record_id=str(payload.get('record_id') or ''))
    payload["attachment_files"] = existing_attachments
    payload["signature_file"] = sig_meta
    return payload


def _current_payload(actor: Actor, form_data: Dict[str, Any], grouped_options: Dict[str, List[str]]) -> Dict[str, Any]:
    keys = EXPENSE_WIDGET_KEYS
    amount_untaxed = safe_int(st.session_state.get(keys["amount_untaxed"], 0))
    tax_mode = str(st.session_state.get(keys["tax_mode"], "5%"))
    tax_amount = safe_int(round(amount_untaxed * 0.05)) if tax_mode == "5%" else 0
    amount_total = amount_untaxed + tax_amount
    payment_target = str(st.session_state.get(keys["payment_target"], "employee"))
    plan_code = _select_or_value_for_payload("plan_code")
    department = _select_or_value_for_payload("department")
    employee_name = _select_or_value_for_payload("employee_name") if payment_target == "employee" else ""
    employee_no = _select_or_value_for_payload("employee_no") if payment_target == "employee" else ""
    payload = {
        "record_id": st.session_state.get(_edit_key(actor), "") or str(form_data.get("record_id", "")),
        "form_date": str(st.session_state.get(keys["form_date"], date.today())),
        "plan_code": plan_code,
        "purpose_desc": str(st.session_state.get(keys["purpose_desc"], "")),
        "payment_target": payment_target,
        "employee_enabled": payment_target == "employee",
        "employee_name": employee_name,
        "employee_no": employee_no,
        "advance_offset_enabled": payment_target == "advance",
        "advance_amount": safe_int(st.session_state.get(keys["advance_amount"], 0)) if payment_target == "advance" else 0,
        "offset_amount": safe_int(st.session_state.get(keys["offset_amount"], 0)) if payment_target == "advance" else 0,
        "balance_refund_amount": safe_int(st.session_state.get(keys["balance_refund_amount"], 0)) if payment_target == "advance" else 0,
        "supplement_amount": safe_int(st.session_state.get(keys["supplement_amount"], 0)) if payment_target == "advance" else 0,
        "vendor_enabled": payment_target == "vendor",
        "vendor_name": str(st.session_state.get(keys["vendor_name"], "")) if payment_target == "vendor" else "",
        "vendor_address": str(st.session_state.get(keys["vendor_address"], "")) if payment_target == "vendor" else "",
        "vendor_payee_name": str(st.session_state.get(keys["vendor_payee_name"], "")) if payment_target == "vendor" else "",
        "receipt_count": safe_int(st.session_state.get(keys["receipt_count"], 0)),
        "amount_untaxed": amount_untaxed,
        "tax_mode": tax_mode,
        "tax_amount": tax_amount,
        "amount_total": amount_total,
        "department": department or "化安處",
        "handler_name": "",
        "project_manager_name": "",
        "department_manager_name": "",
        "accountant_name": "",
        "note_public": str(st.session_state.get(keys["note_public"], "")),
        "remarks_internal": str(st.session_state.get(keys["remarks_internal"], "")),
        "owner_name": actor.name,
        "user_email": actor.email,
        "attachment_files": form_data.get("attachment_files", []),
        "signature_file": form_data.get("signature_file", {}),
    }
    return payload


def _select_or_value_for_payload(field_name: str) -> str:
    keys = EXPENSE_WIDGET_KEYS
    select_key = keys[field_name]
    other_key = f"{select_key}_other" if field_name not in {"plan_code", "department", "employee_name", "employee_no"} else keys.get(f"{field_name}_other", f"{select_key}_other")
    selected = st.session_state.get(select_key, "")
    if selected == "其他":
        return str(st.session_state.get(other_key, "")).strip()
    return str(selected).strip()


def _prepare_pdf_bytes(payload: Dict[str, Any]) -> bytes:
    attachment_paths: List[str] = []
    actor = get_current_actor()
    for idx, x in enumerate(payload.get('attachment_files', []) or []):
        if not isinstance(x, dict):
            p = str(x).strip()
            if p:
                attachment_paths.append(p)
            continue
        p = str(x.get('path', '')).strip()
        if p and Path(p).exists():
            attachment_paths.append(p)
            continue
        drive_file_id = str(x.get('drive_file_id', '')).strip()
        if actor and drive_file_id and hasattr(get_api(), 'download_drive_file'):
            try:
                data = get_api().download_drive_file(actor, drive_file_id)
                file_bytes = data.get('file_bytes', b'')
                if file_bytes:
                    suffix = Path(str(data.get('name') or x.get('name') or f'attachment_{idx}.bin')).suffix or '.bin'
                    tmp = Path(tempfile.gettempdir()) / f"expense_attach_{drive_file_id}{suffix}"
                    tmp.write_bytes(file_bytes)
                    attachment_paths.append(str(tmp))
            except Exception:
                pass
    main = build_pdf_bytes(payload)
    return merge_expense_pdf_with_attachments(main, attachment_paths)


def render_form_page(grouped_options: Dict[str, List[str]], defaults: Dict[str, Any]) -> None:
    form_data = get_form_data(actor, defaults)
    _set_widget_defaults(form_data, grouped_options)

    employee_name_options = option_values(grouped_options, "employee_name")
    employee_no_options = option_values(grouped_options, "employee_no")
    department_options = option_values(grouped_options, "department")
    plan_code_options = option_values(grouped_options, "plan_code")

    card_open("👤 1. 基本資料與用途說明", "＊為必填。")
    c1, c2 = st.columns(2)
    with c1:
        st.date_input("填寫日期", key=EXPENSE_WIDGET_KEYS["form_date"])
    with c2:
        _select_or_other_live("計畫代號", plan_code_options, EXPENSE_WIDGET_KEYS["plan_code"], EXPENSE_WIDGET_KEYS["plan_code_other"])
    st.text_area("用途說明 *", key=EXPENSE_WIDGET_KEYS["purpose_desc"], height=100)
    card_close()

    card_open("💳 2. 付款對象", "付款對象僅能三擇一，並只顯示對應欄位。")
    st.radio(
        "付款對象",
        options=["employee", "advance", "vendor"],
        format_func=lambda x: {"employee": "員工姓名", "advance": "借支沖抵", "vendor": "逕付廠商"}[x],
        horizontal=True,
        key=EXPENSE_WIDGET_KEYS["payment_target"],
    )
    payment_target = st.session_state.get(EXPENSE_WIDGET_KEYS["payment_target"], "employee")
    if payment_target == "employee":
        c1, c2 = st.columns(2)
        with c1:
            _select_or_other_live("員工姓名", employee_name_options, EXPENSE_WIDGET_KEYS["employee_name"], EXPENSE_WIDGET_KEYS["employee_name_other"])
        with c2:
            _select_or_other_live("工號", employee_no_options, EXPENSE_WIDGET_KEYS["employee_no"], EXPENSE_WIDGET_KEYS["employee_no_other"])
    elif payment_target == "advance":
        cols = st.columns(4)
        cols[0].number_input("借支金額", min_value=0, step=1, key=EXPENSE_WIDGET_KEYS["advance_amount"])
        cols[1].number_input("沖銷金額", min_value=0, step=1, key=EXPENSE_WIDGET_KEYS["offset_amount"])
        cols[2].number_input("餘額退回", min_value=0, step=1, key=EXPENSE_WIDGET_KEYS["balance_refund_amount"])
        cols[3].number_input("應補差額", min_value=0, step=1, key=EXPENSE_WIDGET_KEYS["supplement_amount"])
    else:
        st.text_input("逕付廠商", key=EXPENSE_WIDGET_KEYS["vendor_name"])
        st.text_input("地址", key=EXPENSE_WIDGET_KEYS["vendor_address"])
        st.text_input("收款人", key=EXPENSE_WIDGET_KEYS["vendor_payee_name"])
    st.number_input("憑證編號", min_value=0, step=1, key=EXPENSE_WIDGET_KEYS["receipt_count"])
    card_close()

    card_open("🧮 3. 金額資訊", "稅額與總金額依未稅金額及稅額方式自動計算。")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.number_input("未稅金額", min_value=0, step=1, key=EXPENSE_WIDGET_KEYS["amount_untaxed"])
    with c2:
        st.selectbox("稅額方式", options=["5%", "免稅"], key=EXPENSE_WIDGET_KEYS["tax_mode"])
    untaxed = safe_int(st.session_state.get(EXPENSE_WIDGET_KEYS["amount_untaxed"], 0))
    tax_mode = st.session_state.get(EXPENSE_WIDGET_KEYS["tax_mode"], "5%")
    tax_amount = safe_int(round(untaxed * 0.05)) if tax_mode == "5%" else 0
    amount_total = untaxed + tax_amount
    with c3:
        st.number_input("稅額", value=tax_amount, min_value=0, step=1, disabled=True)
    st.number_input("總金額", value=amount_total, min_value=0, step=1, disabled=True)
    card_close()

    card_open("📎 4. 附件與簽名")
    st.file_uploader("上傳附件", type=["pdf", "png", "jpg", "jpeg", "webp", "bmp"], accept_multiple_files=True, key=EXPENSE_WIDGET_KEYS["attachments"])
    st.file_uploader("上傳數位簽名檔", type=["png", "jpg", "jpeg", "webp", "bmp"], accept_multiple_files=False, key=EXPENSE_WIDGET_KEYS["signature"])
    existing_atts = list(form_data.get("attachment_files", []) or [])
    if existing_atts:
        st.caption('已附附件')
        for idx, att in enumerate(existing_atts):
            ac1, ac2, ac3, ac4 = st.columns([4, 1, 1, 1])
            ac1.write(f"{idx + 1}. {att.get('name', '')}")
            drive_url = str(att.get('drive_url', '')).strip() if isinstance(att, dict) else ''
            if drive_url:
                ac2.link_button('預覽', drive_url, use_container_width=True)
            else:
                ac2.write('')
            file_bytes, file_name, mime_type = _download_attachment_bytes(actor, att)
            if file_bytes:
                ac3.download_button('下載', data=file_bytes, file_name=file_name, mime=mime_type, key=f'actual_att_dl_{idx}', use_container_width=True)
            else:
                ac3.caption('無法下載')
            if ac4.button('移除', key=f'remove_att_{idx}', use_container_width=True):
                remove_attachment_from_form(actor, idx)
                st.rerun()
    else:
        st.caption('目前沒有已附附件。')
    sig_meta = form_data.get('signature_file', {}) or {}
    if sig_meta:
        sc1, sc2, sc3, sc4 = st.columns([4, 1, 1, 1])
        sc1.write(f"數位簽名檔：{sig_meta.get('name', '')}")
        drive_url = str(sig_meta.get('drive_url', '')).strip() if isinstance(sig_meta, dict) else ''
        if drive_url:
            sc2.link_button('預覽', drive_url, use_container_width=True)
        sig_bytes, sig_name, sig_mime = _download_attachment_bytes(actor, sig_meta)
        if sig_bytes:
            sc3.download_button('下載', data=sig_bytes, file_name=sig_name, mime=sig_mime, key='sig_download_actual', use_container_width=True)
        else:
            sc3.caption('無法下載')
        if sc4.button('移除', key='remove_signature', use_container_width=True):
            remove_signature_from_form(actor)
            st.rerun()
    card_close()

    card_open("📝 5. 其他資訊")
    _select_or_other_live("部門", department_options or ["化安處"], EXPENSE_WIDGET_KEYS["department"], EXPENSE_WIDGET_KEYS["department_other"])
    st.text_area("備註", key=EXPENSE_WIDGET_KEYS["note_public"], height=80)
    st.text_area("內部備註", key=EXPENSE_WIDGET_KEYS["remarks_internal"], height=80)
    card_close()

    payload = _current_payload(actor, form_data, grouped_options)
    set_form_data(actor, payload)
    st.session_state["expense_sidebar_export_df"] = pd.DataFrame([payload])

    pdf_payload = _persist_uploaded_files(actor, payload)
    pdf_bytes = _prepare_pdf_bytes(pdf_payload)

    c1, c2, c3, c4, c5 = st.columns(5)
    if c1.button("儲存草稿", use_container_width=True):
        save_payload = _persist_uploaded_files(actor, payload)
        save_payload["status"] = "draft"
        old_id = str(save_payload.get("record_id") or "").strip()
        local_id = upsert_local_expense_draft(actor.email, save_payload)
        save_payload["record_id"] = local_id
        set_form_data(actor, save_payload)
        st.session_state[_edit_key(actor)] = local_id
        ok, msg = _queue_and_try_sync_expense(actor, 'expense_draft', save_payload)
        if old_id and old_id != local_id:
            remove_local_expense_draft(actor.email, old_id)
        refresh_runtime_cache(actor)
        st.session_state["expense_page"] = "drafts"
        if ok:
            st.success("草稿已儲存並同步。")
        else:
            st.warning(f"草稿已先保存在本機待同步：{msg or '請稍後重新同步'}")
        st.rerun()
    if c2.button("確認無誤並送出", use_container_width=True, type="primary"):
        submit_payload = _persist_uploaded_files(actor, payload)
        submit_payload["status"] = "submitted"
        local_id = upsert_local_expense_draft(actor.email, submit_payload)
        submit_payload["record_id"] = local_id
        set_form_data(actor, submit_payload)
        st.session_state[_edit_key(actor)] = local_id
        ok, msg = _queue_and_try_sync_expense(actor, 'expense_submit', submit_payload)
        refresh_runtime_cache(actor)
        st.session_state["expense_page"] = "submitted"
        if ok:
            st.success("表單已送出並同步。")
        else:
            st.error(f"送出已保存在本機待同步：{msg or '請稍後重新同步'}")
        st.rerun()
    c3.download_button("下載PDF", data=pdf_bytes, file_name=f"支出報帳_{payload.get('record_id') or 'preview'}.pdf", mime="application/pdf", use_container_width=True)
    if c4.button("複製本表單", use_container_width=True):
        copy_payload = _persist_uploaded_files(actor, payload)
        copy_record_into_form(copy_payload, actor, grouped_options)
        st.success("已複製為新表單，可直接修改後再儲存或送出。")
        st.rerun()
    if c5.button("返回列表", use_container_width=True):
        st.session_state["expense_page"] = "all"
        st.rerun()
    record_status = str(form_data.get("status") or "draft").lower()
    extra1, extra2 = st.columns(2)
    delete_label = "作廢此筆" if record_status in {"submitted", "void"} else "刪除此筆"
    if extra1.button(delete_label, use_container_width=True):
        current_id = str(payload.get("record_id") or "").strip()
        payload["status"] = "void" if record_status in {"submitted", "void"} else "deleted"
        if current_id:
            upsert_local_expense_draft(actor.email, payload)
            _queue_and_try_sync_expense(actor, 'expense_soft_delete', payload)
        else:
            upsert_local_expense_draft(actor.email, payload)
        st.session_state["expense_page"] = "submitted" if record_status in {"submitted", "void"} else "drafts"
        st.success(f"已{delete_label.replace('此筆', '')}。")
        st.rerun()
    if extra2.button("清空新增", use_container_width=True):
        clear_form(actor, defaults, grouped_options)
        st.rerun()


def render_record_cards(df: pd.DataFrame, title: str, source: str, grouped_options: Dict[str, List[str]], defaults: Dict[str, Any]) -> None:
    st.subheader(title)
    if source == "backup":
        st.warning("目前為本機備份快照模式，資料可能不是最新。")
    elif source == "local":
        st.info("目前顯示本機草稿。")
    if df.empty:
        st.info("目前沒有資料。")
        return
    for _, row in df.iterrows():
        rec = row.to_dict()
        record_id = str(rec.get("record_id", "") or rec.get("id", ""))
        editable = can_edit_record(actor, rec)
        deletable = can_delete_record(actor, rec)
        hard_deletable = can_hard_delete(actor)
        with st.container(border=True):
            c1, c2, c3 = st.columns([2.2, 1.2, 1.5])
            c1.markdown(f"**{record_id}**")
            c2.write(f"狀態：{rec.get('status','draft')}")
            c3.write(f"金額：{rec.get('amount_total','')}")
            st.write(f"計畫代號：{rec.get('plan_code','')}")
            st.write(f"用途說明：{rec.get('purpose_desc','')}")
            b1, b2, b3, b4 = st.columns(4)
            if b1.button("檢視", key=f"expense_view_{record_id}", use_container_width=True):
                load_record_into_form(rec, actor, grouped_options)
                st.rerun()
            if b2.button("編輯", key=f"expense_edit_{record_id}", disabled=not editable, use_container_width=True):
                load_record_into_form(rec, actor, grouped_options)
                st.rerun()
            if b3.button("刪除", key=f"expense_delete_{record_id}", disabled=not deletable, use_container_width=True):
                rec['status'] = 'void' if str(rec.get('status','')).strip().lower() in {'submitted','void'} else 'deleted'
                upsert_local_expense_draft(owner_email := str(rec.get('user_email') or actor.email or '').strip().lower(), rec)
                ok, msg = _queue_and_try_sync_expense(actor, 'expense_soft_delete', rec)
                refresh_runtime_cache(actor)
                st.success(f"{record_id} 已刪除。" if ok else f"{record_id} 已標記待同步刪除：{msg}")
                st.rerun()
            confirm_key = f"expense_hard_delete_confirm::{record_id}"
            if st.session_state.get(confirm_key):
                cfm1, cfm2 = st.columns(2)
                if cfm1.button("確認永久移除", key=f"expense_hard_delete_yes_{record_id}", type="primary", use_container_width=True):
                    try:
                        archive_deleted_record(rec, system_type="expense", actor_email=actor.email)
                        owner_email = str(rec.get("user_email") or actor.email or "").strip().lower()
                        remove_local_expense_draft(owner_email, record_id, mark_deleted=False)
                        _queue_and_try_sync_expense(actor, 'expense_hard_delete', {'record_id': record_id, 'user_email': owner_email, 'system_type': 'expense'})
                        st.session_state.pop(confirm_key, None)
                        refresh_runtime_cache(actor)
                        st.success(f"{record_id} 已永久移除。")
                        st.rerun()
                    except Exception as e:
                        st.error(f"永久移除失敗：{e}")
                if cfm2.button("取消", key=f"expense_hard_delete_no_{record_id}", use_container_width=True):
                    st.session_state.pop(confirm_key, None)
                    st.rerun()
            elif b4.button("移除", key=f"expense_hard_delete_{record_id}", disabled=not hard_deletable, use_container_width=True):
                st.session_state[confirm_key] = True
                st.warning("此操作會永久移除資料，且已先備份到 deleted archive。")
                st.rerun()


def _month_text(v: Any) -> str:
    d = normalize_date_value(v)
    return d.strftime("%Y-%m")


def _normalize_payment_target_label(v: Any) -> str:
    s = str(v or "").strip().lower()
    mapping = {
        "employee": "employee", "員工姓名": "employee",
        "advance": "advance", "借支沖抵": "advance",
        "vendor": "vendor", "逕付廠商": "vendor",
    }
    return mapping.get(s, s or "employee")


def _payment_target_text(rec: Dict[str, Any]) -> str:
    if to_bool(rec.get("advance_offset_enabled"), False):
        return "借支沖抵"
    if to_bool(rec.get("vendor_enabled"), False):
        return "逕付廠商"
    s = _normalize_payment_target_label(rec.get("payment_target", "employee"))
    return {"employee": "員工姓名", "advance": "借支沖抵", "vendor": "逕付廠商"}.get(s, "員工姓名")


def _owner_text(rec: Dict[str, Any]) -> str:
    return str(rec.get("owner_name") or rec.get("employee_name") or rec.get("created_by_name") or "").strip()


def _record_to_pdf_payload(rec: Dict[str, Any], actor: Actor) -> Dict[str, Any]:
    payload = dict(rec)
    if to_bool(rec.get("advance_offset_enabled"), False):
        payload["payment_target"] = "advance"
    elif to_bool(rec.get("vendor_enabled"), False):
        payload["payment_target"] = "vendor"
    else:
        payload["payment_target"] = str(rec.get("payment_target", "employee") or "employee")
    att = rec.get("attachment_files", [])
    if isinstance(att, str):
        try:
            att = json.loads(att)
        except Exception:
            att = []
    payload["attachment_files"] = att if isinstance(att, list) else []
    sig = rec.get("signature_file", {})
    if isinstance(sig, str):
        try:
            sig = json.loads(sig)
        except Exception:
            sig = {}
    payload["signature_file"] = sig if isinstance(sig, dict) else {}
    payload["tax_amount"] = safe_int(payload.get("tax_amount"))
    payload["amount_total"] = safe_int(payload.get("amount_total"))
    payload["amount_untaxed"] = safe_int(payload.get("amount_untaxed"))
    payload["owner_name"] = payload.get("owner_name") or actor.name
    payload["user_email"] = payload.get("user_email") or actor.email
    return payload


def _render_filters_and_metrics(df: pd.DataFrame, status_default: str, key_prefix: str) -> pd.DataFrame:
    st.markdown(
        """
        <style>
        .exp-list-title{font-size:42px;font-weight:800;line-height:1.1;margin:0 0 14px 0;color:#1f2937;}
        .exp-section-note{font-size:14px;color:#6b7280;margin-bottom:14px;}
        .exp-metric{border:1px solid rgba(15,23,42,.10);border-radius:14px;padding:12px 16px;background:#fff;min-height:78px;}
        .exp-metric-label{font-size:13px;color:#64748b;margin-bottom:8px;}
        .exp-metric-value{font-size:24px;font-weight:800;color:#0f172a;}
        .exp-total{border:1px solid #e7d5c6;background:#faf1e8;border-radius:16px;padding:16px 18px;margin-top:8px;display:flex;justify-content:space-between;align-items:center;}
        .exp-total-label{font-size:18px;font-weight:700;color:#111827;}
        .exp-total-value{font-size:24px;font-weight:900;color:#f97316;}
        .exp-table-head{font-weight:700;color:#111827;padding:10px 6px;border-bottom:1px solid rgba(15,23,42,.10);}
        .exp-table-cell{padding:12px 6px;border-bottom:1px solid rgba(15,23,42,.07);}
        .exp-status-dot{display:inline-block;width:10px;height:10px;border-radius:999px;background:#22c55e;margin-right:6px;vertical-align:middle;}
        </style>
        """,
        unsafe_allow_html=True,
    )
    reset_col, _ = st.columns([1, 5])
    if reset_col.button("重設篩選", key=f"{key_prefix}_reset_filters"):
        for suffix in ["status", "owner", "plan", "record_id", "start", "end", "page_size", "page_no"]:
            st.session_state.pop(f"{key_prefix}_{suffix}", None)
        st.rerun()

    r1 = st.columns(4)
    status_options = ["all", "draft", "deleted", "submitted", "void"]
    default_status = status_default if status_default in status_options else "all"
    current_status = st.session_state.get(f"{key_prefix}_status", default_status)
    if current_status not in status_options:
        current_status = default_status
    labels = {"all": "全部", "draft": "draft", "deleted": "deleted", "submitted": "submitted", "void": "void"}
    status_value = r1[0].selectbox("狀態", status_options, index=status_options.index(current_status), key=f"{key_prefix}_status", format_func=lambda x: labels[x])
    owner_default = st.session_state.get(f"{key_prefix}_owner", actor.name if status_default == "submitted" else "")
    owner_value = r1[1].text_input("填表人包含", value=owner_default, key=f"{key_prefix}_owner")
    plan_value = r1[2].text_input("計畫編號包含", value=st.session_state.get(f"{key_prefix}_plan", ""), key=f"{key_prefix}_plan")
    record_value = r1[3].text_input("表單ID", value=st.session_state.get(f"{key_prefix}_record_id", ""), key=f"{key_prefix}_record_id")
    r2 = st.columns(2)
    start_value = r2[0].text_input("起始年月(YYYY-MM)", value=st.session_state.get(f"{key_prefix}_start", ""), key=f"{key_prefix}_start")
    end_value = r2[1].text_input("結束年月(YYYY-MM)", value=st.session_state.get(f"{key_prefix}_end", ""), key=f"{key_prefix}_end")
    r3 = st.columns(2)
    size_options = [10, 20, 50, 100]
    current_size = int(st.session_state.get(f"{key_prefix}_page_size", 20) or 20)
    if current_size not in size_options:
        current_size = 20
    page_size = r3[0].selectbox("每頁筆數", size_options, index=size_options.index(current_size), key=f"{key_prefix}_page_size")

    if df.empty:
        r3[1].number_input("頁碼", min_value=1, value=1, disabled=True, key=f"{key_prefix}_page_no")
        return df

    filtered = df.copy().fillna("")
    if "status" not in filtered.columns:
        filtered["status"] = status_default if status_default in {"draft", "submitted", "deleted", "void"} else "draft"
    filtered["owner_name"] = filtered.apply(lambda r: _owner_text(r.to_dict()), axis=1)
    filtered["record_id_text"] = filtered.apply(lambda r: str(r.get("record_id") or r.get("id") or ""), axis=1)
    filtered["month_text"] = filtered.apply(lambda r: _month_text(r.get("form_date", "")), axis=1)
    filtered["plan_text"] = filtered.get("plan_code", "").astype(str)
    filtered["status"] = filtered["status"].astype(str).str.lower()

    if status_value != "all":
        filtered = filtered[filtered["status"] == status_value]
    if owner_value.strip():
        filtered = filtered[filtered["owner_name"].str.contains(owner_value.strip(), case=False, na=False)]
    if plan_value.strip():
        filtered = filtered[filtered["plan_text"].str.contains(plan_value.strip(), case=False, na=False)]
    if record_value.strip():
        filtered = filtered[filtered["record_id_text"].str.contains(record_value.strip(), case=False, na=False)]
    if start_value.strip():
        filtered = filtered[filtered["month_text"] >= start_value.strip()]
    if end_value.strip():
        filtered = filtered[filtered["month_text"] <= end_value.strip()]

    total_pages = max(1, (len(filtered) + page_size - 1) // page_size)
    current_page_no = int(st.session_state.get(f"{key_prefix}_page_no", 1) or 1)
    if current_page_no > total_pages:
        current_page_no = total_pages
    page_no = r3[1].number_input("頁碼", min_value=1, max_value=total_pages, value=current_page_no, step=1, key=f"{key_prefix}_page_no")
    page_df = filtered.iloc[(page_no - 1) * page_size : page_no * page_size].copy()

    untaxed_sum = int(page_df["amount_untaxed"].apply(safe_int).sum()) if "amount_untaxed" in page_df.columns else 0
    tax_sum = int(page_df["tax_amount"].apply(safe_int).sum()) if "tax_amount" in page_df.columns else 0
    total_sum = int(page_df["amount_total"].apply(safe_int).sum()) if "amount_total" in page_df.columns else 0
    count = len(page_df)
    avg = int(round(total_sum / count)) if count else 0

    m1, m2, m3, m4 = st.columns(4)
    metrics = [
        (m1, "未稅合計", f"NT$ {untaxed_sum:,}"),
        (m2, "稅金合計", f"NT$ {tax_sum:,}"),
        (m3, "筆數", str(count)),
        (m4, "平均每筆（含稅）", f"NT$ {avg:,}"),
    ]
    for col, label, value in metrics:
        with col:
            st.markdown(f'<div class="exp-metric"><div class="exp-metric-label">{label}</div><div class="exp-metric-value">{value}</div></div>', unsafe_allow_html=True)
    st.markdown(f'<div class="exp-total"><div class="exp-total-label">含稅總計：</div><div class="exp-total-value">NT$ {total_sum:,}</div></div>', unsafe_allow_html=True)
    return page_df


def render_record_list_page(df: pd.DataFrame, title: str, source: str, grouped_options: Dict[str, List[str]], defaults: Dict[str, Any], status_default: str, key_prefix: str) -> None:
    st.markdown(f'<div class="exp-list-title">{title}</div>', unsafe_allow_html=True)
    if source == "backup":
        st.warning("目前為本機備份快照模式，資料可能不是最新。")
    elif source == "local":
        st.info("目前顯示本機草稿。")
    filtered_df = _render_filters_and_metrics(df, status_default=status_default, key_prefix=key_prefix)
    if filtered_df.empty:
        st.info("目前沒有符合篩選條件的資料。")
        return

    headers = ["表單ID", "狀態", "同步狀態", "日期", "填表人", "計畫編號", "付款對象", "總金額", "事由", "更新時間", "操作"]
    widths = [1.2, 0.9, 0.95, 1.0, 1.0, 1.0, 0.95, 0.8, 1.2, 1.1, 3.0]
    cols = st.columns(widths)
    for col, text in zip(cols, headers):
        col.markdown(f'<div class="exp-table-head">{text}</div>', unsafe_allow_html=True)

    for _, row in filtered_df.iterrows():
        rec = row.to_dict()
        record_id = str(rec.get("record_id") or rec.get("id") or "")
        status_text = str(rec.get("status") or status_default or "draft")
        updated_text = str(rec.get("updated_at") or rec.get("modified_at") or rec.get("created_at") or "")
        form_date_text = str(rec.get("form_date") or "")[:10]
        row_cols = st.columns(widths)
        row_cols[0].markdown(f'<div class="exp-table-cell">{record_id}</div>', unsafe_allow_html=True)
        row_cols[1].markdown(f'<div class="exp-table-cell"><span class="exp-status-dot"></span>{status_text}</div>', unsafe_allow_html=True)
        row_cols[2].markdown(f'<div class="exp-table-cell">{get_sync_status_label(rec)}</div>', unsafe_allow_html=True)
        row_cols[3].markdown(f'<div class="exp-table-cell">{form_date_text}</div>', unsafe_allow_html=True)
        row_cols[4].markdown(f'<div class="exp-table-cell">{_owner_text(rec)}</div>', unsafe_allow_html=True)
        row_cols[5].markdown(f'<div class="exp-table-cell">{str(rec.get("plan_code", ""))}</div>', unsafe_allow_html=True)
        row_cols[6].markdown(f'<div class="exp-table-cell">{_payment_target_text(rec)}</div>', unsafe_allow_html=True)
        row_cols[7].markdown(f'<div class="exp-table-cell">{safe_int(rec.get("amount_total")):,}</div>', unsafe_allow_html=True)
        row_cols[8].markdown(f'<div class="exp-table-cell">{str(rec.get("purpose_desc", ""))}</div>', unsafe_allow_html=True)
        row_cols[9].markdown(f'<div class="exp-table-cell">{updated_text}</div>', unsafe_allow_html=True)
        action_cols = row_cols[10].columns(6)
        pdf_payload = _record_to_pdf_payload(rec, actor)
        pdf_bytes = _prepare_pdf_bytes(pdf_payload)
        owner_email = str(rec.get("user_email") or actor.email or "").strip().lower()
        if action_cols[0].button("編輯", key=f"{key_prefix}_edit_{record_id}", use_container_width=True):
            load_record_into_form(rec, actor, grouped_options)
            st.session_state["expense_page"] = "new"
            st.rerun()
        if action_cols[1].button("複製", key=f"{key_prefix}_copy_{record_id}", use_container_width=True):
            copy_record_into_form(rec, actor, grouped_options)
            st.success("已複製為新表單，可直接修改部分欄位後送出。")
            st.rerun()
        action_cols[2].download_button("下載", data=pdf_bytes, file_name=f"支出報帳_{record_id or 'preview'}.pdf", mime="application/pdf", key=f"{key_prefix}_download_{record_id}", use_container_width=True)
        submit_disabled = status_text in {"submitted", "void"}
        if action_cols[3].button("送出", key=f"{key_prefix}_submit_{record_id}", disabled=submit_disabled, use_container_width=True):
            rec["status"] = "submitted"
            upsert_local_expense_draft(owner_email, rec)
            ok, msg = _queue_and_try_sync_expense(actor, 'expense_submit', rec)
            refresh_runtime_cache(actor)
            if ok:
                st.success(f"{record_id} 已送出。")
            else:
                st.warning(f"{record_id} 已加入待同步送出：{msg or '請稍後重新同步'}")
            st.rerun()
        action_label = "作廢" if status_text in {"submitted", "void"} else "刪除"
        if action_cols[4].button(action_label, key=f"{key_prefix}_void_{record_id}", disabled=not can_delete_record(actor, rec), use_container_width=True):
            rec["status"] = "void" if action_label == "作廢" else "deleted"
            upsert_local_expense_draft(owner_email, rec)
            ok, msg = _queue_and_try_sync_expense(actor, 'expense_soft_delete', rec)
            refresh_runtime_cache(actor)
            if ok:
                st.success(f"{record_id} 已{action_label}。")
            else:
                st.warning(f"{record_id} 已加入待同步{action_label}：{msg or '請稍後重新同步'}")
            st.rerun()
        confirm_key = f"expense_hard_delete_confirm::{key_prefix}::{record_id}"
        if st.session_state.get(confirm_key):
            st.warning("此操作會永久移除資料，且已先備份到 deleted archive。")
            cfm1, cfm2 = st.columns(2)
            if cfm1.button("確認移除", key=f"{key_prefix}_hard_delete_yes_{record_id}", type="primary", disabled=not can_hard_delete(actor), use_container_width=True):
                try:
                    archive_deleted_record(rec, system_type="expense", actor_email=actor.email)
                    remove_local_expense_draft(owner_email, record_id, mark_deleted=False)
                    ok, msg = _queue_and_try_sync_expense(actor, 'expense_hard_delete', {'record_id': record_id, 'user_email': owner_email, 'system_type': 'expense'})
                    st.session_state.pop(confirm_key, None)
                    refresh_runtime_cache(actor)
                    if ok:
                        st.success(f"{record_id} 已永久移除。")
                    else:
                        st.warning(f"{record_id} 已加入待同步永久移除：{msg or '請稍後重新同步'}")
                    st.rerun()
                except Exception as e:
                    st.error(f"永久移除失敗：{e}")
            if cfm2.button("取消移除", key=f"{key_prefix}_hard_delete_no_{record_id}", use_container_width=True):
                st.session_state.pop(confirm_key, None)
                st.info("已取消移除。")
                st.rerun()
        elif action_cols[5].button("移除單筆資料", key=f"{key_prefix}_hard_delete_{record_id}", disabled=not can_hard_delete(actor), use_container_width=True):
            st.session_state[confirm_key] = True
            st.rerun()



def render_drafts_page(grouped_options: Dict[str, List[str]], defaults: Dict[str, Any]) -> None:
    df, source = load_records_cloud_or_backup(actor, status="draft")
    if not df.empty:
        df = df[df.get("status", "").astype(str).str.lower().isin(["draft", "deleted"])]
    st.session_state["expense_sidebar_export_df"] = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    render_record_list_page(df, "草稿列表", source, grouped_options, defaults, status_default="draft", key_prefix="drafts")


def render_submitted_page(grouped_options: Dict[str, List[str]], defaults: Dict[str, Any]) -> None:
    df, source = load_records_cloud_or_backup(actor, status="submitted")
    if not df.empty:
        df = df[df.get("status", "").astype(str).str.lower().isin(["submitted", "void"])]
    st.session_state["expense_sidebar_export_df"] = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    render_record_list_page(df, "已送出表單列表", source, grouped_options, defaults, status_default="submitted", key_prefix="submitted")


def render_all_page(grouped_options: Dict[str, List[str]], defaults: Dict[str, Any]) -> None:
    df, source = load_records_cloud_or_backup(actor, status=None)
    if not df.empty:
        df = df.copy().fillna("")
        if "status" not in df.columns:
            df["status"] = "draft"
        if "record_id" in df.columns:
            df = df.drop_duplicates(subset=["record_id"], keep="last")
    st.session_state["expense_sidebar_export_df"] = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    render_record_list_page(df, "全部表單列表", source, grouped_options, defaults, status_default="all", key_prefix="all")


st.set_page_config(page_title="支出報帳", page_icon="💰", layout="wide")
st.session_state.setdefault("expense_sidebar_export_df", pd.DataFrame())
render_header()
actor = require_actor()
api = get_api()
grouped_options, options_source = load_options_with_fallback()
defaults, defaults_source = load_defaults_with_fallback(actor.email)
if options_source == "cache":
    st.warning("目前雲端 Options 無法讀取，已改用本機快取。")
elif options_source == "empty":
    st.info("目前無雲端 Options，已以預設欄位渲染表單。")

with st.sidebar:
    st.markdown("### 目前身份")
    st.write(f"姓名：{actor.name}")
    st.write(f"Email：{actor.email}")
    st.write(f"角色：{actor.role}")
    st.divider()
    page_options = ["new", "drafts", "submitted", "all"]
    current_page = st.session_state.get("expense_page", "new")
    current_index = page_options.index(current_page) if current_page in page_options else 0
    page_choice = st.radio(
        "功能選單",
        options=page_options,
        index=current_index,
        format_func=lambda x: {"new": "📝 新增 / 編輯", "drafts": "📄 草稿列表", "submitted": "📤 已送出列表", "all": "📚 全部資料"}[x],
    )
    if page_choice != current_page:
        st.session_state["expense_page"] = page_choice
        if page_choice == "new":
            clear_form(actor, defaults, grouped_options)
        st.rerun()
    if st.button("➕ 清空並新增一筆", use_container_width=True):
        clear_form(actor, defaults, grouped_options)
        st.session_state["expense_page"] = "new"
        st.rerun()
    render_sync_status_sidebar_expense(actor.email)

render_top_sync_notice_expense(actor.email)
page = st.session_state.get("expense_page", "new")
if page == "drafts":
    render_drafts_page(grouped_options, defaults)
elif page == "submitted":
    render_submitted_page(grouped_options, defaults)
elif page == "all":
    render_all_page(grouped_options, defaults)
else:
    render_form_page(grouped_options, defaults)
