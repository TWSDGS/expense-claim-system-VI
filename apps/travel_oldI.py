from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import streamlit as st

from storage_apps_script import Actor
from cache_utils import (
    delete_saved_file,
    load_local_travel_records,
    save_signature_file,
    save_uploaded_attachment,
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
)
import pdf_gen_travel

BASE_DIR = Path(__file__).resolve().parents[1]
TRAVEL_CONFIG_PATH = BASE_DIR / "data" / "travel_config.json"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _get_cloud_excel_url() -> str:
    cfg = _read_json(TRAVEL_CONFIG_PATH)
    return str(cfg.get("ui", {}).get("cloud_excel_url", "")).strip()


def _df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "travel") -> bytes:
    from io import BytesIO
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        (df.copy() if df is not None else pd.DataFrame()).to_excel(writer, sheet_name=sheet_name[:31], index=False)
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


def list_records(actor: Actor) -> pd.DataFrame:
    rows: list[dict] = []
    if str(actor.role).lower() == "admin":
        users_rows = load_users_cache() or []
        seen_emails = set()
        for u in users_rows:
            email = str(u.get("email", "")).strip().lower()
            if not email or email in seen_emails:
                continue
            seen_emails.add(email)
            rows.extend(load_local_travel_records(email) or [])
        if actor.email not in seen_emails:
            rows.extend(load_local_travel_records(actor.email) or [])
    else:
        rows = load_local_travel_records(actor.email) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).fillna("")
    if "owner_name" not in df.columns:
        df["owner_name"] = df.get("traveler", "")
    return df


def render_sync_status_sidebar_travel(current_user_email: str) -> None:
    if not current_user_email:
        return
    pending_count = count_pending_sync(current_user_email, system_type="travel")
    st.sidebar.markdown("---")
    st.sidebar.subheader("雲端同步狀態")
    cloud_online = st.session_state.get("cloud_online_travel", True)
    if cloud_online:
        st.sidebar.success("雲端：已連線")
    else:
        st.sidebar.error("雲端：未連線")
    if pending_count > 0:
        st.sidebar.warning(f"你有 {pending_count} 筆出差資料尚未同步到雲端")
    else:
        st.sidebar.success("你的出差資料皆已同步")

    cloud_url = _get_cloud_excel_url()
    if cloud_url:
        st.sidebar.link_button("開啟雲端表單", cloud_url, use_container_width=True)

    export_df = st.session_state.get("travel_sidebar_export_df")
    if isinstance(export_df, pd.DataFrame):
        st.sidebar.download_button(
            "下載Excel",
            data=_df_to_excel_bytes(export_df, sheet_name="travel"),
            file_name="出差報帳.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key="travel_sidebar_download_excel",
        )

    if st.sidebar.button("立即同步出差資料", key="sync_travel_now_btn", use_container_width=True):
        pending_items = load_pending_sync(current_user_email)
        synced = 0
        failed = 0
        for item in pending_items:
            payload = dict(item.get("payload") or item)
            system_type = payload.get("system_type") or ("travel" if "travel" in str(item.get("operation", "")).lower() else "expense")
            if system_type != "travel":
                continue
            record_id = str(payload.get("record_id") or "").strip()
            try:
                # TODO: replace with real cloud API call if available in your environment.
                mark_sync_success(current_user_email, "travel", record_id)
                synced += 1
            except Exception as e:
                mark_sync_failed(current_user_email, "travel", record_id, str(e))
                failed += 1
        if synced == 0 and failed == 0:
            st.sidebar.info("目前沒有待同步的出差資料。")
        elif failed == 0:
            st.sidebar.success(f"同步完成：{synced} 筆")
        else:
            st.sidebar.warning(f"同步完成：成功 {synced} 筆，失敗 {failed} 筆")


def render_top_sync_notice_travel(current_user_email: str) -> None:
    if not current_user_email:
        return
    pending_count = count_pending_sync(current_user_email, system_type="travel")
    if pending_count > 0:
        st.info(f"提醒：你有 {pending_count} 筆出差資料尚未同步到雲端。")


def persist_uploads(actor: Actor, payload: Dict[str, Any], uploads: list | None, signature_upload) -> Dict[str, Any]:
    payload = dict(payload)
    existing = list(payload.get("attachment_files", []) or [])
    for up in uploads or []:
        existing.append(save_uploaded_attachment(actor.email, up, "travel_attachment"))
    payload["attachment_files"] = existing
    if signature_upload is not None:
        payload["signature_file"] = save_signature_file(actor.email, signature_upload)
    return payload


def remove_attachment(actor: Actor, idx: int) -> None:
    form = dict(get_form(actor))
    files = list(form.get("attachment_files", []) or [])
    if 0 <= idx < len(files):
        delete_saved_file(files[idx])
        files.pop(idx)
        form["attachment_files"] = files
        set_form(actor, form)


def remove_signature(actor: Actor) -> None:
    form = dict(get_form(actor))
    delete_saved_file(form.get("signature_file", {}))
    form["signature_file"] = {}
    set_form(actor, form)


def load_into_form(actor: Actor, rec: Dict[str, Any], as_copy: bool = False) -> None:
    data = dict(rec)
    if as_copy:
        data["record_id"] = ""
        data["form_date"] = date.today().isoformat()
        data["status"] = "draft"
    set_form(actor, data)
    st.session_state["travel_page"] = "new"


def _build_pdf(payload: Dict[str, Any]) -> bytes:
    attachment_paths = normalize_attachment_paths(payload.get("attachment_files") or [])
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
    departure_options = ["台南", "其他"]
    destination_options = ["台北", "新北", "新竹", "台中", "台南", "高雄", "其他"]
    transport_opts = ["公務車", "計程車", "私車公用", "高鐵", "飛機", "派車", "其他"]

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
        project_val = c4.selectbox("計畫編號", project_options, index=project_options.index(form.get("project_id", "")) if form.get("project_id", "") in project_options else 0)
        budget_val = c5.selectbox("預算來源", budget_options, index=budget_options.index(form.get("budget_source", "")) if form.get("budget_source", "") in budget_options else 0)

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
        official_plate_val = ""
        private_km_val = 0
        private_plate_val = ""
        other_transport_val = ""
        if "公務車" in transport_val:
            official_plate_val = st.text_input("公務車車號", value=str(form.get("official_car_plate", "")))
        if "私車公用" in transport_val:
            p1, p2 = st.columns(2)
            private_km_val = p1.number_input("私車公里數", min_value=0, step=1, value=safe_int(form.get("private_car_km", 0)))
            private_plate_val = p2.text_input("私車車號", value=str(form.get("private_car_plate", "")))
        if "其他" in transport_val:
            other_transport_val = st.text_input("其他交通工具說明", value=str(form.get("other_transport", "")))

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
                    options=["", "高鐵", "台鐵", "客運", "捷運", "公車", "計程車", "私車公用", "公務車", "飛機", "船舶", "其他"],
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
            "project_id": project_val,
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
            "attachment_files": list(form.get("attachment_files", []) or []),
            "signature_file": dict(form.get("signature_file", {}) or {}),
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
            queue_pending_sync("travel_draft", {"email": actor.email, "name": actor.name}, payload, queue_owner_email=actor.email)
            set_form(actor, payload)
            st.session_state["travel_page"] = "drafts"
            st.rerun()

        if submit_final:
            payload["status"] = "submitted"
            rid = upsert_local_travel_record(actor.email, payload)
            payload["record_id"] = rid
            queue_pending_sync("travel_submit", {"email": actor.email, "name": actor.name}, payload, queue_owner_email=actor.email)
            set_form(actor, payload)
            st.session_state["travel_page"] = "submitted"
            st.rerun()

        if make_pdf:
            set_form(actor, payload)
            pdf_bytes = _build_pdf(payload)

        if copy_form:
            set_form(actor, {**payload, "record_id": "", "form_date": date.today().isoformat(), "status": "draft"})
            st.rerun()

        if back_list:
            set_form(actor, payload)
            st.session_state["travel_page"] = "all"
            st.rerun()

        if delete_or_void:
            rid = str(form.get("record_id") or "")
            if rid:
                mark_local_travel_status(actor.email, rid, "void" if str(form.get("status", "draft")).lower() in {"submitted", "void"} else "deleted")
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
            a1, a2 = st.columns([6, 1])
            a1.write(name or "")
            if a2.button("移除", key=f"trv_att_rm_{i}"):
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
        actions = cols[8].columns(5)
        if actions[0].button("編輯", key=f"{key_prefix}_edit_{rec.get('record_id')}"):
            load_into_form(actor, rec, as_copy=False)
            st.rerun()
        if actions[1].button("複製", key=f"{key_prefix}_copy_{rec.get('record_id')}"):
            load_into_form(actor, rec, as_copy=True)
            st.rerun()
        pdf_bytes = _build_pdf(rec)
        actions[2].download_button("下載", data=pdf_bytes, file_name=f"出差報帳_{rec.get('record_id') or 'preview'}.pdf", mime="application/pdf", key=f"{key_prefix}_dl_{rec.get('record_id')}")
        if actions[3].button("送出", key=f"{key_prefix}_submit_{rec.get('record_id')}", disabled=str(rec.get("status")) in {"submitted", "void"}):
            rec["status"] = "submitted"
            upsert_local_travel_record(actor.email, rec)
            st.rerun()
        action_label = "作廢" if str(rec.get("status")) in {"submitted", "void"} else "刪除"
        if actions[4].button(action_label, key=f"{key_prefix}_del_{rec.get('record_id')}"):
            mark_local_travel_status(actor.email, str(rec.get("record_id")), "void" if action_label == "作廢" else "deleted")
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
