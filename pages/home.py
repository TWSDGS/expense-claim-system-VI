from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, List

import streamlit as st

from storage_apps_script import AppsScriptStorage, Actor
from cache_utils import load_users_cache, save_users_cache
from cache_utils import clear_global_cache_files


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
EXPENSE_CONFIG_PATH = DATA_DIR / "config.json"


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _get_web_app_url() -> str:
    cfg = _read_json(EXPENSE_CONFIG_PATH)
    return (
        cfg.get("google", {}).get("apps_script_url")
        or st.secrets.get("APPS_SCRIPT_WEB_APP_URL", "")
    ).strip()


@st.cache_resource(show_spinner=False)
def get_api() -> AppsScriptStorage:
    return AppsScriptStorage(
        web_app_url=_get_web_app_url(),
        system="expense",
        timeout=20,
    )


def build_actor_from_user(user: Dict[str, Any]) -> Actor:
    return Actor(
        name=user.get("name", "") or user.get("user_name", ""),
        email=user.get("email", ""),
        role=user.get("role", "user"),
        employee_no=user.get("employee_no", ""),
        department=user.get("department", ""),
    )


def clear_user_runtime_state() -> None:
    keys_to_clear = [
        "expense_form_data",
        "travel_form_data",
        "expense_editing_record_id",
        "travel_editing_record_id",
        "expense_page",
        "travel_page",
        "expense_options_grouped",
        "expense_options_source",
        "travel_options_df",
        "travel_options_source",
    ]

    dynamic_prefixes = [
        "expense_defaults_",
        "travel_defaults_",
        "expense_form_data::",  # Per-user form data key
        "travel_form_data::",   # Per-user travel form data key
        "expense_editing_record_id::",  # Per-user editing record ID
        "travel_editing_record_id::",   # Per-user travel editing record ID
    ]

    for k in list(st.session_state.keys()):
        if k in keys_to_clear:
            st.session_state.pop(k, None)
            continue
        for prefix in dynamic_prefixes:
            if str(k).startswith(prefix):
                st.session_state.pop(k, None)
                break


def store_actor(actor: Actor) -> None:
    prev_email = str(st.session_state.get("actor_email", "")).strip().lower()
    new_email = str(actor.email).strip().lower()

    if prev_email and prev_email != new_email:
        # Clear all user-specific runtime state when switching users
        clear_user_runtime_state()
        # Also clear per-user keys from previous email
        for k in list(st.session_state.keys()):
            if prev_email in k:
                st.session_state.pop(k, None)

    st.session_state["actor_name"] = actor.name
    st.session_state["actor_email"] = actor.email
    st.session_state["actor_role"] = actor.role
    st.session_state["actor_employee_no"] = actor.employee_no
    st.session_state["actor_department"] = actor.department

def clear_actor_session_state():
    keys_to_clear = [
        "actor_name",
        "actor_email",
        "actor_role",
        "actor_employee_no",
        "actor_department",
        "selected_actor_email",
        "selected_actor_name",
        "selected_actor_role",
        "selected_actor_employee_no",
        "selected_actor_department",
        "users_rows",
        "user_defaults_rows",
        "options_rows",
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]


def render_refresh_cloud_settings_button():
    st.markdown("### 雲端設定同步")
    st.caption("重新抓取雲端的使用者、預設值與選項設定，並更新本機快取。")

    if st.button("重新同步雲端設定", key="refresh_cloud_settings_btn", use_container_width=True):
        clear_global_cache_files()
        clear_actor_session_state()
        st.success("已清除本機快取，請重新選擇身份。")
        st.rerun()

def render_actor_card(actor: Actor) -> None:
    st.markdown(
        f"""
        <div style="border:1px solid #dbe2ea;border-radius:12px;padding:12px 14px;background:#f8fafc;">
          <div style="font-size:20px;font-weight:700;">目前身份：{actor.name}</div>
          <div>Email：{actor.email}</div>
          <div>角色：{actor.role}</div>
          <div>員工編號：{actor.employee_no}</div>
          <div>部門：{actor.department}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def load_users_with_fallback() -> tuple[List[Dict[str, Any]], str]:
    try:
        rows = get_api().users_list()
        if rows:
            save_users_cache(rows)
            return rows, "cloud"
    except Exception:
        pass

    cached = load_users_cache()
    if cached:
        return cached, "cache"

    return [], "manual"


st.title("💼 報帳管理入口")

users, source = load_users_with_fallback()

if source == "cloud":
    st.success("已使用雲端 Users 身份清單。")
elif source == "cache":
    st.warning("目前雲端 Users 無法讀取，已改用本機快取身份清單。")
else:
    st.error("目前雲端與本機快取都無法讀取 Users，請改用手動輸入身份。")

if users:
    users = sorted(users, key=lambda x: str(x.get("sort_order", "9999")))
    labels = [f"{u.get('name', u.get('user_name',''))}｜{u.get('email','')}｜{u.get('role','user')}" for u in users]
    label_to_user = {label: user for label, user in zip(labels, users)}

    selected_label = st.selectbox("選擇身份", labels)
    selected_user = label_to_user[selected_label]
    actor = build_actor_from_user(selected_user)
else:
    name = st.text_input("姓名")
    email = st.text_input("Email")
    role = st.selectbox("角色", ["user", "admin"], index=0)
    employee_no = st.text_input("員工編號")
    department = st.text_input("部門")
    actor = Actor(
        name=name.strip(),
        email=email.strip().lower(),
        role=role,
        employee_no=employee_no.strip(),
        department=department.strip(),
    )

if actor.name and actor.email:
    store_actor(actor)
    render_actor_card(actor)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("進入 支出報帳", use_container_width=True, type="primary"):
            st.switch_page("expense.py")
    with c2:
        if st.button("進入 出差報帳", use_container_width=True):
            st.switch_page("apps/travel_old.py")
else:
    st.info("請先完成身份選擇或手動輸入。")
