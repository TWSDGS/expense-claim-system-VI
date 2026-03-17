from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import pandas as pd


def _safe_import_cache_utils():
    import cache_utils as cu  # type: ignore
    return cu


def _normalize_df(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, tuple) and len(data) == 2 and isinstance(data[0], list):
        df = pd.DataFrame(data[0])
    else:
        df = pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.fillna("")
    if "record_id" not in df.columns:
        df["record_id"] = ""
    if "status" not in df.columns:
        df["status"] = "draft"
    if "version" not in df.columns:
        df["version"] = 1
    if "updated_at" not in df.columns:
        df["updated_at"] = ""
    return df


def _record_id_of(row: Dict[str, Any]) -> str:
    return str((row or {}).get("record_id") or (row or {}).get("id") or "").strip()


def _pending_matches_entity(item: Dict[str, Any], entity_type: str) -> bool:
    payload = dict((item or {}).get("payload") or {})
    op = str((item or {}).get("operation") or "").lower()
    system_type = str(payload.get("system_type") or "").lower()
    if system_type:
        return system_type == entity_type.lower()
    if entity_type.lower() == "travel":
        return op.startswith("travel") or "travel" in op
    return op.startswith("expense") or "expense" in op or not ("travel" in op)


def _status_after_soft_delete(existing_status: str) -> str:
    return "void" if str(existing_status).lower() == "submitted" else "deleted"


def _overlay_pending(base_df: pd.DataFrame, pending_items: List[Dict[str, Any]]) -> pd.DataFrame:
    df = _normalize_df(base_df)
    by_id: Dict[str, Dict[str, Any]] = {}
    if not df.empty:
        for row in df.to_dict(orient="records"):
            rid = _record_id_of(row)
            if rid:
                by_id[rid] = dict(row)

    for item in pending_items:
        payload = dict((item or {}).get("payload") or {})
        rid = _record_id_of(payload)
        if not rid:
            continue
        op = str((item or {}).get("operation") or "").lower()
        if op.endswith("hard_delete"):
            by_id.pop(rid, None)
            continue

        existing = dict(by_id.get(rid, {}))
        existing.update(payload)
        if op.endswith("soft_delete"):
            existing["status"] = _status_after_soft_delete(existing.get("status", payload.get("status", "draft")))
        existing["needs_sync"] = True
        existing["sync_status"] = item.get("sync_status") or "pending"
        by_id[rid] = existing

    rows = list(by_id.values())
    out = _normalize_df(rows)
    if not out.empty and "record_id" in out.columns:
        out = out.drop_duplicates(subset=["record_id"], keep="last")
    return out


def build_master_dataframe(
    entity_type: str,
    actor_or_owner: Any,
    api_or_fetcher: Any = None,
    *,
    fetch_cloud_rows: Optional[Callable[[], Iterable[Dict[str, Any]]]] = None,
    local_rows: Optional[Iterable[Dict[str, Any]]] = None,
    force_refresh: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Compatible with both calling styles used across patched files:

    1) build_master_dataframe("expense", actor, api, force_refresh=False)
    2) build_master_dataframe("expense", actor.email, fetch_cloud_rows=..., local_rows=...)
    3) build_master_dataframe("expense", actor.email, some_fetch_function, local_rows=...)
    """
    cu = _safe_import_cache_utils()

    if isinstance(actor_or_owner, str):
        owner_email = actor_or_owner.strip().lower()
        actor = None
        actor_role = "user"
    else:
        actor = actor_or_owner
        owner_email = str(getattr(actor_or_owner, "email", "") or "").strip().lower()
        actor_role = str(getattr(actor_or_owner, "role", "user") or "user")

    if fetch_cloud_rows is None and callable(api_or_fetcher) and not hasattr(api_or_fetcher, "records_df"):
        fetch_cloud_rows = api_or_fetcher
        api = None
    else:
        api = api_or_fetcher

    cache_key = owner_email or "global"

    snap_df = pd.DataFrame()
    if not force_refresh:
        try:
            snap_df = _normalize_df(cu.load_master_snapshot(entity_type, cache_key))
        except Exception:
            snap_df = pd.DataFrame()

    cloud_df = pd.DataFrame()
    source = "empty"
    cloud_online = False
    try:
        if callable(fetch_cloud_rows):
            cloud_df = _normalize_df(list(fetch_cloud_rows() or []))
        elif api is not None and hasattr(api, "records_df"):
            cloud_df = _normalize_df(api.records_df(actor=actor, status=None, owner_only=False))
        else:
            cloud_df = pd.DataFrame()
        source = "cloud"
        cloud_online = True
        try:
            cu.save_master_snapshot(entity_type, cache_key, cloud_df.to_dict(orient="records"))
        except Exception:
            pass
    except Exception:
        cloud_df = snap_df
        source = "snapshot" if not snap_df.empty else "empty"
        cloud_online = False

    local_df = _normalize_df(list(local_rows or [])) if local_rows is not None else pd.DataFrame()

    try:
        pending_all = list(cu.load_pending_sync(owner_email))
    except Exception:
        pending_all = []

    pending_items: List[Dict[str, Any]] = []
    for item in pending_all:
        if not _pending_matches_entity(item, entity_type):
            continue
        payload = dict((item or {}).get("payload") or {})
        payload_owner = str(payload.get("user_email") or owner_email).strip().lower()
        if actor_role != "admin" and owner_email and payload_owner and payload_owner != owner_email:
            continue
        pending_items.append(item)

    base_df = cloud_df if not cloud_df.empty else local_df
    if not local_df.empty:
        local_by_id = {}
        if not base_df.empty:
            for row in base_df.to_dict(orient="records"):
                rid = _record_id_of(row)
                if rid:
                    local_by_id[rid] = dict(row)
        for row in local_df.to_dict(orient="records"):
            rid = _record_id_of(row)
            if rid:
                local_by_id[rid] = dict(row)
        base_df = _normalize_df(list(local_by_id.values()))

    master_df = _overlay_pending(base_df, pending_items)
    report = {
        "entity_type": entity_type,
        "source": source,
        "master_count": int(len(master_df.index)) if isinstance(master_df, pd.DataFrame) else 0,
        "cloud_count": int(len(cloud_df.index)) if isinstance(cloud_df, pd.DataFrame) else 0,
        "local_count": int(len(local_df.index)) if isinstance(local_df, pd.DataFrame) else 0,
        "pending_count": len(pending_items),
        "cloud_online": cloud_online,
    }
    return master_df, report


def sync_pending_events(entity_type: str, actor: Any, api: Any) -> Dict[str, Any]:
    cu = _safe_import_cache_utils()
    owner_email = str(getattr(actor, "email", "") or "").strip().lower()
    role = str(getattr(actor, "role", "user") or "user")

    try:
        pending_all = list(cu.load_pending_sync(owner_email))
    except Exception:
        pending_all = []

    relevant: List[Dict[str, Any]] = []
    for item in pending_all:
        if not _pending_matches_entity(item, entity_type):
            continue
        payload = dict((item or {}).get("payload") or {})
        payload_owner = str(payload.get("user_email") or owner_email).strip().lower()
        if role != "admin" and owner_email and payload_owner and payload_owner != owner_email:
            continue
        relevant.append(item)

    synced = 0
    failed = 0
    conflicts = 0
    conflict_records: List[Dict[str, Any]] = []

    for item in relevant:
        op = str(item.get("operation") or "")
        payload = dict(item.get("payload") or {})
        event_id = item.get("event_id") or payload.get("event_id")
        lower_op = op.lower()
        try:
            if lower_op.endswith("hard_delete"):
                api.record_hard_delete(actor=actor, record_id=payload.get("record_id"))
            elif lower_op.endswith("soft_delete"):
                if hasattr(api, "record_soft_delete"):
                    api.record_soft_delete(actor=actor, record_id=payload.get("record_id"))
                else:
                    payload.setdefault("status", "deleted")
                    api.record_save_draft(actor=actor, payload=payload)
            elif lower_op.endswith("submit") or lower_op in {"expense_submit", "travel_submit"}:
                api.record_submit(actor=actor, payload=payload)
            elif lower_op.endswith("restore") and hasattr(api, "record_restore"):
                api.record_restore(actor=actor, payload=payload)
            else:
                api.record_save_draft(actor=actor, payload=payload)

            try:
                cu.mark_sync_success(owner_email, entity_type, str(payload.get('record_id') or ''))
                cu.remove_pending_sync_item(owner_email, event_id=str(event_id or ''), record_id=str(payload.get('record_id') or ''), system_type=entity_type)
            except Exception:
                pass
            synced += 1
        except Exception as e:
            msg = str(e)
            if "VERSION_CONFLICT" in msg:
                conflicts += 1
                conflict_records.append({
                    "event_id": event_id,
                    "record_id": payload.get("record_id"),
                    "message": msg,
                })
                try:
                    cu.mark_sync_failed(owner_email, entity_type, str(payload.get('record_id') or ''), msg)
                    current_item = dict(item)
                    current_payload = dict(current_item.get('payload') or {})
                    current_payload['sync_status'] = 'conflict'
                    current_payload['sync_message'] = msg
                    current_payload['needs_sync'] = True
                    current_item['payload'] = current_payload
                    current_item['last_error'] = msg
                    current_item['retry_count'] = int(current_item.get('retry_count') or 0) + 1
                    cu.update_pending_sync_item(owner_email, str(event_id or ''), current_item)
                except Exception:
                    pass
            else:
                failed += 1
                try:
                    cu.mark_sync_failed(owner_email, entity_type, str(payload.get('record_id') or ''), msg)
                except Exception:
                    pass

    return {
        "synced": synced,
        "failed": failed,
        "conflicts": conflicts,
        "conflict_records": conflict_records,
        "remaining": max(0, len(relevant) - synced),
    }
