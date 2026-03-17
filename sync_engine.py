from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import pandas as pd


def _cu():
    import cache_utils as cu  # type: ignore
    return cu


def _normalize_df(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        df = data.copy()
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.fillna("")
    if "record_id" not in df.columns:
        df["record_id"] = ""
    if "status" not in df.columns:
        df["status"] = "draft"
    return df


def _record_id(obj: Dict[str, Any]) -> str:
    return str((obj or {}).get("record_id") or "").strip()


def _match_entity(item: Dict[str, Any], entity_type: str) -> bool:
    payload = dict(item.get("payload") or {})
    system_type = str(payload.get("system_type") or "").strip().lower()
    op = str(item.get("operation") or "").strip().lower()
    if system_type:
        return system_type == entity_type.lower()
    if entity_type.lower() == "travel":
        return "travel" in op
    return "travel" not in op


def _load_pending_items(owner_email: str) -> List[Dict[str, Any]]:
    cu = _cu()
    try:
        return list(cu.load_pending_sync(owner_email) or [])
    except TypeError:
        try:
            return list(cu.load_pending_sync() or [])
        except Exception:
            return []
    except Exception:
        return []


def _save_snapshot(entity_type: str, owner_key: str, df: pd.DataFrame) -> None:
    cu = _cu()
    try:
        cu.save_master_snapshot(entity_type, owner_key, df.to_dict(orient="records"))
    except Exception:
        pass


def _load_snapshot(entity_type: str, owner_key: str) -> pd.DataFrame:
    cu = _cu()
    try:
        return _normalize_df(cu.load_master_snapshot(entity_type, owner_key))
    except Exception:
        return pd.DataFrame()


def build_master_dataframe(
    entity_type: str,
    actor_or_owner: Any,
    api_or_fetcher: Any = None,
    *,
    fetch_cloud_rows: Optional[Callable[[], Iterable[Dict[str, Any]]]] = None,
    local_rows: Optional[Iterable[Dict[str, Any]]] = None,
    force_refresh: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    if isinstance(actor_or_owner, str):
        owner_email = actor_or_owner.strip().lower()
        actor = None
        actor_role = "user"
    else:
        actor = actor_or_owner
        owner_email = str(getattr(actor, "email", "") or "").strip().lower()
        actor_role = str(getattr(actor, "role", "user") or "user").lower()

    if fetch_cloud_rows is None and callable(api_or_fetcher) and not hasattr(api_or_fetcher, "records_df"):
        fetch_cloud_rows = api_or_fetcher
        api = None
    else:
        api = api_or_fetcher

    owner_key = owner_email or "global"
    snapshot_df = pd.DataFrame() if force_refresh else _load_snapshot(entity_type, owner_key)

    cloud_df = pd.DataFrame()
    cloud_online = False
    source = "empty"
    try:
        if callable(fetch_cloud_rows):
            cloud_df = _normalize_df(list(fetch_cloud_rows() or []))
        elif api is not None and hasattr(api, "records_df"):
            cloud_df = _normalize_df(api.records_df(actor=actor, status=None, owner_only=(actor_role != "admin")))
        source = "cloud"
        cloud_online = True
        _save_snapshot(entity_type, owner_key, cloud_df)
    except Exception:
        cloud_df = snapshot_df
        source = "snapshot" if not snapshot_df.empty else "empty"

    local_df = _normalize_df(list(local_rows or [])) if local_rows is not None else pd.DataFrame()
    base_df = cloud_df if not cloud_df.empty else local_df
    if not local_df.empty:
        by_id: Dict[str, Dict[str, Any]] = {}
        for row in base_df.to_dict(orient="records") if not base_df.empty else []:
            rid = _record_id(row)
            if rid:
                by_id[rid] = dict(row)
        for row in local_df.to_dict(orient="records"):
            rid = _record_id(row)
            if rid:
                by_id[rid] = dict(row)
        base_df = _normalize_df(list(by_id.values()))

    pending_all = _load_pending_items(owner_email)
    pending_items: List[Dict[str, Any]] = []
    for item in pending_all:
        if not _match_entity(item, entity_type):
            continue
        payload = dict(item.get("payload") or {})
        payload_owner = str(payload.get("user_email") or owner_email).strip().lower()
        if actor_role != "admin" and owner_email and payload_owner and payload_owner != owner_email:
            continue
        pending_items.append(item)

    by_id: Dict[str, Dict[str, Any]] = {}
    for row in base_df.to_dict(orient="records") if not base_df.empty else []:
        rid = _record_id(row)
        if rid:
            by_id[rid] = dict(row)
    for item in pending_items:
        payload = dict(item.get("payload") or {})
        rid = _record_id(payload)
        if not rid:
            continue
        op = str(item.get("operation") or "").lower()
        if op.endswith("hard_delete"):
            by_id.pop(rid, None)
            continue
        row = dict(by_id.get(rid, {}))
        row.update(payload)
        if op.endswith("soft_delete"):
            row["status"] = "void" if str(row.get("status", "")).lower() == "submitted" else "deleted"
        row["needs_sync"] = True
        row["sync_status"] = item.get("sync_status") or payload.get("sync_status") or "pending"
        by_id[rid] = row

    master_df = _normalize_df(list(by_id.values()))
    report = {
        "entity_type": entity_type,
        "source": source,
        "master_count": len(master_df.index) if isinstance(master_df, pd.DataFrame) else 0,
        "cloud_count": len(cloud_df.index) if isinstance(cloud_df, pd.DataFrame) else 0,
        "local_count": len(local_df.index) if isinstance(local_df, pd.DataFrame) else 0,
        "pending_count": len(pending_items),
        "cloud_online": cloud_online,
    }
    return master_df, report


def _mark_success(owner_email: str, entity_type: str, record_id: str, event_id: str) -> None:
    cu = _cu()
    try:
        cu.mark_sync_success(owner_email, entity_type, record_id)
    except TypeError:
        try:
            cu.mark_sync_success(event_id)
        except Exception:
            pass
    except Exception:
        pass
    try:
        cu.remove_pending_sync_item(owner_email, event_id=event_id, record_id=record_id, system_type=entity_type)
    except TypeError:
        try:
            cu.remove_pending_sync_item(event_id)
        except Exception:
            pass
    except Exception:
        pass


def _mark_failed(owner_email: str, entity_type: str, record_id: str, event_id: str, msg: str) -> None:
    cu = _cu()
    try:
        cu.mark_sync_failed(owner_email, entity_type, record_id, msg)
    except TypeError:
        try:
            cu.mark_sync_failed(event_id, msg)
        except Exception:
            pass
    except Exception:
        pass


def _mark_conflict(owner_email: str, event_id: str, item: Dict[str, Any], msg: str) -> None:
    cu = _cu()
    new_item = dict(item)
    new_item["sync_status"] = "conflict"
    new_item["last_error"] = msg
    payload = dict(new_item.get("payload") or {})
    payload["sync_status"] = "conflict"
    payload["sync_message"] = msg
    new_item["payload"] = payload
    try:
        cu.update_pending_sync_item(owner_email, event_id, new_item)
    except TypeError:
        try:
            cu.update_pending_sync_item(event_id, new_item)
        except Exception:
            pass
    except Exception:
        pass


def sync_pending_events(entity_type: str, actor: Any, api: Any) -> Dict[str, Any]:
    owner_email = str(getattr(actor, "email", "") or "").strip().lower()
    role = str(getattr(actor, "role", "user") or "user").lower()
    pending_all = _load_pending_items(owner_email)
    relevant: List[Dict[str, Any]] = []
    for item in pending_all:
        if not _match_entity(item, entity_type):
            continue
        payload = dict(item.get("payload") or {})
        payload_owner = str(payload.get("user_email") or owner_email).strip().lower()
        if role != "admin" and owner_email and payload_owner and payload_owner != owner_email:
            continue
        relevant.append(item)

    synced = failed = conflicts = 0
    for item in relevant:
        payload = dict(item.get("payload") or {})
        record_id = _record_id(payload)
        event_id = str(item.get("event_id") or payload.get("event_id") or "").strip()
        op = str(item.get("operation") or "").lower()
        try:
            if op.endswith("hard_delete"):
                api.record_hard_delete(actor=actor, record_id=record_id)
            elif op.endswith("soft_delete"):
                if hasattr(api, "record_soft_delete"):
                    api.record_soft_delete(actor=actor, record_id=record_id)
                else:
                    payload.setdefault("status", "deleted")
                    api.record_save_draft(actor=actor, payload=payload)
            elif op.endswith("restore") and hasattr(api, "record_restore"):
                api.record_restore(actor=actor, payload=payload)
            elif op.endswith("submit") or op in {"expense_submit", "travel_submit"}:
                api.record_submit(actor=actor, payload=payload)
            else:
                api.record_save_draft(actor=actor, payload=payload)
            _mark_success(owner_email, entity_type, record_id, event_id)
            synced += 1
        except Exception as e:
            msg = str(e)
            if "VERSION_CONFLICT" in msg:
                _mark_conflict(owner_email, event_id, item, msg)
                conflicts += 1
            else:
                _mark_failed(owner_email, entity_type, record_id, event_id, msg)
                failed += 1
    return {"synced": synced, "failed": failed, "conflicts": conflicts, "remaining": len(_load_pending_items(owner_email))}
