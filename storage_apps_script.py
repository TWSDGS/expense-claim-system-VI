from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import requests


class AppsScriptAPIError(Exception):
    pass


@dataclass
class Actor:
    name: str
    email: str
    role: str = "user"
    employee_no: str = ""
    department: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name or "",
            "email": (self.email or "").strip().lower(),
            "role": self.role or "user",
            "employee_no": self.employee_no or "",
            "department": self.department or "",
        }


class AppsScriptStorage:
    def __init__(
        self,
        web_app_url: str,
        system: str,
        timeout: int = 20,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.web_app_url = web_app_url.strip()
        self.system = system.strip().lower()
        self.timeout = timeout
        self.session = session or requests.Session()

        if self.system not in {"expense", "travel"}:
            raise ValueError("system must be 'expense' or 'travel'")
        if not self.web_app_url:
            raise ValueError("web_app_url is required")

    def _get(self, action: str, **params: Any) -> Dict[str, Any]:
        query = {"action": action, "system": self.system}
        query.update({k: v for k, v in params.items() if v is not None})

        try:
            resp = self.session.get(
                self.web_app_url,
                params=query,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise AppsScriptAPIError(f"GET request failed: {exc}") from exc

        return self._parse_response(resp)

    def _post(self, action: str, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = {
            "action": action,
            "system": self.system,
            "actor": actor.to_dict(),
            "payload": payload or {},
        }

        try:
            resp = self.session.post(
                self.web_app_url,
                data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json; charset=utf-8"},
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise AppsScriptAPIError(f"POST request failed: {exc}") from exc

        return self._parse_response(resp)

    @staticmethod
    def _parse_response(resp: requests.Response) -> Dict[str, Any]:
        text = resp.text or ""

        if "<!DOCTYPE html" in text[:200] or "<html" in text[:200].lower():
            raise AppsScriptAPIError(
                "Apps Script did not return JSON. "
                "可能原因：Web App URL 錯誤、未重新部署、權限不足、Code.gs 內部執行錯誤、或 spreadsheetId / sheet 名稱設定錯誤。"
            )

        try:
            data = resp.json()
        except ValueError as exc:
            raise AppsScriptAPIError(f"Invalid JSON response: {text[:500]}") from exc

        if not isinstance(data, dict):
            raise AppsScriptAPIError("Response is not a JSON object")

        if not data.get("ok", False):
            raise AppsScriptAPIError(data.get("message", "Unknown API error"))

        return data

    @staticmethod
    def _rows_from_response(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = data.get("data", {})
        rows = payload.get("rows", [])
        if not isinstance(rows, list):
            raise AppsScriptAPIError("Invalid response shape: data.rows is not a list")
        return rows

    def ping(self) -> Dict[str, Any]:
        return self._get("ping")

    def users_list(self) -> List[Dict[str, Any]]:
        return self._rows_from_response(self._get("users_list"))

    def users_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.users_list())

    def user_defaults_list(self, email: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._rows_from_response(self._get("user_defaults_list", email=email))

    def user_defaults_df(self, email: Optional[str] = None) -> pd.DataFrame:
        return pd.DataFrame(self.user_defaults_list(email=email))

    def options_list(self, option_type: Optional[str] = None) -> List[Dict[str, Any]]:
        return self._rows_from_response(self._get("options_list", option_type=option_type))

    def options_df(self, option_type: Optional[str] = None) -> pd.DataFrame:
        return pd.DataFrame(self.options_list(option_type=option_type))

    def get_all_options_grouped(self) -> dict[str, list[str]]:
        rows = self.options_list(option_type=None)
        grouped: dict[str, list[str]] = {}

        for row in rows:
            option_type = str(row.get("option_type", "")).strip()
            option_value = str(row.get("option_value", "")).strip()
            if not option_type or not option_value:
                continue
            grouped.setdefault(option_type, [])
            if option_value not in grouped[option_type]:
                grouped[option_type].append(option_value)

        return grouped

    def record_list_all(
        self,
        actor: Optional[Actor] = None,
        status: Optional[str] = None,
        owner_only: bool = False,
    ) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {
            "status": status,
            "owner_only": str(owner_only).lower(),
        }
        if actor:
            params.update(
                {
                    "actor_name": actor.name,
                    "actor_email": actor.email,
                    "actor_role": actor.role,
                }
            )
        return self._rows_from_response(self._get("record_list_all", **params))

    def records_df(
        self,
        actor: Optional[Actor] = None,
        status: Optional[str] = None,
        owner_only: bool = False,
    ) -> pd.DataFrame:
        rows = self.record_list_all(actor=actor, status=status, owner_only=owner_only)
        return pd.DataFrame(rows)

    def record_save_draft(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("record_save_draft", actor=actor, payload=payload)

    def record_submit(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("record_submit", actor=actor, payload=payload)

    def record_soft_delete(self, actor: Actor, record_id: str) -> Dict[str, Any]:
        return self._post("record_soft_delete", actor=actor, payload={"record_id": record_id})

    def record_hard_delete(self, actor: Actor, record_id: str) -> Dict[str, Any]:
        return self._post("record_hard_delete", actor=actor, payload={"record_id": record_id})

    def record_restore(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post("record_restore", actor=actor, payload=payload)

    def get_single_user_defaults(self, email: str) -> Dict[str, Any]:
        rows = self.user_defaults_list(email=email)
        return rows[0] if rows else {}

    def get_option_values(self, option_type: str, include_other: bool = True) -> List[str]:
        rows = self.options_list(option_type=option_type)
        values = [str(r.get("option_value", "")).strip() for r in rows if str(r.get("option_value", "")).strip()]
        if include_other and "其他" not in values:
            values.append("其他")
        return values

    def upload_drive_file(
        self,
        actor: Actor,
        *,
        filename: str,
        file_bytes: bytes,
        mime_type: str = '',
        category: str = 'attachment',
        record_id: str = '',
        owner_email: str = '',
    ) -> Dict[str, Any]:
        payload = {
            "filename": filename or 'file.bin',
            "content_base64": base64.b64encode(file_bytes or b'').decode('ascii'),
            "mime_type": mime_type or 'application/octet-stream',
            "category": category or 'attachment',
            "record_id": record_id or '',
            "owner_email": (owner_email or actor.email or '').strip().lower(),
        }
        return self._post("upload_drive_file", actor=actor, payload=payload).get("data", {})

    def delete_drive_file(self, actor: Actor, drive_file_id: str) -> Dict[str, Any]:
        return self._post("delete_drive_file", actor=actor, payload={"drive_file_id": drive_file_id}).get("data", {})

    def download_drive_file(self, actor: Actor, drive_file_id: str) -> Dict[str, Any]:
        data = self._post("get_drive_file_content", actor=actor, payload={"drive_file_id": drive_file_id}).get("data", {})
        content_base64 = str(data.get("content_base64") or "")
        data["file_bytes"] = base64.b64decode(content_base64) if content_base64 else b""
        return data

