from __future__ import annotations

import json
import os
from typing import Dict, List, Optional

import pandas as pd

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsStorageError(Exception):
    pass


def _get_gspread_client(service_account_file: str = "", service_account_json: str = ""):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as e:
        raise GoogleSheetsStorageError(
            "Missing dependency. Please install: gspread google-auth"
        ) from e

    if service_account_json:
        info = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        return gspread.authorize(creds)

    if not service_account_file:
        raise GoogleSheetsStorageError(
            "GOOGLE_SERVICE_ACCOUNT_FILE is required (or GOOGLE_SERVICE_ACCOUNT_JSON)."
        )

    if not os.path.exists(service_account_file):
        raise GoogleSheetsStorageError(f"Service account json not found: {service_account_file}")

    creds = Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    return gspread.authorize(creds)


def ensure_worksheet(
    sheet_id: str,
    worksheet_name: str,
    columns: List[str],
    service_account_file: str = "",
    service_account_json: str = "",
    second_header_zh: Optional[List[str]] = None,
) -> None:
    """
    確保 worksheet 存在，且至少有第1列表頭。
    若提供 second_header_zh，則會建立第2列中文表頭。
    """
    gc = _get_gspread_client(service_account_file, service_account_json)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=max(10, len(columns)))

    values = ws.get_all_values()
    if not values:
        ws.append_row(columns, value_input_option="RAW")
        if second_header_zh:
            ws.append_row(second_header_zh, value_input_option="RAW")
        return

    current_header = values[0] if values else []
    if current_header != columns:
        # 覆寫第一列
        end_col = len(columns)
        ws.update(f"A1:{_col_letter(end_col)}1", [columns], value_input_option="RAW")

    if second_header_zh:
        values = ws.get_all_values()
        if len(values) < 2:
            ws.insert_row(second_header_zh, 2, value_input_option="RAW")
        else:
            second_row = values[1]
            if second_row[: len(second_header_zh)] != second_header_zh:
                ws.update(f"A2:{_col_letter(len(second_header_zh))}2", [second_header_zh], value_input_option="RAW")


def load_all_google(
    sheet_id: str,
    worksheet_name: str,
    columns: Optional[List[str]] = None,
    service_account_file: str = "",
    service_account_json: str = "",
    data_start_row: int = 2,
) -> pd.DataFrame:
    """
    從 Google Sheet 讀資料。
    data_start_row:
      - 2 代表第1列表頭，第2列起資料
      - 3 代表第1列英文、第2列中文、第3列起資料
    """
    gc = _get_gspread_client(service_account_file, service_account_json)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=columns or [])

    header = values[0]
    rows = values[data_start_row - 1 :] if len(values) >= data_start_row else []

    df = pd.DataFrame(rows, columns=header)
    if columns:
        for c in columns:
            if c not in df.columns:
                df[c] = ""
        df = df[columns]
    return df.fillna("")


def _find_row_index_by_id(ws, record_id: str, id_col_name: str = "id", header_row: int = 1) -> Optional[int]:
    values = ws.get_all_values()
    if not values or len(values) < header_row:
        return None

    header = values[header_row - 1]
    if id_col_name not in header:
        return None

    id_idx = header.index(id_col_name)
    for i, row in enumerate(values[header_row:], start=header_row + 1):
        if len(row) > id_idx and str(row[id_idx]).strip() == str(record_id).strip():
            return i
    return None


def upsert_record_google(
    sheet_id: str,
    payload: Dict,
    worksheet_name: str,
    columns: Optional[List[str]] = None,
    service_account_file: str = "",
    service_account_json: str = "",
    header_row: int = 1,
) -> None:
    gc = _get_gspread_client(service_account_file, service_account_json)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    values = ws.get_all_values()
    if not values:
        if not columns:
            raise GoogleSheetsStorageError("Worksheet is empty and columns not provided.")
        ws.append_row(columns, value_input_option="RAW")
        header = columns
    else:
        header = values[header_row - 1]

    if columns:
        missing = [c for c in columns if c not in header]
        if missing:
            header = header + missing
            ws.update(f"A{header_row}:{_col_letter(len(header))}{header_row}", [header], value_input_option="RAW")

    record_id = str(payload.get("id", "")).strip()
    if not record_id:
        raise GoogleSheetsStorageError("payload.id is required")

    row = [str(payload.get(col, "")) for col in header]
    row_idx = _find_row_index_by_id(ws, record_id, "id", header_row=header_row)

    if row_idx is None:
        ws.append_row(row, value_input_option="RAW")
    else:
        ws.update(f"A{row_idx}:{_col_letter(len(row))}{row_idx}", [row], value_input_option="RAW")


def delete_record_google(
    sheet_id: str,
    record_id: str,
    worksheet_name: str,
    service_account_file: str = "",
    service_account_json: str = "",
    header_row: int = 1,
) -> None:
    gc = _get_gspread_client(service_account_file, service_account_json)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)

    row_idx = _find_row_index_by_id(ws, str(record_id), "id", header_row=header_row)
    if row_idx is None:
        return
    ws.delete_rows(row_idx)


def build_sheet_url(sheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}"


def _col_letter(n: int) -> str:
    """
    1 -> A, 27 -> AA
    """
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result