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
