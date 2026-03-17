const WEBAPP_API_CONFIG = {
  TIMEZONE: 'Asia/Taipei',
  HEADER_KEY_ROW: 1,
  HEADER_LABEL_ROW: 2,
  DATA_START_ROW: 3,

  SYSTEMS: {
    expense: {
      spreadsheetId: '1_l3O2VImO7vFhe1MZS0t4ktbHD4v53QSekj19BcRjBw',
      sheets: {
        submitted: '申請表單',
        draft: '草稿列表',
        users: 'Users',
        userDefaults: 'UserDefaults',
        options: 'Options',
        logs: '操作紀錄',
        settings: '系統設定',
        syncEvents: '同步事件',
        deletedArchive: '刪除備援',
      },
      formType: 'expense',
    },
    travel: {
      spreadsheetId: '1qC_4HAcKJPJ3vIAh_X9bZ8Fnw0DGYIr3NRGD669QK2Q',
      sheets: {
        submitted: '申請表單',
        draft: '草稿列表',
        users: 'Users',
        userDefaults: 'UserDefaults',
        options: 'Options',
        logs: '操作紀錄',
        settings: '系統設定',
        syncEvents: '同步事件',
        deletedArchive: '刪除備援',
      },
      formType: 'travel',
    },
  },
};

const DRIVE_ROOT_FOLDER_IDS = {
  expense: '1fCrjdY48SiXJ2CHBkwrVFkO1hana_sh1',
  travel: '1Nuva1xe__N6CRpRUcx4Fi7_bwXwhN_Zw',
};

const RECORD_META_COLUMNS = [
  ['version', '版本'],
  ['last_event_id', '最後事件ID'],
  ['last_synced_at', '最後同步時間'],
  ['deleted_reason', '刪除原因'],
];

function withMetaColumns_(schema) {
  return schema.concat(RECORD_META_COLUMNS);
}

const SHEET_SCHEMAS = {
  expense_submitted: withMetaColumns_([
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['plan_code', '計畫代號'], ['purpose_desc', '用途說明'],
    ['employee_enabled', '員工姓名_是否勾選'], ['employee_name', '員工姓名'], ['employee_no', '工號'],
    ['advance_offset_enabled', '借支沖抵_是否勾選'], ['advance_amount', '借支金額'], ['offset_amount', '沖銷金額'],
    ['balance_refund_amount', '餘額退回'], ['supplement_amount', '應補差額'],
    ['vendor_enabled', '逕付廠商_是否勾選'], ['vendor_name', '逕付廠商'], ['vendor_address', '地址'], ['vendor_payee_name', '收款人'],
    ['receipt_count', '憑證編號'], ['amount_untaxed', '未稅金額'], ['tax_mode', '稅額方式'], ['tax_amount', '稅額'], ['amount_total', '金額'],
    ['attachments', '附件'], ['signature_file', '數位簽名檔'],
    ['handler_name', '經辦人'], ['project_manager_name', '計畫主管'], ['department_manager_name', '部門主管'], ['accountant_name', '會計'],
    ['department', '部門'], ['note_public', '備註'], ['remarks_internal', '內部備註'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ]),

  expense_draft: withMetaColumns_([
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['plan_code', '計畫代號'], ['purpose_desc', '用途說明'],
    ['employee_enabled', '員工姓名_是否勾選'], ['employee_name', '員工姓名'], ['employee_no', '工號'],
    ['advance_offset_enabled', '借支沖抵_是否勾選'], ['advance_amount', '借支金額'], ['offset_amount', '沖銷金額'],
    ['balance_refund_amount', '餘額退回'], ['supplement_amount', '應補差額'],
    ['vendor_enabled', '逕付廠商_是否勾選'], ['vendor_name', '逕付廠商'], ['vendor_address', '地址'], ['vendor_payee_name', '收款人'],
    ['receipt_count', '憑證編號'], ['amount_untaxed', '未稅金額'], ['tax_mode', '稅額方式'], ['tax_amount', '稅額'], ['amount_total', '金額'],
    ['attachments', '附件'], ['signature_file', '數位簽名檔'],
    ['handler_name', '經辦人'], ['project_manager_name', '計畫主管'], ['department_manager_name', '部門主管'], ['accountant_name', '會計'],
    ['department', '部門'], ['note_public', '備註'], ['remarks_internal', '內部備註'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ]),

  travel_submitted: withMetaColumns_([
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['employee_name', '出差人'], ['employee_no', '員工編號'], ['department', '部門'], ['plan_code', '計畫代號'],
    ['trip_purpose', '出差事由'], ['from_location', '出發地'], ['to_location', '目的地'],
    ['trip_date_start', '起始日期'], ['trip_time_start', '起始時間'], ['start_time', '起始時間_原始'], ['trip_date_end', '結束日期'], ['trip_time_end', '結束時間'], ['end_time', '結束時間_原始'], ['trip_days', '共天'],
    ['transport_tools', '交通方式'], ['transportation_type', '交通方式_字串'], ['gov_car_no', '公務車車號'], ['private_car_km', '私車公里數'], ['private_car_no', '私車車號'], ['other_transport_desc', '其他交通工具說明'],
    ['estimated_cost', '出差費預估'], ['expense_rows', '出差明細_JSON'], ['detail_dates', '出差明細_日期'], ['detail_routes', '出差明細_起訖地點'], ['detail_vehicle_types', '出差明細_車別'], ['detail_transport_fees', '出差明細_交通費'], ['detail_misc_fees', '出差明細_膳雜費'], ['detail_lodging_fees', '出差明細_住宿費'], ['detail_other_fees', '出差明細_其它'], ['detail_receipt_nos', '出差明細_單據編號'], ['amount_total', '合計'], ['amount_total_upper', '總計新台幣'],
    ['attachments', '附件'], ['signature_file', '數位簽名檔'],
    ['handler_name', '出差人'], ['project_manager_name', '計畫主持人'], ['department_manager_name', '部門主管'], ['accountant_name', '管理處會計'],
    ['note_public', '備註'], ['remarks_internal', '內部備註'], ['send_pdf_to_email', '送出後寄送PDF到信箱'], ['budget_source', '預算來源'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ]),

  travel_draft: withMetaColumns_([
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['employee_name', '出差人'], ['employee_no', '員工編號'], ['department', '部門'], ['plan_code', '計畫代號'],
    ['trip_purpose', '出差事由'], ['from_location', '出發地'], ['to_location', '目的地'],
    ['trip_date_start', '起始日期'], ['trip_time_start', '起始時間'], ['trip_date_end', '結束日期'], ['trip_time_end', '結束時間'], ['trip_days', '共天'],
    ['transport_tools', '交通方式'], ['transportation_type', '交通方式_字串'], ['gov_car_no', '公務車車號'], ['private_car_km', '私車公里數'], ['private_car_no', '私車車號'], ['other_transport_desc', '其他交通工具說明'],
    ['estimated_cost', '出差費預估'], ['expense_rows', '出差明細_JSON'], ['detail_dates', '出差明細_日期'], ['detail_routes', '出差明細_起訖地點'], ['detail_vehicle_types', '出差明細_車別'], ['detail_transport_fees', '出差明細_交通費'], ['detail_misc_fees', '出差明細_膳雜費'], ['detail_lodging_fees', '出差明細_住宿費'], ['detail_other_fees', '出差明細_其它'], ['detail_receipt_nos', '出差明細_單據編號'], ['amount_total', '合計'], ['amount_total_upper', '總計新台幣'],
    ['attachments', '附件'], ['signature_file', '數位簽名檔'],
    ['handler_name', '出差人'], ['project_manager_name', '計畫主持人'], ['department_manager_name', '部門主管'], ['accountant_name', '管理處會計'],
    ['note_public', '備註'], ['remarks_internal', '內部備註'], ['send_pdf_to_email', '送出後寄送PDF到信箱'], ['budget_source', '預算來源'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ]),

  users: [
    ['name', '姓名'], ['email', 'Email'], ['role', '角色'], ['employee_no', '員工編號'], ['department', '部門'],
    ['is_active', '是否啟用'], ['sort_order', '排序'], ['can_view_all', '可看全部'], ['can_edit_all', '可編輯全部'], ['can_delete_all', '可刪除全部'], ['can_hard_delete', '可永久刪除'],
  ],

  user_defaults: [
    ['email', 'Email'], ['default_employee_name', '預設姓名'], ['default_employee_no', '預設員編'], ['default_department', '預設部門'], ['default_plan_code', '預設計畫代號'],
    ['default_handler_name', '預設經辦人/出差人'], ['default_project_manager_name', '預設計畫主管/主持人'], ['default_department_manager_name', '預設部門主管'], ['default_accountant_name', '預設會計/管理處會計'],
    ['default_note_public', '預設備註'], ['default_trip_time_start', '預設出差起始時間'], ['default_trip_time_end', '預設出差結束時間'], ['is_active', '是否啟用'],
  ],

  options: [
    ['option_type', '選項類型'], ['option_value', '選項值'], ['sort_order', '排序'], ['is_active', '是否啟用'], ['remark', '備註'],
  ],

  logs: [
    ['log_id', '紀錄編號'], ['record_id', '表單編號'], ['action', '動作'], ['actor_name', '執行者姓名'], ['actor_email', '執行者Email'], ['actor_role', '執行者角色'],
    ['target_status_before', '原狀態'], ['target_status_after', '新狀態'], ['action_time', '動作時間'], ['action_result', '結果'], ['message', '訊息'],
  ],

  settings: [
    ['setting_key', '設定鍵'], ['setting_value', '設定值'], ['remark', '備註'],
  ],

  sync_events: [
    ['event_id', '事件ID'], ['record_id', '表單編號'], ['system', '系統'], ['action', '動作'], ['request_hash', '請求雜湊'],
    ['expected_version', '預期版本'], ['applied_version', '套用後版本'], ['status', '狀態'], ['actor_email', '執行者Email'],
    ['created_at', '建立時間'], ['response_json', '回應JSON'], ['message', '訊息'],
  ],

  deleted_archive: [
    ['archive_id', '備援編號'], ['record_id', '表單編號'], ['system', '系統'], ['from_sheet', '原分頁'], ['deleted_at', '刪除時間'],
    ['deleted_by', '刪除者'], ['delete_action', '刪除動作'], ['record_json', '資料JSON'], ['version', '版本'], ['last_event_id', '最後事件ID'],
  ],
};

function setupAllSystems() { setupSystem_('expense'); setupSystem_('travel'); }
function setupExpenseSystem() { setupSystem_('expense'); }
function setupTravelSystem() { setupSystem_('travel'); }

function setupSystem_(systemKey) {
  const system = requireSystem_(systemKey);
  const ss = SpreadsheetApp.openById(system.spreadsheetId);
  if (systemKey === 'expense') {
    ensureSheetSchema_(ss, system.sheets.submitted, SHEET_SCHEMAS.expense_submitted);
    ensureSheetSchema_(ss, system.sheets.draft, SHEET_SCHEMAS.expense_draft);
  } else {
    ensureSheetSchema_(ss, system.sheets.submitted, SHEET_SCHEMAS.travel_submitted);
    ensureSheetSchema_(ss, system.sheets.draft, SHEET_SCHEMAS.travel_draft);
  }
  ensureSheetSchema_(ss, system.sheets.users, SHEET_SCHEMAS.users);
  ensureSheetSchema_(ss, system.sheets.userDefaults, SHEET_SCHEMAS.user_defaults);
  ensureSheetSchema_(ss, system.sheets.options, SHEET_SCHEMAS.options);
  ensureSheetSchema_(ss, system.sheets.logs, SHEET_SCHEMAS.logs);
  ensureSheetSchema_(ss, system.sheets.settings, SHEET_SCHEMAS.settings);
  ensureSheetSchema_(ss, system.sheets.syncEvents, SHEET_SCHEMAS.sync_events);
  ensureSheetSchema_(ss, system.sheets.deletedArchive, SHEET_SCHEMAS.deleted_archive);
  seedDefaultData_(ss, systemKey);
}

function ensureSystemInfra_(system) {
  const ss = SpreadsheetApp.openById(system.spreadsheetId);
  const formSchemaSubmitted = system.formType === 'expense' ? SHEET_SCHEMAS.expense_submitted : SHEET_SCHEMAS.travel_submitted;
  const formSchemaDraft = system.formType === 'expense' ? SHEET_SCHEMAS.expense_draft : SHEET_SCHEMAS.travel_draft;
  ensureSheetSchema_(ss, system.sheets.submitted, formSchemaSubmitted);
  ensureSheetSchema_(ss, system.sheets.draft, formSchemaDraft);
  ensureSheetSchema_(ss, system.sheets.logs, SHEET_SCHEMAS.logs);
  ensureSheetSchema_(ss, system.sheets.syncEvents, SHEET_SCHEMAS.sync_events);
  ensureSheetSchema_(ss, system.sheets.deletedArchive, SHEET_SCHEMAS.deleted_archive);
}

function ensureSheetSchema_(ss, sheetName, schema) {
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) sheet = ss.insertSheet(sheetName);
  const keys = schema.map(r => r[0]);
  const labels = schema.map(r => r[1]);
  const currentCols = sheet.getMaxColumns();
  if (currentCols < keys.length) sheet.insertColumnsAfter(currentCols, keys.length - currentCols);
  sheet.getRange(1, 1, 1, keys.length).setValues([keys]);
  sheet.getRange(2, 1, 1, labels.length).setValues([labels]);
  if (sheet.getFrozenRows() < 2) sheet.setFrozenRows(2);
}

function seedDefaultData_(ss, systemKey) {
  seedUsers_(ss.getSheetByName('Users'));
  seedUserDefaults_(ss.getSheetByName('UserDefaults'));
  seedOptions_(ss.getSheetByName('Options'), systemKey);
  seedSettings_(ss.getSheetByName('系統設定'), systemKey);
}

function seedUsers_(sheet) {
  if (sheet.getLastRow() >= 3) return;
  const rows = [
    ['Katherine', 'katherine@example.com', 'admin', 'A001', '化安處', true, 1, true, true, true, true],
    ['測試使用者', 'user@example.com', 'user', 'A002', '化安處', true, 2, false, false, false, false],
  ];
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function seedUserDefaults_(sheet) {
  if (sheet.getLastRow() >= 3) return;
  const rows = [
    ['katherine@example.com', 'Katherine', 'A001', '化安處', 'TEST-001', 'Katherine', '主管A', '處長A', '會計A', '', '09:00', '17:00', true],
    ['user@example.com', '測試使用者', 'A002', '化安處', 'TEST-002', '測試使用者', '主管B', '處長B', '會計B', '', '09:00', '17:00', true],
  ];
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function seedOptions_(sheet, systemKey) {
  if (sheet.getLastRow() >= 3) return;
  let rows = [
    ['employee_name', 'Katherine', 1, true, ''], ['employee_name', '測試使用者', 2, true, ''],
    ['employee_no', 'A001', 1, true, ''], ['employee_no', 'A002', 2, true, ''],
    ['department', '化安處', 1, true, ''], ['plan_code', 'TEST-001', 1, true, ''], ['plan_code', 'TEST-002', 2, true, ''],
  ];
  if (systemKey === 'expense') rows = rows.concat([['tax_mode', '5%', 1, true, ''], ['tax_mode', '免稅', 2, true, '']]);
  if (systemKey === 'travel') rows = rows.concat([
    ['from_location', '台南', 1, true, ''], ['from_location', '其他', 2, true, ''],
    ['to_location', '台北', 1, true, ''], ['to_location', '新北', 2, true, ''], ['to_location', '新竹', 3, true, ''], ['to_location', '台中', 4, true, ''], ['to_location', '台南', 5, true, ''], ['to_location', '高雄', 6, true, ''], ['to_location', '其他', 7, true, ''],
    ['vehicle_type', '高鐵', 1, true, ''], ['vehicle_type', '台鐵', 2, true, ''], ['vehicle_type', '客運', 3, true, ''], ['vehicle_type', '捷運', 4, true, ''], ['vehicle_type', '公車', 5, true, ''], ['vehicle_type', '計程車', 6, true, ''], ['vehicle_type', '私車公用', 7, true, ''], ['vehicle_type', '公務車', 8, true, ''], ['vehicle_type', '飛機', 9, true, ''], ['vehicle_type', '船舶', 10, true, ''], ['vehicle_type', '其他', 11, true, ''],
  ]);
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function seedSettings_(sheet, systemKey) {
  if (sheet.getLastRow() >= 3) return;
  const rows = [['system_name', systemKey === 'expense' ? '支出憑證黏存單系統' : '國內出差申請單系統', '系統名稱'], ['version', 'v2026.03.version-idempotent', '版本']];
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function doGet(e) {
  try {
    const params = e && e.parameter ? e.parameter : {};
    const action = (params.action || 'ping').trim();
    let result;
    switch (action) {
      case 'ping': result = ok_('pong', { server_time: nowIso_() }); break;
      case 'users_list': result = handleUsersList_(params); break;
      case 'user_defaults_list': result = handleUserDefaultsList_(params); break;
      case 'options_list': result = handleOptionsList_(params); break;
      case 'record_list_all': result = handleRecordListAll_(params); break;
      default: result = err_('unknown action: ' + action, 'UNKNOWN_ACTION');
    }
    return jsonOutput_(result);
  } catch (error) {
    return jsonOutput_(err_(stringifyError_(error), 'SERVER_ERROR'));
  }
}

function doPost(e) {
  try {
    const body = parseJsonBody_(e);
    const action = ((body.action || '') + '').trim();
    let result;
    switch (action) {
      case 'record_save_draft': result = handleRecordSaveDraft_(body); break;
      case 'record_submit': result = handleRecordSubmit_(body); break;
      case 'record_soft_delete': result = handleRecordSoftDelete_(body); break;
      case 'record_hard_delete': result = handleRecordHardDelete_(body); break;
      case 'record_restore': result = handleRecordRestore_(body); break;
      case 'upload_drive_file': result = handleUploadDriveFile_(body); break;
      case 'delete_drive_file': result = handleDeleteDriveFile_(body); break;
      case 'get_drive_file_content': result = handleGetDriveFileContent_(body); break;
      default: result = err_('unknown action: ' + action, 'UNKNOWN_ACTION');
    }
    return jsonOutput_(result);
  } catch (error) {
    return jsonOutput_(err_(stringifyError_(error), 'SERVER_ERROR'));
  }
}

function handleUploadDriveFile_(body) {
  const system = requireSystem_(body.system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const filename = String(payload.filename || '').trim();
  const contentBase64 = String(payload.content_base64 || '').trim();
  const mimeType = String(payload.mime_type || 'application/octet-stream').trim() || 'application/octet-stream';
  const category = String(payload.category || 'attachment').trim() || 'attachment';
  const recordId = String(payload.record_id || '').trim();
  const ownerEmail = normalizeEmail_(payload.owner_email || actor.email || '');

  if (!filename) return err_('filename is required', 'VALIDATION_ERROR');
  if (!contentBase64) return err_('content_base64 is required', 'VALIDATION_ERROR');

  const bytes = Utilities.base64Decode(contentBase64);
  const folder = getAttachmentCategoryFolder_(system, category);
  const safeName = buildSafeDriveFilename_(recordId, filename);
  const file = folder.createFile(bytes, safeName, mimeType);

  const data = {
    drive_file_id: file.getId(),
    drive_url: file.getUrl(),
    drive_folder_id: folder.getId(),
    name: file.getName(),
    filename: file.getName(),
    mime_type: mimeType,
    size: bytes.length,
    category: category,
    record_id: recordId,
    owner_email: ownerEmail,
    updated_at: nowIso_(),
  };
  return ok_('file uploaded', data);
}

function handleDeleteDriveFile_(body) {
  const payload = body.payload || {};
  const fileId = String(payload.drive_file_id || payload.file_id || '').trim();
  if (!fileId) return err_('drive_file_id is required', 'VALIDATION_ERROR');
  const file = DriveApp.getFileById(fileId);
  file.setTrashed(true);
  return ok_('file trashed', { drive_file_id: fileId, trashed: true });
}

function handleGetDriveFileContent_(body) {
  const payload = body.payload || {};
  const fileId = String(payload.drive_file_id || payload.file_id || '').trim();
  if (!fileId) return err_('drive_file_id is required', 'VALIDATION_ERROR');
  const file = DriveApp.getFileById(fileId);
  const blob = file.getBlob();
  const data = {
    drive_file_id: fileId,
    filename: file.getName(),
    name: file.getName(),
    mime_type: blob.getContentType(),
    size: blob.getBytes().length,
    content_base64: Utilities.base64Encode(blob.getBytes()),
  };
  return ok_('file content loaded', data);
}

function handleUsersList_(params) {
  const system = requireSystem_(params.system);
  ensureSystemInfra_(system);
  const rows = readSheetObjects_(system, system.sheets.users)
    .filter(r => truthy_(r.is_active) || r.is_active === '' || r.is_active === undefined)
    .sort((a, b) => num_(a.sort_order) - num_(b.sort_order));
  return ok_('users loaded', { rows: rows, count: rows.length });
}

function handleUserDefaultsList_(params) {
  const system = requireSystem_(params.system);
  ensureSystemInfra_(system);
  let rows = readSheetObjects_(system, system.sheets.userDefaults)
    .filter(r => truthy_(r.is_active) || r.is_active === '' || r.is_active === undefined);
  const email = normalizeEmail_(params.email || '');
  if (email) rows = rows.filter(r => normalizeEmail_(r.email) === email);
  return ok_('user defaults loaded', { rows: rows, count: rows.length });
}

function handleOptionsList_(params) {
  const system = requireSystem_(params.system);
  ensureSystemInfra_(system);
  let rows = readSheetObjects_(system, system.sheets.options)
    .filter(r => truthy_(r.is_active) || r.is_active === '' || r.is_active === undefined)
    .sort((a, b) => num_(a.sort_order) - num_(b.sort_order));
  const optionType = (params.option_type || '').trim();
  if (optionType) rows = rows.filter(r => (r.option_type || '') === optionType);
  return ok_('options loaded', { rows: rows, count: rows.length });
}

function handleRecordListAll_(params) {
  const system = requireSystem_(params.system);
  ensureSystemInfra_(system);
  const actor = buildActorFromParams_(params);
  let submittedRows = readSheetObjects_(system, system.sheets.submitted).map(r => { r._sheet_name = system.sheets.submitted; return r; });
  let draftRows = readSheetObjects_(system, system.sheets.draft).map(r => { r._sheet_name = system.sheets.draft; return r; });
  let allRows = dedupeRowsByRecordId_(submittedRows.concat(draftRows));
  const status = ((params.status || '') + '').trim();
  if (status) allRows = allRows.filter(r => String(r.status || '') === status);
  const ownerOnly = ((params.owner_only || '') + '').trim().toLowerCase() === 'true';
  if (ownerOnly && actor.email) allRows = allRows.filter(r => normalizeEmail_(r.user_email) === normalizeEmail_(actor.email));
  allRows.sort((a, b) => compareRowsDesc_(a, b));
  return ok_('records loaded', { rows: allRows, count: allRows.length });
}

function handleRecordSaveDraft_(body) {
  return processRecordWrite_(body, 'draft');
}

function handleRecordSubmit_(body) {
  return processRecordWrite_(body, 'submitted');
}

function handleRecordSoftDelete_(body) {
  const system = requireSystem_(body.system);
  ensureSystemInfra_(system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const recordId = ((payload.record_id || '') + '').trim();
  if (!recordId) return err_('record_id is required', 'VALIDATION_ERROR');
  const meta = buildMutationMeta_(payload, body, actor, 'soft_delete');
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const replay = getReplayIfHandled_(system, meta.eventId);
    if (replay) return replay;
    const existing = findRecordAnywhere_(system, recordId);
    if (!existing) return err_('record not found', 'NOT_FOUND');
    const conflict = checkVersionConflict_(existing.record, meta);
    if (conflict) return conflict;
    const targetStatus = String(payload.status || '').trim().toLowerCase() || (String(existing.record.status || '').trim().toLowerCase() === 'submitted' ? 'void' : 'deleted');
    const now = nowIso_();
    const record = Object.assign({}, existing.record, {
      status: targetStatus,
      is_deleted: targetStatus === 'deleted',
      deleted_at: now,
      deleted_by: actor.email || '',
      deleted_reason: String(payload.deleted_reason || payload.reason || ''),
      updated_at: now,
      updated_by: actor.email || '',
      version: currentVersion_(existing.record) + 1,
      last_event_id: meta.eventId,
      last_synced_at: now,
    });
    archiveRecord_(system, existing.sheetName, record, actor, 'soft_delete');
    upsertRecordToSheet_(system, system.sheets.draft, record, true);
    if (existing.sheetName !== system.sheets.draft) deleteRecordByIdFromSheet_(system, existing.sheetName, recordId);
    appendLog_(system, recordId, 'soft_delete', actor, existing.record.status || '', targetStatus, 'success', 'soft delete applied');
    const response = ok_('record soft deleted', { record_id: recordId, status: targetStatus, version: record.version, event_id: meta.eventId, idempotent: false });
    appendSyncEvent_(system, meta, recordId, targetStatus, record.version, 'applied', response, 'soft delete applied');
    return response;
  } finally { lock.releaseLock(); }
}

function handleRecordHardDelete_(body) {
  const system = requireSystem_(body.system);
  ensureSystemInfra_(system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const recordId = ((payload.record_id || '') + '').trim();
  if (!recordId) return err_('record_id is required', 'VALIDATION_ERROR');
  const meta = buildMutationMeta_(payload, body, actor, 'hard_delete');
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const replay = getReplayIfHandled_(system, meta.eventId);
    if (replay) return replay;
    const existing = findRecordAnywhere_(system, recordId);
    if (!existing) return err_('record not found', 'NOT_FOUND');
    const conflict = checkVersionConflict_(existing.record, meta);
    if (conflict) return conflict;
    archiveRecord_(system, existing.sheetName, existing.record, actor, 'hard_delete');
    deleteRecordByIdFromSheet_(system, existing.sheetName, recordId);
    appendLog_(system, recordId, 'hard_delete', actor, existing.record.status || '', '', 'success', 'hard delete applied');
    const response = ok_('record hard deleted', { record_id: recordId, event_id: meta.eventId, idempotent: false, deleted_version: currentVersion_(existing.record) });
    appendSyncEvent_(system, meta, recordId, '', currentVersion_(existing.record), 'applied', response, 'hard delete applied');
    return response;
  } finally { lock.releaseLock(); }
}

function handleRecordRestore_(body) {
  const system = requireSystem_(body.system);
  ensureSystemInfra_(system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const recordId = ((payload.record_id || '') + '').trim();
  if (!recordId) return err_('record_id is required', 'VALIDATION_ERROR');
  const meta = buildMutationMeta_(payload, body, actor, 'restore');
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const replay = getReplayIfHandled_(system, meta.eventId);
    if (replay) return replay;
    const existing = findRecordAnywhere_(system, recordId);
    if (!existing) return err_('record not found', 'NOT_FOUND');
    const conflict = checkVersionConflict_(existing.record, meta);
    if (conflict) return conflict;
    const beforeStatus = String(existing.record.status || '').trim().toLowerCase();
    const restoredStatus = beforeStatus === 'void' ? 'submitted' : 'draft';
    const now = nowIso_();
    const record = Object.assign({}, existing.record, {
      status: restoredStatus,
      is_deleted: false,
      deleted_at: '',
      deleted_by: '',
      deleted_reason: '',
      updated_at: now,
      updated_by: actor.email || '',
      version: currentVersion_(existing.record) + 1,
      last_event_id: meta.eventId,
      last_synced_at: now,
    });
    const targetSheet = restoredStatus === 'submitted' ? system.sheets.submitted : system.sheets.draft;
    upsertRecordToSheet_(system, targetSheet, record, true);
    if (existing.sheetName !== targetSheet) deleteRecordByIdFromSheet_(system, existing.sheetName, recordId);
    appendLog_(system, recordId, 'restore', actor, beforeStatus, restoredStatus, 'success', 'restore applied');
    const response = ok_('record restored', { record_id: recordId, status: restoredStatus, version: record.version, event_id: meta.eventId, idempotent: false });
    appendSyncEvent_(system, meta, recordId, restoredStatus, record.version, 'applied', response, 'restore applied');
    return response;
  } finally { lock.releaseLock(); }
}

function processRecordWrite_(body, finalStatus) {
  const system = requireSystem_(body.system);
  ensureSystemInfra_(system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    const meta = buildMutationMeta_(payload, body, actor, finalStatus === 'submitted' ? 'submit' : 'save_draft');
    const replay = getReplayIfHandled_(system, meta.eventId);
    if (replay) return replay;
    const recordId = String(payload.record_id || '').trim();
    const existing = recordId ? findRecordAnywhere_(system, recordId) : null;
    const conflict = checkVersionConflict_(existing ? existing.record : null, meta);
    if (conflict) return conflict;
    const record = sanitizeRecordForWrite_(system, payload, actor, finalStatus, existing ? existing.record : null, meta);
    if (!record.record_id) record.record_id = generateRecordId_(system, record, actor);
    const targetSheet = finalStatus === 'submitted' ? system.sheets.submitted : system.sheets.draft;
    if (existing) {
      record.created_at = existing.record.created_at || record.created_at;
      record.created_by = existing.record.created_by || record.created_by;
      upsertRecordToSheet_(system, targetSheet, record, true);
      if (existing.sheetName !== targetSheet) deleteRecordByIdFromSheet_(system, existing.sheetName, record.record_id);
      appendLog_(system, record.record_id, finalStatus === 'submitted' ? 'submit' : 'save_draft', actor, existing.record.status || '', finalStatus, 'success', 'record upserted');
    } else {
      upsertRecordToSheet_(system, targetSheet, record, true);
      appendLog_(system, record.record_id, finalStatus === 'submitted' ? 'submit' : 'save_draft', actor, '', finalStatus, 'success', 'record inserted');
    }
    const response = ok_(finalStatus === 'submitted' ? 'record submitted' : 'draft saved', {
      record_id: record.record_id,
      status: finalStatus,
      version: record.version,
      updated_at: record.updated_at,
      event_id: meta.eventId,
      idempotent: false,
    });
    appendSyncEvent_(system, meta, record.record_id, finalStatus, record.version, 'applied', response, 'write applied');
    return response;
  } finally { lock.releaseLock(); }
}

function getDriveRootFolder_(system) {
  const folderId = DRIVE_ROOT_FOLDER_IDS[system.formType];
  if (!folderId) throw new Error('Drive root folder id not configured for ' + system.formType);
  return DriveApp.getFolderById(folderId);
}

function getAttachmentCategoryFolder_(system, category) {
  const root = getDriveRootFolder_(system);
  return findOrCreateFolderByName_(root, String(category || 'attachment').trim() || 'attachment');
}

function findOrCreateFolderByName_(parentFolder, name) {
  const it = parentFolder.getFoldersByName(name);
  if (it.hasNext()) return it.next();
  return parentFolder.createFolder(name);
}

function buildSafeDriveFilename_(recordId, filename) {
  const base = String(filename || 'upload.bin').replace(/[\\\/:*?"<>|]+/g, '_').trim() || 'upload.bin';
  if (!recordId) return base;
  return recordId + '__' + base;
}

function readSheetObjects_(system, sheetName) {
  const sheet = getSheet_(system, sheetName);
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < WEBAPP_API_CONFIG.DATA_START_ROW || lastCol < 1) return [];
  const headers = sheet.getRange(WEBAPP_API_CONFIG.HEADER_KEY_ROW, 1, 1, lastCol).getValues()[0];
  const rowCount = lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1;
  const values = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, 1, rowCount, lastCol).getValues();
  return values.map((row, idx) => rowToObject_(headers, row, WEBAPP_API_CONFIG.DATA_START_ROW + idx)).filter(obj => !isEmptyRowObject_(obj, headers));
}

function upsertRecordToSheet_(system, sheetName, record, allowInsert) {
  const sheet = getSheet_(system, sheetName);
  const headers = getHeaderKeys_(sheet);
  const lastRow = sheet.getLastRow();
  let targetRow = null;
  if (lastRow >= WEBAPP_API_CONFIG.DATA_START_ROW) {
    const idCol = findHeaderIndex_(headers, 'record_id');
    if (idCol >= 0) {
      const idValues = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, idCol + 1, lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1, 1).getValues().flat();
      for (let i = 0; i < idValues.length; i++) {
        if (((idValues[i] || '') + '').trim() === record.record_id) { targetRow = WEBAPP_API_CONFIG.DATA_START_ROW + i; break; }
      }
    }
  }
  const rowValues = headers.map(h => record[h] !== undefined ? record[h] : '');
  if (targetRow) { sheet.getRange(targetRow, 1, 1, headers.length).setValues([rowValues]); return targetRow; }
  if (!allowInsert) throw new Error('record not found and insert not allowed');
  const insertRow = Math.max(sheet.getLastRow() + 1, WEBAPP_API_CONFIG.DATA_START_ROW);
  ensureSheetRows_(sheet, insertRow);
  sheet.getRange(insertRow, 1, 1, headers.length).setValues([rowValues]);
  return insertRow;
}

function deleteRecordByIdFromSheet_(system, sheetName, recordId) {
  const sheet = getSheet_(system, sheetName);
  const headers = getHeaderKeys_(sheet);
  const idCol = findHeaderIndex_(headers, 'record_id');
  if (idCol < 0) return false;
  const lastRow = sheet.getLastRow();
  if (lastRow < WEBAPP_API_CONFIG.DATA_START_ROW) return false;
  const idValues = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, idCol + 1, lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1, 1).getValues().flat();
  for (let i = idValues.length - 1; i >= 0; i--) {
    if (((idValues[i] || '') + '').trim() === recordId) { sheet.deleteRow(WEBAPP_API_CONFIG.DATA_START_ROW + i); return true; }
  }
  return false;
}

function findRecordAnywhere_(system, recordId) {
  const submitted = readSheetObjects_(system, system.sheets.submitted);
  const foundSubmitted = submitted.find(r => ((r.record_id || '') + '').trim() === recordId);
  if (foundSubmitted) return { sheetName: system.sheets.submitted, record: foundSubmitted };
  const draft = readSheetObjects_(system, system.sheets.draft);
  const foundDraft = draft.find(r => ((r.record_id || '') + '').trim() === recordId);
  if (foundDraft) return { sheetName: system.sheets.draft, record: foundDraft };
  return null;
}

function sanitizeRecordForWrite_(system, payload, actor, finalStatus, existingRecord, mutationMeta) {
  const now = nowIso_();
  const clean = Object.assign({}, payload || {});

  clean.record_id = (clean.record_id || '').trim();
  clean.status = finalStatus;
  clean.form_type = system.formType;
  clean.owner_name = clean.owner_name || actor.name || '';
  clean.user_email = normalizeEmail_(clean.user_email || actor.email || '');
  clean.actor_role = actor.role || 'user';
  if (!clean.created_at) clean.created_at = (existingRecord && existingRecord.created_at) || now;
  if (!clean.created_by) clean.created_by = (existingRecord && existingRecord.created_by) || actor.email || '';
  clean.updated_at = now;
  clean.updated_by = actor.email || '';
  clean.version = currentVersion_(existingRecord) + 1;
  clean.last_event_id = mutationMeta.eventId;
  clean.last_synced_at = now;
  if (finalStatus === 'submitted') {
    clean.submitted_at = now;
    clean.submitted_by = actor.email || '';
    clean.is_deleted = false;
  }
  if (finalStatus === 'draft') {
    clean.is_deleted = false;
    if (!clean.submitted_at && existingRecord) clean.submitted_at = existingRecord.submitted_at || '';
    if (!clean.submitted_by && existingRecord) clean.submitted_by = existingRecord.submitted_by || '';
  }
  if (!clean.source_system) clean.source_system = system.formType;

  if (system.formType === 'expense') {
    clean.department = clean.department || '化安處';
    clean.amount_untaxed = Math.round(Number(clean.amount_untaxed || 0));
    clean.tax_amount = Math.round(Number(clean.tax_amount || 0));
    clean.amount_total = Math.round(Number(clean.amount_total || 0));
    clean.receipt_count = Math.round(Number(clean.receipt_count || 0));
    clean.attachments = normalizeJsonText_(clean.attachments || clean.attachment_files || []);
    clean.signature_file = normalizeJsonText_(clean.signature_file || '');
    clean.handler_name = clean.handler_name || '';
    clean.project_manager_name = clean.project_manager_name || '';
    clean.department_manager_name = clean.department_manager_name || '';
    clean.accountant_name = clean.accountant_name || '';
    return clean;
  }

  clean.employee_name = clean.employee_name || clean.traveler || clean.handler_name || '';
  clean.employee_no = clean.employee_no || actor.employee_no || '';
  clean.department = clean.department || actor.department || '';
  clean.plan_code = clean.plan_code || clean.project_id || '';
  clean.trip_purpose = clean.trip_purpose || clean.purpose || '';
  clean.from_location = clean.from_location || clean.departure_location || '';
  clean.to_location = clean.to_location || clean.destination_location || '';
  clean.trip_date_start = normalizeDateText_(clean.trip_date_start || clean.start_date || '');
  clean.trip_date_end = normalizeDateText_(clean.trip_date_end || clean.end_date || '');
  clean.trip_time_start = clean.trip_time_start || clean.start_time || '';
  clean.start_time = clean.start_time || clean.trip_time_start || '';
  clean.trip_time_end = clean.trip_time_end || clean.end_time || '';
  clean.end_time = clean.end_time || clean.trip_time_end || '';
  clean.budget_source = clean.budget_source || '';

  const transportList = normalizeTransportList_(clean.transport_tools || clean.transport_mode || clean.transport_options || clean.transportation_type || []);
  clean.transport_tools = JSON.stringify(transportList, null, 0);
  clean.transportation_type = transportList.join(',');
  clean.gov_car_no = clean.gov_car_no || clean.official_car_plate || '';
  clean.private_car_km = Number(clean.private_car_km || clean.private_mileage || 0) || 0;
  clean.private_car_no = clean.private_car_no || clean.private_car_plate || '';
  clean.other_transport_desc = clean.other_transport_desc || clean.other_transport_note || clean.other_transport || '';

  const rows = normalizeExpenseRows_(clean.expense_rows || clean.details || []);
  clean.expense_rows = JSON.stringify(rows, null, 0);
  const detailCols = flattenExpenseRowsColumns_(rows);
  clean.detail_dates = detailCols.detail_dates;
  clean.detail_routes = detailCols.detail_routes;
  clean.detail_vehicle_types = detailCols.detail_vehicle_types;
  clean.detail_transport_fees = detailCols.detail_transport_fees;
  clean.detail_misc_fees = detailCols.detail_misc_fees;
  clean.detail_lodging_fees = detailCols.detail_lodging_fees;
  clean.detail_other_fees = detailCols.detail_other_fees;
  clean.detail_receipt_nos = detailCols.detail_receipt_nos;
  clean.amount_total = Math.round(Number(clean.amount_total || sumExpenseRows_(rows)) || 0);
  clean.amount_total_upper = clean.amount_total_upper || numberToChineseMoney_(clean.amount_total);
  clean.estimated_cost = Math.round(Number(clean.estimated_cost || clean.amount_total || 0) || 0);

  clean.attachments = normalizeJsonText_(clean.attachments || clean.attachment_files || []);
  clean.signature_file = normalizeJsonText_(clean.signature_file || '');

  clean.handler_name = clean.handler_name || clean.employee_name || '';
  clean.project_manager_name = clean.project_manager_name || '';
  clean.department_manager_name = clean.department_manager_name || '';
  clean.accountant_name = clean.accountant_name || '';
  clean.note_public = clean.note_public || '';
  clean.remarks_internal = clean.remarks_internal || '';
  clean.send_pdf_to_email = truthy_(clean.send_pdf_to_email);
  clean.trip_days = calcTripDays_(clean.trip_date_start, clean.trip_date_end);

  return clean;
}

function buildMutationMeta_(payload, body, actor, action) {
  const eventId = String((payload && payload.event_id) || body.event_id || '').trim() || generateEventId_();
  const expectedVersionRaw = (payload && (payload.expected_version !== undefined ? payload.expected_version : payload.base_version));
  const expectedVersion = expectedVersionRaw === '' || expectedVersionRaw === null || expectedVersionRaw === undefined ? null : Number(expectedVersionRaw);
  return {
    eventId: eventId,
    action: action,
    expectedVersion: isNaN(expectedVersion) ? null : expectedVersion,
    requestHash: makeRequestHash_(payload || {}),
    actorEmail: normalizeEmail_(actor.email || ''),
  };
}

function getReplayIfHandled_(system, eventId) {
  if (!eventId) return null;
  const existing = findSyncEvent_(system, eventId);
  if (!existing || String(existing.status || '') !== 'applied') return null;
  let parsed = {};
  try { parsed = JSON.parse(String(existing.response_json || '{}')); } catch (e) { parsed = {}; }
  if (parsed && typeof parsed === 'object') {
    parsed.ok = parsed.ok !== false;
    parsed.message = parsed.message || 'idempotent replay';
    parsed.data = Object.assign({}, parsed.data || {}, { idempotent: true, event_id: eventId, replayed: true });
    return parsed;
  }
  return ok_('idempotent replay', { idempotent: true, event_id: eventId, replayed: true });
}

function findSyncEvent_(system, eventId) {
  if (!eventId) return null;
  const rows = readSheetObjects_(system, system.sheets.syncEvents);
  return rows.find(r => String(r.event_id || '').trim() === String(eventId).trim()) || null;
}

function appendSyncEvent_(system, meta, recordId, status, appliedVersion, responseObj, responseDataOrMessage, maybeMessage) {
  const sheet = getSheet_(system, system.sheets.syncEvents);
  const headers = getHeaderKeys_(sheet);
  const existing = findSyncEvent_(system, meta.eventId);
  const response = maybeMessage === undefined ? responseObj : responseDataOrMessage;
  const message = maybeMessage === undefined ? responseDataOrMessage : maybeMessage;
  const row = {
    event_id: meta.eventId,
    record_id: recordId || '',
    system: system.formType,
    action: meta.action,
    request_hash: meta.requestHash,
    expected_version: meta.expectedVersion === null ? '' : meta.expectedVersion,
    applied_version: appliedVersion === undefined || appliedVersion === null ? '' : appliedVersion,
    status: status,
    actor_email: meta.actorEmail,
    created_at: nowIso_(),
    response_json: JSON.stringify(response || {}),
    message: String(message || ''),
  };
  if (existing) {
    upsertGenericObjectByKey_(sheet, headers, 'event_id', row, true);
  } else {
    upsertGenericObjectByKey_(sheet, headers, 'event_id', row, true);
  }
}

function archiveRecord_(system, fromSheet, record, actor, deleteAction) {
  const sheet = getSheet_(system, system.sheets.deletedArchive);
  const headers = getHeaderKeys_(sheet);
  const archiveId = 'ARC-' + Utilities.getUuid();
  const row = {
    archive_id: archiveId,
    record_id: record.record_id || '',
    system: system.formType,
    from_sheet: fromSheet || '',
    deleted_at: nowIso_(),
    deleted_by: actor.email || '',
    delete_action: deleteAction || '',
    record_json: JSON.stringify(record || {}),
    version: currentVersion_(record),
    last_event_id: record.last_event_id || '',
  };
  upsertGenericObjectByKey_(sheet, headers, 'archive_id', row, true);
  return archiveId;
}

function appendLog_(system, recordId, action, actor, beforeStatus, afterStatus, result, message) {
  const sheet = getSheet_(system, system.sheets.logs);
  const headers = getHeaderKeys_(sheet);
  const row = {
    log_id: 'LOG-' + Utilities.getUuid(),
    record_id: recordId || '',
    action: action || '',
    actor_name: actor.name || '',
    actor_email: actor.email || '',
    actor_role: actor.role || '',
    target_status_before: beforeStatus || '',
    target_status_after: afterStatus || '',
    action_time: nowIso_(),
    action_result: result || '',
    message: message || '',
  };
  upsertGenericObjectByKey_(sheet, headers, 'log_id', row, true);
}

function checkVersionConflict_(existingRecord, meta) {
  if (!existingRecord || meta.expectedVersion === null) return null;
  const currentVersion = currentVersion_(existingRecord);
  if (currentVersion !== meta.expectedVersion) {
    return err_('version conflict', 'VERSION_CONFLICT', {
      record_id: existingRecord.record_id || '',
      current_version: currentVersion,
      expected_version: meta.expectedVersion,
      updated_at: existingRecord.updated_at || '',
      updated_by: existingRecord.updated_by || '',
      event_id: meta.eventId,
    });
  }
  return null;
}

function currentVersion_(record) {
  const n = Number((record || {}).version || 0);
  return isNaN(n) ? 0 : n;
}

function dedupeRowsByRecordId_(rows) {
  const map = {};
  const extras = [];
  (rows || []).forEach(r => {
    const rid = String((r || {}).record_id || '').trim();
    if (!rid) {
      extras.push(r);
      return;
    }
    if (!map[rid]) {
      map[rid] = r;
      return;
    }
    map[rid] = chooseNewerRow_(map[rid], r);
  });
  return Object.keys(map).map(k => map[k]).concat(extras);
}

function chooseNewerRow_(a, b) {
  const va = currentVersion_(a), vb = currentVersion_(b);
  if (va !== vb) return vb > va ? b : a;
  return compareRowsDesc_(a, b) <= 0 ? a : b;
}

function compareRowsDesc_(a, b) {
  const sa = String((a && (a.updated_at || a.created_at)) || '');
  const sb = String((b && (b.updated_at || b.created_at)) || '');
  return sb.localeCompare(sa);
}

function upsertGenericObjectByKey_(sheet, headers, keyName, obj, allowInsert) {
  const lastRow = sheet.getLastRow();
  const keyCol = findHeaderIndex_(headers, keyName);
  if (keyCol < 0) throw new Error('key column not found: ' + keyName);
  let targetRow = null;
  if (lastRow >= WEBAPP_API_CONFIG.DATA_START_ROW) {
    const values = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, keyCol + 1, lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1, 1).getValues().flat();
    for (let i = 0; i < values.length; i++) {
      if (String(values[i] || '').trim() === String(obj[keyName] || '').trim()) {
        targetRow = WEBAPP_API_CONFIG.DATA_START_ROW + i;
        break;
      }
    }
  }
  const rowValues = headers.map(h => obj[h] !== undefined ? obj[h] : '');
  if (targetRow) {
    sheet.getRange(targetRow, 1, 1, headers.length).setValues([rowValues]);
    return targetRow;
  }
  if (!allowInsert) throw new Error('row not found and insert not allowed');
  const insertRow = Math.max(sheet.getLastRow() + 1, WEBAPP_API_CONFIG.DATA_START_ROW);
  ensureSheetRows_(sheet, insertRow);
  sheet.getRange(insertRow, 1, 1, headers.length).setValues([rowValues]);
  return insertRow;
}

function makeRequestHash_(payload) {
  const text = JSON.stringify(payload || {});
  const bytes = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, text, Utilities.Charset.UTF_8);
  return bytes.map(function(b) { const s = (b < 0 ? b + 256 : b).toString(16); return ('0' + s).slice(-2); }).join('');
}

function generateEventId_() {
  return 'EVT-' + Utilities.getUuid();
}

function normalizeDateText_(v) {
  if (!v) return '';
  const s = String(v).trim().replace(/\//g, '-');
  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!m) return s;
  return m[1] + '-' + ('0' + m[2]).slice(-2) + '-' + ('0' + m[3]).slice(-2);
}

function normalizeTransportList_(value) {
  if (Array.isArray(value)) return value.map(x => String(x || '').trim()).filter(Boolean);
  const s = String(value || '').trim();
  if (!s) return [];
  try {
    const parsed = JSON.parse(s);
    if (Array.isArray(parsed)) return parsed.map(x => String(x || '').trim()).filter(Boolean);
  } catch (e) {}
  return s.split(',').map(x => x.trim()).filter(Boolean);
}

function normalizeExpenseRows_(value) {
  let rows = value;
  if (typeof rows === 'string') {
    try { rows = JSON.parse(rows); } catch (e) { rows = []; }
  }
  if (!Array.isArray(rows)) rows = [];
  return rows.map(r => ({
    '日期': normalizeDateText_(r['日期'] || r.date || ''),
    '起訖地點': r['起訖地點'] || r.route || '',
    '車別': r['車別'] || r.vehicle_type || '',
    '交通費': Number(r['交通費'] || r.transport_fee || 0) || 0,
    '膳雜費': Number(r['膳雜費'] || r.meal_fee || 0) || 0,
    '住宿費': Number(r['住宿費'] || r.lodging_fee || 0) || 0,
    '其它': Number(r['其它'] || r.other_fee || 0) || 0,
    '單據編號': r['單據編號'] || r.receipt_no || ''
  }));
}

function sumExpenseRows_(rows) {
  return (rows || []).reduce((acc, r) => acc + Number(r['交通費'] || 0) + Number(r['膳雜費'] || 0) + Number(r['住宿費'] || 0) + Number(r['其它'] || 0), 0);
}

function flattenExpenseRowsColumns_(rows) {
  const safe = Array.isArray(rows) ? rows : [];
  const line = key => safe.map(r => String(r[key] === undefined || r[key] === null ? '' : r[key]).trim()).join('\n');
  return {
    detail_dates: line('日期'),
    detail_routes: line('起訖地點'),
    detail_vehicle_types: line('車別'),
    detail_transport_fees: line('交通費'),
    detail_misc_fees: line('膳雜費'),
    detail_lodging_fees: line('住宿費'),
    detail_other_fees: line('其它'),
    detail_receipt_nos: line('單據編號'),
  };
}

function normalizeJsonText_(value) {
  if (value === '' || value === null || value === undefined) return '';
  if (typeof value === 'string') {
    const s = value.trim();
    if (!s) return '';
    try { JSON.parse(s); return s; } catch (e) { return JSON.stringify(value); }
  }
  return JSON.stringify(value);
}

function calcTripDays_(startDate, endDate) {
  try {
    const s = new Date(startDate); const e = new Date(endDate);
    if (isNaN(s.getTime()) || isNaN(e.getTime())) return '';
    const diff = Math.floor((e - s) / (1000 * 60 * 60 * 24)) + 1;
    return diff < 1 ? 1 : diff;
  } catch (e) { return ''; }
}

function numberToChineseMoney_(num) { if (num === '' || num === null || num === undefined) return ''; return String(num); }
function getSheet_(system, sheetName) { const ss = SpreadsheetApp.openById(system.spreadsheetId); let sheet = ss.getSheetByName(sheetName); if (!sheet) sheet = ss.insertSheet(sheetName); return sheet; }
function getHeaderKeys_(sheet) { const lastCol = sheet.getLastColumn(); return sheet.getRange(WEBAPP_API_CONFIG.HEADER_KEY_ROW, 1, 1, lastCol).getValues()[0]; }
function findHeaderIndex_(headers, key) { return headers.indexOf(key); }
function rowToObject_(headers, row, rowNumber) { const obj = {}; headers.forEach((h, i) => { obj[h] = row[i]; }); obj._row_number = rowNumber; return obj; }
function isEmptyRowObject_(obj, headers) { for (let i = 0; i < headers.length; i++) { const v = obj[headers[i]]; if (v !== '' && v !== null && v !== undefined) return false; } return true; }
function ensureSheetRows_(sheet, targetRow) { const maxRows = sheet.getMaxRows(); if (maxRows < targetRow) sheet.insertRowsAfter(maxRows, targetRow - maxRows); }
function parseJsonBody_(e) { if (!e || !e.postData || !e.postData.contents) throw new Error('missing post body'); return JSON.parse(e.postData.contents); }
function buildActorFromParams_(params) { return normalizeActor_({ name: params.actor_name || '', email: params.actor_email || '', role: params.actor_role || 'user' }); }
function normalizeActor_(actor) { return { name: (actor.name || '').trim(), email: normalizeEmail_(actor.email || ''), role: (actor.role || 'user').trim(), employee_no: actor.employee_no || '', department: actor.department || '' }; }
function requireSystem_(systemKey) { const key = ((systemKey || '') + '').trim().toLowerCase(); const system = WEBAPP_API_CONFIG.SYSTEMS[key]; if (!system) throw new Error('invalid system, expected expense or travel'); return system; }
function generateRecordId_(system, record, actor) {
  const employeeNo = String(record.employee_no || actor.employee_no || '00000').replace(/\D/g, '') || '00000';
  const rawDate = String(record.form_date || Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'yyyy-MM-dd')).replace(/\//g, '-');
  const m = rawDate.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  const yyyy = m ? Number(m[1]) : Number(Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'yyyy'));
  const mm = m ? ('0' + m[2]).slice(-2) : Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'MM');
  const dd = m ? ('0' + m[3]).slice(-2) : Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'dd');
  const rocYmd = ('000' + (yyyy - 1911)).slice(-3) + mm + dd;
  const formPrefix = system.formType === 'travel' ? 'TR' : 'EX';
  const prefix = formPrefix + employeeNo + rocYmd;
  const existingIds = [].concat(readSheetObjects_(system, system.sheets.submitted).map(r => String(r.record_id || '').trim())).concat(readSheetObjects_(system, system.sheets.draft).map(r => String(r.record_id || '').trim()));
  let maxSeq = 0;
  existingIds.forEach(id => { if (id.indexOf(prefix) === 0) { const seq = parseInt(id.slice(prefix.length), 10); if (!isNaN(seq) && seq > maxSeq) maxSeq = seq; } });
  return prefix + ('000' + (maxSeq + 1)).slice(-3);
}
function normalizeEmail_(email) { return String(email || '').trim().toLowerCase(); }
function truthy_(v) { if (v === true) return true; const s = String(v || '').trim().toLowerCase(); return ['true', '1', 'yes', 'y'].indexOf(s) >= 0; }
function num_(v) { const n = Number(v); return isNaN(n) ? 999999 : n; }
function nowIso_() { return Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, "yyyy-MM-dd'T'HH:mm:ss"); }
function ok_(message, data) { return { ok: true, message: message, data: data || {} }; }
function err_(message, code, data) { return { ok: false, message: message, code: code || 'ERROR', data: data || {} }; }
function jsonOutput_(obj) { return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON); }
function stringifyError_(error) { return error.stack || error.message || String(error || 'unknown error'); }
