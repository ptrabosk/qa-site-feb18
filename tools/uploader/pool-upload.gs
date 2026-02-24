const SPREADSHEET_ID = '1JJpkiAxa-l6t-UMcTY64SOtbhqtJYMrctLVuBjj5Oiw';
const ASSIGNMENTS_SHEET = 'Assignments';
const POOL_SHEET = 'Pool';
const SESSION_LOGS_SHEET = 'Session Logs';
const DATA_SHEET = 'Data';
const UPLOADED_SCENARIOS_SHEET = 'Uploaded Scenarios';
const UPLOADED_TEMPLATES_SHEET = 'Uploaded Templates';
const QA_SESSIONS_SHEET = 'qa_sessions';
const QA_ASSIGNMENT_HISTORY_SHEET = 'qa_assignment_history';
const QA_SNAPSHOTS_SHEET = 'qa_snapshots';

const SESSION_CAP = 20; // Legacy compatibility field; no longer enforced.
const TARGET_QUEUE_SIZE = 5;
const SESSION_TIMEOUT_MINUTES = 10;
const SNAPSHOT_TTL_HOURS = 48;
const MAX_SNAPSHOT_PAYLOAD_CHARS = 45000;

const ASSIGNMENTS_HEADERS = [
  'send_id',
  'assignee_email',
  'status',
  'assignment_id',
  'created_at',
  'updated_at',
  'done_at',
  'editor_token',
  'viewer_token',
  'form_state_json',
  'internal_note',
  'assigned_session_id',
  'assigned_at'
];

const POOL_HEADERS = ['send_id', 'status'];
const SESSION_HEADERS = [
  'session_id',
  'agent_email',
  'state',
  'submitted_count',
  'cap',
  'started_at',
  'last_heartbeat_at',
  'ended_at',
  'end_reason',
  'app_base'
];
const HISTORY_HEADERS = [
  'event_at',
  'event_type',
  'assignment_id',
  'send_id',
  'from_status',
  'to_status',
  'agent_email',
  'session_id',
  'detail'
];
const SNAPSHOT_HEADERS = [
  'snapshot_id',
  'snapshot_token',
  'assignment_id',
  'send_id',
  'created_by_email',
  'created_at',
  'expires_at',
  'status',
  'payload_json'
];

const UPLOADED_JSON_HEADERS = ['record_id', 'payload_json', 'updated_at'];
const ACTIVE_ASSIGNMENT_STATUSES = { ASSIGNED: true, IN_PROGRESS: true };

function doGet(e) {
  try {
    const action = getRequestAction_(e);

    if (action === 'queue') {
      const email = normalizeEmail_(getParam_(e, 'email'));
      const appBase = getParam_(e, 'app_base');
      const sessionId = getParam_(e, 'session_id');
      if (!email) return jsonResponse_({ error: 'Missing required query param: email' });
      if (!sessionId) return jsonResponse_({ error: 'Missing required query param: session_id' });

      return withScriptLock_(function() {
        return jsonResponse_(getOrTopUpQueueForSession_(email, appBase, sessionId));
      });
    }

    if (action === 'getAssignment') {
      const assignmentId = getParam_(e, 'assignment_id');
      const token = getParam_(e, 'token');
      const sessionId = getParam_(e, 'session_id');
      if (!assignmentId || !token || !sessionId) {
        return jsonResponse_({ error: 'Missing required query params: assignment_id, token, session_id' });
      }

      const result = getAssignmentForSession_(assignmentId, token, sessionId);
      if (result.error) return jsonResponse_({ error: result.error });
      return jsonResponse_(result);
    }

    if (action === 'getUploadedScenarios') {
      return jsonResponse_({ scenarios: readUploadedJsonList_(UPLOADED_SCENARIOS_SHEET) });
    }

    if (action === 'getUploadedTemplates') {
      return jsonResponse_({ templates: readUploadedJsonList_(UPLOADED_TEMPLATES_SHEET) });
    }

    if (action === 'getSnapshot') {
      const snapshotId = getParam_(e, 'snapshot_id');
      const snapshotToken = getParam_(e, 'snapshot_token');
      if (!snapshotId || !snapshotToken) {
        return jsonResponse_({ error: 'Missing required query params: snapshot_id, snapshot_token' });
      }

      return withScriptLock_(function() {
        const result = getSnapshotByToken_(snapshotId, snapshotToken);
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    return jsonResponse_({ status: 'ok', message: 'Agent Training Data Collector is running' });
  } catch (error) {
    console.error('Error in doGet:', error);
    return jsonResponse_({ error: String(error) });
  }
}

function doPost(e) {
  try {
    if (!e || !e.postData || !e.postData.contents) {
      throw new Error('No POST data received. This function should be called via HTTP POST.');
    }

    const data = JSON.parse(e.postData.contents);
    const action = getRequestAction_(e, data);

    if (action === 'saveDraft') {
      const assignmentId = data.assignment_id;
      const token = data.token;
      const sessionId = data.session_id;
      if (!assignmentId || !token || !sessionId) {
        return jsonResponse_({ error: 'Missing assignment_id, token, or session_id' });
      }

      return withScriptLock_(function() {
        const result = updateAssignmentDraftForSession_(assignmentId, token, sessionId, data.form_state_json, data.internal_note);
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'done') {
      const assignmentId = data.assignment_id;
      const token = data.token;
      const sessionId = data.session_id;
      if (!assignmentId || !token || !sessionId) {
        return jsonResponse_({ error: 'Missing assignment_id, token, or session_id' });
      }

      return withScriptLock_(function() {
        const result = markAssignmentDoneForSession_(assignmentId, token, sessionId, data.app_base);
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'skipAssignment') {
      const assignmentId = data.assignment_id;
      const token = data.token;
      const sessionId = data.session_id;
      if (!assignmentId || !token || !sessionId) {
        return jsonResponse_({ error: 'Missing assignment_id, token, or session_id' });
      }

      return withScriptLock_(function() {
        const result = skipAssignmentForSession_(
          assignmentId,
          token,
          sessionId,
          data.app_base,
          data.reason
        );
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'heartbeat') {
      const email = normalizeEmail_(data.email);
      const sessionId = String(data.session_id || '').trim();
      if (!email || !sessionId) return jsonResponse_({ error: 'Missing email or session_id' });

      return withScriptLock_(function() {
        const result = heartbeatSession_(email, sessionId, data.client_ts);
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'releaseSession') {
      const email = normalizeEmail_(data.email);
      const sessionId = String(data.session_id || '').trim();
      const reason = String(data.reason || 'manual').trim();
      if (!email || !sessionId) return jsonResponse_({ error: 'Missing email or session_id' });

      return withScriptLock_(function() {
        const result = releaseSession_(email, sessionId, reason || 'manual');
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'createSnapshot') {
      const assignmentId = String(data.assignment_id || '').trim();
      const token = String(data.token || '').trim();
      const sessionId = String(data.session_id || '').trim();
      const appBase = String(data.app_base || '').trim();
      if (!assignmentId || !token || !sessionId) {
        return jsonResponse_({ error: 'Missing assignment_id, token, or session_id' });
      }

      return withScriptLock_(function() {
        const result = createSnapshotForSession_({
          assignment_id: assignmentId,
          token: token,
          session_id: sessionId,
          agent_email: normalizeEmail_(data.agent_email),
          app_base: appBase,
          snapshot_payload: data.snapshot_payload
        });
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'createScenarioSnapshot') {
      return withScriptLock_(function() {
        const result = createScenarioSnapshot_({
          app_base: String(data.app_base || '').trim(),
          agent_email: normalizeEmail_(data.agent_email || ''),
          agent_name: String(data.agent_name || '').trim(),
          snapshot_payload: data.snapshot_payload
        });
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'addToPool') {
      const sendIds = Array.isArray(data.send_ids) ? data.send_ids : [];
      return withScriptLock_(function() {
        const result = addSendIdsToPool_(sendIds);
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'resetAssignmentsFromAudit') {
      const sendIds = Array.isArray(data.send_ids) ? data.send_ids : [];
      return withScriptLock_(function() {
        const result = resetAssignmentsFromAudit_(sendIds);
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_(result);
      });
    }

    if (action === 'setUploadedScenarios') {
      const scenarios = Array.isArray(data.scenarios) ? data.scenarios : [];
      return withScriptLock_(function() {
        writeUploadedJsonList_(UPLOADED_SCENARIOS_SHEET, scenarios);
        return jsonResponse_({ ok: true, scenarios: scenarios });
      });
    }

    if (action === 'appendUploadedScenarios') {
      const scenarios = Array.isArray(data.scenarios) ? data.scenarios : [];
      const reset = !!data.reset;
      return withScriptLock_(function() {
        const result = appendUploadedJsonList_(UPLOADED_SCENARIOS_SHEET, scenarios, { reset: reset });
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_({ ok: true, added: result.added, total: result.total });
      });
    }

    if (action === 'clearUploadedScenarios') {
      return withScriptLock_(function() {
        writeUploadedJsonList_(UPLOADED_SCENARIOS_SHEET, []);
        return jsonResponse_({ ok: true, scenarios: [] });
      });
    }

    if (action === 'setUploadedTemplates') {
      const templates = Array.isArray(data.templates) ? data.templates : [];
      return withScriptLock_(function() {
        writeUploadedJsonList_(UPLOADED_TEMPLATES_SHEET, templates);
        return jsonResponse_({ ok: true, templates: templates });
      });
    }

    if (action === 'appendUploadedTemplates') {
      const templates = Array.isArray(data.templates) ? data.templates : [];
      const reset = !!data.reset;
      return withScriptLock_(function() {
        const result = appendUploadedJsonList_(UPLOADED_TEMPLATES_SHEET, templates, { reset: reset });
        if (result.error) return jsonResponse_({ error: result.error });
        return jsonResponse_({ ok: true, added: result.added, total: result.total });
      });
    }

    if (action === 'clearUploadedTemplates') {
      return withScriptLock_(function() {
        writeUploadedJsonList_(UPLOADED_TEMPLATES_SHEET, []);
        return jsonResponse_({ ok: true, templates: [] });
      });
    }

    // Existing session logging/evaluation/chat logging behavior.
    return handleLegacyPost_(data);
  } catch (error) {
    console.error('Error in doPost:', error);
    return jsonResponse_({ status: 'error', message: String(error), error: String(error) });
  }
}

function getOrTopUpQueueForSession_(email, appBaseUrl, sessionId) {
  const state = loadAssignmentState_();
  const now = nowIso_();
  const baseUrl = resolveAppBaseUrl_(appBaseUrl);

  cleanupStaleSessions_(state, now);
  const session = resolveQueueSession_(state, email, sessionId, baseUrl, now);
  const activeSessionId = String(session && session.session_id ? session.session_id : '').trim();

  if (activeSessionId && isSessionActive_(session)) {
    releaseOtherActiveSessionsForEmail_(state, email, activeSessionId, now);
    releaseActiveAssignmentsForEmailOutsideSession_(state, email, activeSessionId, now, 'superseded');
    topUpQueueForSession_(state, email, activeSessionId, now, baseUrl);
  }

  persistAssignmentState_(state);

  const assignments = getActiveAssignmentsForSession_(state.assignmentsRows, email, activeSessionId)
    .slice(0, TARGET_QUEUE_SIZE)
    .map(function(a) {
      return {
        assignment_id: a.assignment_id,
        send_id: a.send_id,
        status: a.status,
        edit_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.editor_token, 'edit'),
        view_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.viewer_token, 'view')
      };
    });

  return {
    assignments: assignments,
    session: sessionToPayload_(session)
  };
}

function getAssignmentForSession_(assignmentId, token, sessionId) {
  const state = loadAssignmentReadState_();

  const sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  if (sessionIndex < 0) return { error: 'Session not found' };
  const session = rowToSessionObject_(state.sessionsRows[sessionIndex]);

  const assignmentWithIndex = findAssignmentById_(state.assignmentsRows, assignmentId);
  if (!assignmentWithIndex) return { error: 'Assignment not found' };
  const assignment = assignmentWithIndex.assignment;

  const role = tokenRoleForAssignment_(assignment, token);
  if (!role) return { error: 'Unauthorized token' };

  const ownerSessionId = String(assignment.assigned_session_id || '').trim();
  const status = String(assignment.status || '').toUpperCase();
  if (status !== 'DONE' && ownerSessionId !== sessionId) {
    return { error: 'Assignment is not reserved for this session' };
  }

  return {
    assignment: {
      assignment_id: assignment.assignment_id,
      send_id: assignment.send_id,
      status: assignment.status,
      form_state_json: assignment.form_state_json || '',
      internal_note: assignment.internal_note || '',
      role: role
    },
    session: sessionToPayload_(session)
  };
}

function loadAssignmentReadState_() {
  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const assignmentsSheet = getOrCreateSheet_(spreadsheet, ASSIGNMENTS_SHEET, ASSIGNMENTS_HEADERS);
  const sessionsSheet = getOrCreateSheet_(spreadsheet, QA_SESSIONS_SHEET, SESSION_HEADERS);
  const assignmentsRows = getSheetDataRows_(assignmentsSheet, ASSIGNMENTS_HEADERS.length);
  const sessionsRows = getSheetDataRows_(sessionsSheet, SESSION_HEADERS.length);

  return {
    assignmentsRows: assignmentsRows,
    sessionsRows: sessionsRows
  };
}

function updateAssignmentDraftForSession_(assignmentId, token, sessionId, formStateJson, internalNote) {
  const state = loadAssignmentState_();
  const now = nowIso_();

  cleanupStaleSessions_(state, now);

  const sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  if (sessionIndex < 0) return { error: 'Session not found' };
  const session = rowToSessionObject_(state.sessionsRows[sessionIndex]);

  const found = findAssignmentById_(state.assignmentsRows, assignmentId);
  if (!found) return { error: 'Assignment not found' };
  const assignment = found.assignment;

  if (String(assignment.editor_token || '') !== String(token || '')) {
    return { error: 'Unauthorized: editor token required' };
  }

  const status = String(assignment.status || '').toUpperCase();
  if (status !== 'DONE' && String(assignment.assigned_session_id || '') !== sessionId) {
    return { error: 'Assignment is not reserved for this session' };
  }

  assignment.form_state_json = stringifyMaybe_(formStateJson);
  assignment.internal_note = internalNote != null ? String(internalNote) : '';
  assignment.updated_at = now;
  if (status === 'ASSIGNED') {
    assignment.status = 'IN_PROGRESS';
  }

  if (isSessionActive_(session)) {
    session.last_heartbeat_at = now;
  }

  setAssignmentRow_(state, found.index, assignment);
  setSessionRow_(state, sessionIndex, session);
  persistAssignmentState_(state);

  return { ok: true, session: sessionToPayload_(session) };
}

function markAssignmentDoneForSession_(assignmentId, token, sessionId, appBaseUrl) {
  const state = loadAssignmentState_();
  const now = nowIso_();
  const baseUrl = resolveAppBaseUrl_(appBaseUrl);

  cleanupStaleSessions_(state, now);

  const sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  if (sessionIndex < 0) return { error: 'Session not found' };
  const session = rowToSessionObject_(state.sessionsRows[sessionIndex]);
  const email = normalizeEmail_(session.agent_email);

  const found = findAssignmentById_(state.assignmentsRows, assignmentId);
  if (!found) return { error: 'Assignment not found' };
  const assignment = found.assignment;

  if (String(assignment.editor_token || '') !== String(token || '')) {
    return { error: 'Unauthorized: editor token required' };
  }

  if (String(assignment.assigned_session_id || '') !== sessionId) {
    return { error: 'Assignment is not reserved for this session' };
  }

  const beforeStatus = String(assignment.status || '').toUpperCase();
  if (beforeStatus === 'DONE') {
    if (isSessionLive_(session)) {
      session.last_heartbeat_at = now;
      setSessionRow_(state, sessionIndex, session);
      persistAssignmentState_(state);
    }

    const assignmentsAlreadyDone = getActiveAssignmentsForSession_(state.assignmentsRows, email, sessionId)
      .slice(0, TARGET_QUEUE_SIZE)
      .map(function(a) {
        return {
          assignment_id: a.assignment_id,
          send_id: a.send_id,
          status: a.status,
          edit_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.editor_token, 'edit'),
          view_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.viewer_token, 'view')
        };
      });

    return {
      assignments: assignmentsAlreadyDone,
      session: sessionToPayload_(session),
      already_done: true
    };
  }
  if (!ACTIVE_ASSIGNMENT_STATUSES[beforeStatus]) {
    return { error: 'Assignment is not in an active state' };
  }

  assignment.status = 'DONE';
  assignment.updated_at = now;
  assignment.done_at = now;

  const poolIndex = findPoolIndexBySendId_(state.poolRows, assignment.send_id);
  if (poolIndex >= 0) {
    setPoolStatusByIndex_(state, poolIndex, 'DONE');
  }

  appendHistoryRow_(state.historyRowsToAppend, {
    event_type: 'assignment_done',
    assignment_id: assignment.assignment_id,
    send_id: assignment.send_id,
    from_status: beforeStatus,
    to_status: 'DONE',
    agent_email: email,
    session_id: sessionId,
    detail: ''
  });

  const submitted = toInt_(session.submitted_count, 0) + 1;
  session.submitted_count = submitted;
  session.last_heartbeat_at = now;

  if (isSessionActive_(session)) {
    assignFromPool_(state, email, sessionId, now, baseUrl, 1);
  }

  setAssignmentRow_(state, found.index, assignment);
  setSessionRow_(state, sessionIndex, session);
  persistAssignmentState_(state);

  const assignments = getActiveAssignmentsForSession_(state.assignmentsRows, email, sessionId)
    .slice(0, TARGET_QUEUE_SIZE)
    .map(function(a) {
      return {
        assignment_id: a.assignment_id,
        send_id: a.send_id,
        status: a.status,
        edit_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.editor_token, 'edit'),
        view_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.viewer_token, 'view')
      };
    });

  return {
    assignments: assignments,
    session: sessionToPayload_(session)
  };
}

function skipAssignmentForSession_(assignmentId, token, sessionId, appBaseUrl, reason) {
  const state = loadAssignmentState_();
  const now = nowIso_();
  const baseUrl = resolveAppBaseUrl_(appBaseUrl);
  const skipReason = String(reason || 'missing_scenario').trim() || 'missing_scenario';

  cleanupStaleSessions_(state, now);

  const sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  if (sessionIndex < 0) return { error: 'Session not found' };
  const session = rowToSessionObject_(state.sessionsRows[sessionIndex]);
  const email = normalizeEmail_(session.agent_email);

  const found = findAssignmentById_(state.assignmentsRows, assignmentId);
  if (!found) return { error: 'Assignment not found' };
  const assignment = found.assignment;

  if (String(assignment.editor_token || '') !== String(token || '')) {
    return { error: 'Unauthorized: editor token required' };
  }

  if (String(assignment.assigned_session_id || '') !== sessionId) {
    return { error: 'Assignment is not reserved for this session' };
  }

  const beforeStatus = String(assignment.status || '').toUpperCase();
  if (!ACTIVE_ASSIGNMENT_STATUSES[beforeStatus]) {
    return { error: 'Assignment is not in an active state' };
  }

  releaseSingleAssignment_(state, assignment, beforeStatus, now, `skipped:${skipReason}`);

  appendHistoryRow_(state.historyRowsToAppend, {
    event_type: 'assignment_skipped_invalid',
    assignment_id: assignment.assignment_id,
    send_id: assignment.send_id,
    from_status: beforeStatus,
    to_status: 'UNASSIGNED',
    agent_email: email,
    session_id: sessionId,
    detail: skipReason
  });

  session.last_heartbeat_at = now;
  if (isSessionActive_(session)) {
    assignFromPool_(state, email, sessionId, now, baseUrl, 1);
  }

  setAssignmentRow_(state, found.index, assignment);
  setSessionRow_(state, sessionIndex, session);
  persistAssignmentState_(state);

  const assignments = getActiveAssignmentsForSession_(state.assignmentsRows, email, sessionId)
    .slice(0, TARGET_QUEUE_SIZE)
    .map(function(a) {
      return {
        assignment_id: a.assignment_id,
        send_id: a.send_id,
        status: a.status,
        edit_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.editor_token, 'edit'),
        view_url: buildAssignmentUrl_(baseUrl, a.assignment_id, a.viewer_token, 'view')
      };
    });

  return {
    assignments: assignments,
    session: sessionToPayload_(session),
    skipped_assignment_id: String(assignmentId || '')
  };
}

function heartbeatSession_(email, sessionId, clientTs) {
  const state = loadAssignmentState_();
  const now = nowIso_();

  cleanupStaleSessions_(state, now);
  let sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  let session;
  if (sessionIndex < 0) {
    session = createSessionObject_(email, sessionId, '', now);
    appendSessionRow_(state, session);
    appendHistoryRow_(state.historyRowsToAppend, {
      event_type: 'session_created',
      assignment_id: '',
      send_id: '',
      from_status: '',
      to_status: '',
      agent_email: email,
      session_id: sessionId,
      detail: 'created_from_heartbeat'
    });
    sessionIndex = state.sessionsRows.length - 1;
  } else {
    session = rowToSessionObject_(state.sessionsRows[sessionIndex]);
    if (
      normalizeEmail_(session.agent_email) &&
      normalizeEmail_(session.agent_email) !== normalizeEmail_(email)
    ) {
      return { error: 'Session email mismatch' };
    }
  }

  session.agent_email = email;
  if (isSessionLive_(session)) {
    session.state = 'ACTIVE';
    session.ended_at = '';
    session.end_reason = '';
    session.last_heartbeat_at = now;
  }

  setSessionRow_(state, sessionIndex, session);
  persistAssignmentState_(state);

  return {
    ok: true,
    session: sessionToPayload_(session),
    client_ts: clientTs || ''
  };
}

function releaseSession_(email, sessionId, reason) {
  const state = loadAssignmentState_();
  const now = nowIso_();

  cleanupStaleSessions_(state, now);

  const sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  if (sessionIndex < 0) {
    persistAssignmentState_(state);
    return { ok: true, released_count: 0 };
  }

  const session = rowToSessionObject_(state.sessionsRows[sessionIndex]);
  if (normalizeEmail_(session.agent_email) !== normalizeEmail_(email)) {
    return { error: 'Session email mismatch' };
  }

  if (isSessionLive_(session)) {
    session.state = 'COOLDOWN';
    session.ended_at = '';
    session.end_reason = reason || 'cooldown_started';
    session.last_heartbeat_at = now;
  }

  appendHistoryRow_(state.historyRowsToAppend, {
    event_type: 'session_cooldown_started',
    assignment_id: '',
    send_id: '',
    from_status: '',
    to_status: '',
    agent_email: session.agent_email,
    session_id: sessionId,
    detail: `reason=${reason || 'manual'}`
  });

  setSessionRow_(state, sessionIndex, session);
  persistAssignmentState_(state);

  return {
    ok: true,
    released_count: 0,
    session: sessionToPayload_(session)
  };
}

function createSnapshotForSession_(options) {
  const assignmentId = String((options && options.assignment_id) || '').trim();
  const token = String((options && options.token) || '').trim();
  const sessionId = String((options && options.session_id) || '').trim();
  const agentEmail = normalizeEmail_((options && options.agent_email) || '');
  const appBase = String((options && options.app_base) || '').trim();
  const snapshotPayloadInput = options ? options.snapshot_payload : null;

  if (!assignmentId || !token || !sessionId) {
    return { error: 'Missing assignment_id, token, or session_id' };
  }
  if (!agentEmail) {
    return { error: 'Missing agent_email' };
  }
  if (!snapshotPayloadInput || typeof snapshotPayloadInput !== 'object') {
    return { error: 'Missing snapshot_payload' };
  }

  const state = loadAssignmentState_();
  const now = nowIso_();

  cleanupStaleSessions_(state, now);

  const sessionIndex = findSessionIndex_(state.sessionsRows, sessionId);
  if (sessionIndex < 0) return { error: 'Session not found' };
  const session = rowToSessionObject_(state.sessionsRows[sessionIndex]);
  if (normalizeEmail_(session.agent_email) !== agentEmail) {
    return { error: 'Session email mismatch' };
  }
  if (!isSessionActive_(session)) {
    return { error: 'Session is not active' };
  }

  const found = findAssignmentById_(state.assignmentsRows, assignmentId);
  if (!found) return { error: 'Assignment not found' };
  const assignment = found.assignment;
  if (String(assignment.editor_token || '') !== token) {
    return { error: 'Unauthorized: editor token required' };
  }

  const status = String(assignment.status || '').toUpperCase();
  if (status !== 'DONE' && String(assignment.assigned_session_id || '') !== sessionId) {
    return { error: 'Assignment is not reserved for this session' };
  }

  const payload = normalizeSnapshotPayload_(snapshotPayloadInput, assignment, now);
  if (!payload) {
    return { error: 'Snapshot payload is invalid' };
  }
  const payloadJson = JSON.stringify(payload);
  if (payloadJson.length > MAX_SNAPSHOT_PAYLOAD_CHARS) {
    return { error: 'Snapshot payload is too large' };
  }

  const snapshotId = Utilities.getUuid();
  const snapshotToken = generateOpaqueToken_();
  const expiresAt = addHoursToIso_(now, SNAPSHOT_TTL_HOURS);
  const snapshotsSheet = getSnapshotsSheet_();
  appendSnapshotRow_(snapshotsSheet, {
    snapshot_id: snapshotId,
    snapshot_token: snapshotToken,
    assignment_id: assignment.assignment_id,
    send_id: assignment.send_id,
    created_by_email: agentEmail,
    created_at: now,
    expires_at: expiresAt,
    status: 'ACTIVE',
    payload_json: payloadJson
  });

  appendHistoryRow_(state.historyRowsToAppend, {
    event_type: 'snapshot_created',
    assignment_id: assignment.assignment_id,
    send_id: assignment.send_id,
    from_status: '',
    to_status: '',
    agent_email: agentEmail,
    session_id: sessionId,
    detail: snapshotId
  });
  persistAssignmentState_(state);

  const baseUrl = resolveAppBaseUrl_(appBase || session.app_base || '');
  return {
    ok: true,
    snapshot_id: snapshotId,
    snapshot_token: snapshotToken,
    expires_at: expiresAt,
    share_url: buildSnapshotUrl_(baseUrl, snapshotId, snapshotToken)
  };
}

function createScenarioSnapshot_(options) {
  const appBase = String((options && options.app_base) || '').trim();
  const agentEmail = normalizeEmail_((options && options.agent_email) || '');
  const snapshotPayloadInput = options ? options.snapshot_payload : null;
  if (!snapshotPayloadInput || typeof snapshotPayloadInput !== 'object') {
    return { error: 'Missing snapshot_payload' };
  }

  const now = nowIso_();
  const payload = normalizeScenarioSnapshotPayload_(snapshotPayloadInput, now);
  if (!payload) {
    return { error: 'Snapshot payload is invalid' };
  }

  const payloadJson = JSON.stringify(payload);
  if (payloadJson.length > MAX_SNAPSHOT_PAYLOAD_CHARS) {
    return { error: 'Snapshot payload is too large' };
  }

  const snapshotId = Utilities.getUuid();
  const snapshotToken = generateOpaqueToken_();
  const expiresAt = addHoursToIso_(now, SNAPSHOT_TTL_HOURS);
  const snapshotsSheet = getSnapshotsSheet_();
  appendSnapshotRow_(snapshotsSheet, {
    snapshot_id: snapshotId,
    snapshot_token: snapshotToken,
    assignment_id: String(payload.assignment_id || ''),
    send_id: String(payload.send_id || ''),
    created_by_email: agentEmail || '',
    created_at: now,
    expires_at: expiresAt,
    status: 'ACTIVE',
    payload_json: payloadJson
  });

  const baseUrl = resolveAppBaseUrl_(appBase);
  return {
    ok: true,
    snapshot_id: snapshotId,
    snapshot_token: snapshotToken,
    expires_at: expiresAt,
    share_url: buildSnapshotUrl_(baseUrl, snapshotId, snapshotToken)
  };
}

function getSnapshotByToken_(snapshotId, snapshotToken) {
  const id = String(snapshotId || '').trim();
  const token = String(snapshotToken || '').trim();
  if (!id || !token) return { error: 'Missing snapshot_id or snapshot_token' };

  const sheet = getSnapshotsSheet_();
  const rows = getSheetDataRows_(sheet, SNAPSHOT_HEADERS.length);
  const found = findSnapshotRowById_(rows, id);
  if (!found) return { error: 'This snapshot link is invalid or expired.' };

  const snapshot = rowToSnapshotObject_(found.row);
  if (String(snapshot.snapshot_token || '') !== token) {
    return { error: 'This snapshot link is invalid or expired.' };
  }

  const nowMs = Date.now();
  const status = String(snapshot.status || 'ACTIVE').toUpperCase();
  if (status !== 'ACTIVE' || isSnapshotExpired_(snapshot, nowMs)) {
    if (status !== 'EXPIRED') {
      updateSnapshotStatus_(sheet, found.index, 'EXPIRED');
    }
    return { error: 'This snapshot link is invalid or expired.' };
  }

  const payload = parseJsonSafe_(snapshot.payload_json);
  if (!payload || typeof payload !== 'object') {
    return { error: 'Snapshot payload is invalid' };
  }

  return {
    ok: true,
    snapshot: {
      snapshot_id: snapshot.snapshot_id,
      created_at: snapshot.created_at,
      expires_at: snapshot.expires_at,
      payload: payload
    }
  };
}

function loadAssignmentState_() {
  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const assignmentsSheet = getOrCreateSheet_(spreadsheet, ASSIGNMENTS_SHEET, ASSIGNMENTS_HEADERS);
  const poolSheet = getOrCreateSheet_(spreadsheet, POOL_SHEET, POOL_HEADERS);
  const sessionsSheet = getOrCreateSheet_(spreadsheet, QA_SESSIONS_SHEET, SESSION_HEADERS);
  const historySheet = getOrCreateSheet_(spreadsheet, QA_ASSIGNMENT_HISTORY_SHEET, HISTORY_HEADERS);
  const assignmentsRows = getSheetDataRows_(assignmentsSheet, ASSIGNMENTS_HEADERS.length);
  const poolRows = getSheetDataRows_(poolSheet, POOL_HEADERS.length);
  const sessionsRows = getSheetDataRows_(sessionsSheet, SESSION_HEADERS.length);

  return {
    assignmentsSheet: assignmentsSheet,
    poolSheet: poolSheet,
    sessionsSheet: sessionsSheet,
    historySheet: historySheet,
    assignmentsRows: assignmentsRows,
    poolRows: poolRows,
    sessionsRows: sessionsRows,
    historyRowsToAppend: [],
    assignmentsDirty: {},
    poolDirty: {},
    sessionsDirty: {}
  };
}

function persistAssignmentState_(state) {
  writeDirtySheetDataRows_(state.assignmentsSheet, state.assignmentsRows, ASSIGNMENTS_HEADERS.length, state.assignmentsDirty);
  writeDirtySheetDataRows_(state.poolSheet, state.poolRows, POOL_HEADERS.length, state.poolDirty);
  writeDirtySheetDataRows_(state.sessionsSheet, state.sessionsRows, SESSION_HEADERS.length, state.sessionsDirty);

  if (state.historyRowsToAppend && state.historyRowsToAppend.length > 0) {
    const startRow = state.historySheet.getLastRow() + 1;
    state.historySheet
      .getRange(startRow, 1, state.historyRowsToAppend.length, HISTORY_HEADERS.length)
      .setValues(state.historyRowsToAppend);
  }

  state.historyRowsToAppend = [];
  state.assignmentsDirty = {};
  state.poolDirty = {};
  state.sessionsDirty = {};
}

function writeDirtySheetDataRows_(sheet, rows, colCount, dirtyMap) {
  const nextRows = Array.isArray(rows) ? rows : [];
  const dirtyIndexes = getDirtyIndexes_(dirtyMap, nextRows.length);
  if (!dirtyIndexes.length) return;

  const groups = groupContiguousIndexes_(dirtyIndexes);
  for (let i = 0; i < groups.length; i++) {
    const startIndex = groups[i].start;
    const length = groups[i].length;
    const values = nextRows.slice(startIndex, startIndex + length);
    if (!values.length) continue;
    sheet.getRange(startIndex + 2, 1, values.length, colCount).setValues(values);
  }
}

function getDirtyIndexes_(dirtyMap, rowCount) {
  if (!dirtyMap) return [];
  const indexes = [];
  for (const key in dirtyMap) {
    if (!Object.prototype.hasOwnProperty.call(dirtyMap, key)) continue;
    const idx = Number(key);
    if (!Number.isFinite(idx)) continue;
    if (idx < 0 || idx >= rowCount) continue;
    indexes.push(idx);
  }
  indexes.sort(function(a, b) { return a - b; });
  return indexes;
}

function groupContiguousIndexes_(indexes) {
  if (!indexes || !indexes.length) return [];
  const groups = [];
  let start = indexes[0];
  let prev = indexes[0];

  for (let i = 1; i < indexes.length; i++) {
    const current = indexes[i];
    if (current === prev + 1) {
      prev = current;
      continue;
    }
    groups.push({ start: start, length: (prev - start + 1) });
    start = current;
    prev = current;
  }
  groups.push({ start: start, length: (prev - start + 1) });
  return groups;
}

function markDirtyIndex_(dirtyMap, index) {
  const idx = Number(index);
  if (!dirtyMap || !Number.isFinite(idx) || idx < 0) return;
  dirtyMap[String(idx)] = true;
}

function setAssignmentRow_(state, index, assignment) {
  state.assignmentsRows[index] = assignmentToRow_(assignment);
  markDirtyIndex_(state.assignmentsDirty, index);
}

function appendAssignmentRow_(state, assignment) {
  const index = state.assignmentsRows.push(assignmentToRow_(assignment)) - 1;
  markDirtyIndex_(state.assignmentsDirty, index);
  return index;
}

function setSessionRow_(state, index, session) {
  state.sessionsRows[index] = sessionToRow_(session);
  markDirtyIndex_(state.sessionsDirty, index);
}

function appendSessionRow_(state, session) {
  const index = state.sessionsRows.push(sessionToRow_(session)) - 1;
  markDirtyIndex_(state.sessionsDirty, index);
  return index;
}

function setPoolStatusByIndex_(state, index, status) {
  const idx = Number(index);
  if (!Number.isFinite(idx) || idx < 0 || idx >= state.poolRows.length) return;
  state.poolRows[idx][1] = String(status || '');
  markDirtyIndex_(state.poolDirty, idx);
}

function cleanupStaleSessions_(state, nowIso) {
  const nowMs = parseIsoMs_(nowIso);
  const timeoutMs = SESSION_TIMEOUT_MINUTES * 60 * 1000;
  let timedOutCount = 0;
  let releasedCount = 0;

  for (let i = 0; i < state.sessionsRows.length; i++) {
    const session = rowToSessionObject_(state.sessionsRows[i]);
    const stateName = String(session.state || '').toUpperCase();
    if (stateName !== 'ACTIVE' && stateName !== 'COOLDOWN') continue;

    const anchor = session.last_heartbeat_at || session.started_at;
    const anchorMs = parseIsoMs_(anchor);
    if (!anchorMs) continue;
    if ((nowMs - anchorMs) < timeoutMs) continue;

    const timeoutReason = stateName === 'COOLDOWN' ? 'logout_cooldown_timeout' : 'heartbeat_timeout';
    const released = releaseAssignmentsForSession_(state, session.session_id, timeoutReason, nowIso);
    releasedCount += Math.max(0, Number(released) || 0);
    session.state = 'TIMED_OUT';
    session.ended_at = nowIso;
    session.end_reason = timeoutReason;
    session.last_heartbeat_at = nowIso;
    setSessionRow_(state, i, session);
    timedOutCount += 1;

    appendHistoryRow_(state.historyRowsToAppend, {
      event_type: 'session_timed_out',
      assignment_id: '',
      send_id: '',
      from_status: '',
      to_status: '',
      agent_email: session.agent_email,
      session_id: session.session_id,
      detail: `released=${released};from=${stateName}`
    });
  }

  return {
    timed_out_count: timedOutCount,
    released_count: releasedCount
  };
}

function resolveQueueSession_(state, email, requestedSessionId, appBase, nowIso) {
  const normalizedEmail = normalizeEmail_(email);
  const requested = String(requestedSessionId || '').trim();
  const requestedIndex = requested ? findSessionIndex_(state.sessionsRows, requested) : -1;

  if (requested && requestedIndex >= 0) {
    const existingRequested = rowToSessionObject_(state.sessionsRows[requestedIndex]);
    if (
      normalizeEmail_(existingRequested.agent_email) === normalizedEmail &&
      isSessionReclaimable_(existingRequested)
    ) {
      existingRequested.state = 'ACTIVE';
      existingRequested.agent_email = normalizedEmail;
      existingRequested.last_heartbeat_at = nowIso;
      existingRequested.ended_at = '';
      existingRequested.end_reason = '';
      if (appBase) existingRequested.app_base = appBase;
      setSessionRow_(state, requestedIndex, existingRequested);
      appendHistoryRow_(state.historyRowsToAppend, {
        event_type: 'session_reclaimed',
        assignment_id: '',
        send_id: '',
        from_status: '',
        to_status: '',
        agent_email: normalizedEmail,
        session_id: existingRequested.session_id,
        detail: 'source=requested_session_id'
      });
      return existingRequested;
    }
  }

  let bestIndex = -1;
  let bestHeartbeatMs = -1;
  for (let i = 0; i < state.sessionsRows.length; i++) {
    const candidate = rowToSessionObject_(state.sessionsRows[i]);
    if (normalizeEmail_(candidate.agent_email) !== normalizedEmail) continue;
    if (!isSessionReclaimable_(candidate)) continue;
    const heartbeatMs = parseIsoMs_(candidate.last_heartbeat_at || candidate.started_at);
    if (heartbeatMs >= bestHeartbeatMs) {
      bestHeartbeatMs = heartbeatMs;
      bestIndex = i;
    }
  }

  if (bestIndex >= 0) {
    const reclaimed = rowToSessionObject_(state.sessionsRows[bestIndex]);
    reclaimed.state = 'ACTIVE';
    reclaimed.agent_email = normalizedEmail;
    reclaimed.last_heartbeat_at = nowIso;
    reclaimed.ended_at = '';
    reclaimed.end_reason = '';
    if (appBase) reclaimed.app_base = appBase;
    setSessionRow_(state, bestIndex, reclaimed);
    appendHistoryRow_(state.historyRowsToAppend, {
      event_type: 'session_reclaimed',
      assignment_id: '',
      send_id: '',
      from_status: '',
      to_status: '',
      agent_email: normalizedEmail,
      session_id: reclaimed.session_id,
      detail: 'source=email_lookup'
    });
    return reclaimed;
  }

  const newSessionId = (requested && requestedIndex < 0) ? requested : createServerSessionId_();
  const created = createSessionObject_(normalizedEmail, newSessionId, appBase, nowIso);
  appendSessionRow_(state, created);
  appendHistoryRow_(state.historyRowsToAppend, {
    event_type: 'session_created',
    assignment_id: '',
    send_id: '',
    from_status: '',
    to_status: '',
    agent_email: normalizedEmail,
    session_id: newSessionId,
    detail: 'source=new_queue_session'
  });
  return created;
}

function createServerSessionId_() {
  return Utilities.getUuid();
}

function isSessionReclaimable_(session) {
  return isSessionLive_(session);
}

function createSessionObject_(email, sessionId, appBase, nowIso) {
  return {
    session_id: sessionId,
    agent_email: email,
    state: 'ACTIVE',
    submitted_count: 0,
    cap: SESSION_CAP,
    started_at: nowIso,
    last_heartbeat_at: nowIso,
    ended_at: '',
    end_reason: '',
    app_base: appBase || ''
  };
}

function releaseOtherActiveSessionsForEmail_(state, email, keepSessionId, nowIso) {
  for (let i = 0; i < state.sessionsRows.length; i++) {
    const session = rowToSessionObject_(state.sessionsRows[i]);
    if (session.session_id === keepSessionId) continue;
    if (normalizeEmail_(session.agent_email) !== normalizeEmail_(email)) continue;
    if (!isSessionLive_(session)) continue;

    const released = releaseAssignmentsForSession_(state, session.session_id, 'superseded', nowIso);
    session.state = 'TIMED_OUT';
    session.ended_at = nowIso;
    session.end_reason = 'superseded_by_new_session';
    session.last_heartbeat_at = nowIso;
    setSessionRow_(state, i, session);

    appendHistoryRow_(state.historyRowsToAppend, {
      event_type: 'session_superseded',
      assignment_id: '',
      send_id: '',
      from_status: '',
      to_status: '',
      agent_email: email,
      session_id: session.session_id,
      detail: `released=${released}`
    });
  }
}

function releaseActiveAssignmentsForEmailOutsideSession_(state, email, keepSessionId, nowIso, reason) {
  for (let i = 0; i < state.assignmentsRows.length; i++) {
    const assignment = rowToAssignmentObject_(state.assignmentsRows[i]);
    const status = String(assignment.status || '').toUpperCase();
    if (!ACTIVE_ASSIGNMENT_STATUSES[status]) continue;
    if (normalizeEmail_(assignment.assignee_email) !== normalizeEmail_(email)) continue;
    if (String(assignment.assigned_session_id || '') === keepSessionId) continue;

    const released = releaseSingleAssignment_(state, assignment, status, nowIso, reason || 'superseded');
    if (released) {
      setAssignmentRow_(state, i, assignment);
    }
  }
}

function topUpQueueForSession_(state, email, sessionId, nowIso, appBase) {
  const active = getActiveAssignmentsForSession_(state.assignmentsRows, email, sessionId);
  const needed = Math.max(0, TARGET_QUEUE_SIZE - active.length);
  if (needed <= 0) return;
  assignFromPool_(state, email, sessionId, nowIso, appBase, needed);
}

function assignFromPool_(state, email, sessionId, nowIso, appBase, count) {
  let needed = Math.max(0, toInt_(count, 0));
  if (!needed) return;
  const assignedInThisPass = {};

  for (let i = 0; i < state.poolRows.length && needed > 0; i++) {
    const poolSendId = String(state.poolRows[i][0] || '').trim();
    const poolStatus = String(state.poolRows[i][1] || '').toUpperCase();
    if (!poolSendId) continue;
    if (poolStatus && poolStatus !== 'AVAILABLE') continue;
    if (assignedInThisPass[poolSendId]) continue;

    // Hard guard: never assign the same send_id while an active assignment exists.
    if (hasAssignmentForSendIdWithStatuses_(state.assignmentsRows, poolSendId, ACTIVE_ASSIGNMENT_STATUSES)) {
      continue;
    }

    // If this send_id is already done, mark duplicate pool rows done too to avoid resurfacing it.
    if (hasAssignmentForSendIdWithStatuses_(state.assignmentsRows, poolSendId, { DONE: true })) {
      setPoolStatusByIndex_(state, i, 'DONE');
      continue;
    }

    const assignment = {
      send_id: poolSendId,
      assignee_email: email,
      status: 'ASSIGNED',
      assignment_id: Utilities.getUuid(),
      created_at: nowIso,
      updated_at: nowIso,
      done_at: '',
      editor_token: generateOpaqueToken_(),
      viewer_token: generateOpaqueToken_(),
      form_state_json: '',
      internal_note: '',
      assigned_session_id: sessionId,
      assigned_at: nowIso
    };

    appendAssignmentRow_(state, assignment);
    setPoolStatusByIndex_(state, i, 'ASSIGNED');

    appendHistoryRow_(state.historyRowsToAppend, {
      event_type: 'assignment_assigned',
      assignment_id: assignment.assignment_id,
      send_id: assignment.send_id,
      from_status: 'UNASSIGNED',
      to_status: 'ASSIGNED',
      agent_email: email,
      session_id: sessionId,
      detail: appBase ? `app_base=${appBase}` : ''
    });

    assignedInThisPass[poolSendId] = true;
    needed--;
  }
}

function releaseAssignmentsForSession_(state, sessionId, reason, nowIso) {
  let releasedCount = 0;
  for (let i = 0; i < state.assignmentsRows.length; i++) {
    const assignment = rowToAssignmentObject_(state.assignmentsRows[i]);
    const status = String(assignment.status || '').toUpperCase();
    if (!ACTIVE_ASSIGNMENT_STATUSES[status]) continue;
    if (String(assignment.assigned_session_id || '') !== String(sessionId || '')) continue;

    const released = releaseSingleAssignment_(state, assignment, status, nowIso, reason || 'released');
    if (!released) continue;
    setAssignmentRow_(state, i, assignment);
    releasedCount++;
  }
  return releasedCount;
}

function releaseSingleAssignment_(state, assignment, fromStatus, nowIso, reason) {
  if (!assignment) return false;
  const sendId = String(assignment.send_id || '');
  const poolIndex = findPoolIndexBySendId_(state.poolRows, sendId);
  if (poolIndex >= 0) {
    const poolStatus = String(state.poolRows[poolIndex][1] || '').toUpperCase();
    if (poolStatus !== 'DONE') {
      setPoolStatusByIndex_(state, poolIndex, 'AVAILABLE');
    }
  }

  const agentEmail = normalizeEmail_(assignment.assignee_email);
  const sessionId = String(assignment.assigned_session_id || '');

  assignment.status = 'RELEASED';
  assignment.assignee_email = '';
  assignment.updated_at = nowIso;
  assignment.form_state_json = '';
  assignment.internal_note = '';
  assignment.assigned_session_id = '';
  assignment.assigned_at = '';

  appendHistoryRow_(state.historyRowsToAppend, {
    event_type: 'assignment_released',
    assignment_id: assignment.assignment_id,
    send_id: assignment.send_id,
    from_status: fromStatus,
    to_status: 'UNASSIGNED',
    agent_email: agentEmail,
    session_id: sessionId,
    detail: reason || ''
  });
  return true;
}

function addSendIdsToPool_(sendIds) {
  const ids = Array.isArray(sendIds)
    ? sendIds.map(function(v) { return String(v || '').trim(); }).filter(function(v) { return !!v; })
    : [];
  if (!ids.length) {
    return { error: 'Missing send_ids' };
  }

  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const poolSheet = getOrCreateSheet_(spreadsheet, POOL_SHEET, POOL_HEADERS);
  const poolValues = getSheetDataRows_(poolSheet, POOL_HEADERS.length);

  const uniqueIds = [];
  const seen = {};
  for (let i = 0; i < ids.length; i++) {
    if (seen[ids[i]]) continue;
    seen[ids[i]] = true;
    uniqueIds.push(ids[i]);
  }

  const indexBySendId = {};
  for (let i = 0; i < poolValues.length; i++) {
    const existing = String(poolValues[i][0] || '').trim();
    if (!existing) continue;
    if (indexBySendId[existing] == null) {
      indexBySendId[existing] = i;
    }
  }

  let added = 0;
  let reactivated = 0;
  const newRows = [];

  for (let i = 0; i < uniqueIds.length; i++) {
    const sendId = uniqueIds[i];
    const idx = indexBySendId[sendId];
    if (idx == null) {
      newRows.push([sendId, 'AVAILABLE']);
      added++;
      continue;
    }

    const currentStatus = String(poolValues[idx][1] || '').trim().toUpperCase();
    if (currentStatus !== 'ASSIGNED') {
      if (currentStatus !== 'AVAILABLE') {
        reactivated++;
      }
      poolValues[idx][1] = 'AVAILABLE';
    }
  }

  if (poolValues.length > 0) {
    const statusColumn = poolValues.map(function(row) { return [row[1]]; });
    poolSheet.getRange(2, 2, statusColumn.length, 1).setValues(statusColumn);
  }

  if (newRows.length > 0) {
    const startRow = poolSheet.getLastRow() + 1;
    poolSheet.getRange(startRow, 1, newRows.length, POOL_HEADERS.length).setValues(newRows);
  }

  return { ok: true, added: added, reactivated: reactivated, total: uniqueIds.length };
}

function resetAssignmentsFromAudit_(sendIds) {
  const ids = Array.isArray(sendIds)
    ? sendIds.map(function(v) { return String(v || '').trim(); }).filter(function(v) { return !!v; })
    : [];
  if (!ids.length) {
    return { error: 'Missing send_ids' };
  }

  const uniqueIds = [];
  const seen = {};
  for (let i = 0; i < ids.length; i++) {
    if (seen[ids[i]]) continue;
    seen[ids[i]] = true;
    uniqueIds.push(ids[i]);
  }

  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const assignmentsSheet = getOrCreateSheet_(spreadsheet, ASSIGNMENTS_SHEET, ASSIGNMENTS_HEADERS);
  const poolSheet = getOrCreateSheet_(spreadsheet, POOL_SHEET, POOL_HEADERS);

  const assignmentLastRow = assignmentsSheet.getLastRow();
  if (assignmentLastRow > 1) {
    assignmentsSheet.getRange(2, 1, assignmentLastRow - 1, ASSIGNMENTS_HEADERS.length).clearContent();
  }

  const poolLastRow = poolSheet.getLastRow();
  if (poolLastRow > 1) {
    poolSheet.getRange(2, 1, poolLastRow - 1, POOL_HEADERS.length).clearContent();
  }

  const now = nowIso_();
  const assignmentRows = uniqueIds.map(function(sendId) {
    return [
      sendId,
      '',
      'AVAILABLE',
      Utilities.getUuid(),
      now,
      now,
      '',
      Utilities.getUuid(),
      Utilities.getUuid(),
      '',
      '',
      '',
      ''
    ];
  });

  const poolRows = uniqueIds.map(function(sendId) {
    return [sendId, 'AVAILABLE'];
  });

  assignmentsSheet.getRange(2, 1, assignmentRows.length, ASSIGNMENTS_HEADERS.length).setValues(assignmentRows);
  poolSheet.getRange(2, 1, poolRows.length, POOL_HEADERS.length).setValues(poolRows);

  return {
    ok: true,
    total: uniqueIds.length,
    assignments_reset: true,
    pool_reset: true
  };
}

function readUploadedJsonList_(sheetName) {
  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sheet = getOrCreateSheet_(spreadsheet, sheetName, UPLOADED_JSON_HEADERS);
  const rows = getSheetDataRows_(sheet, UPLOADED_JSON_HEADERS.length);
  const items = [];

  for (let i = 0; i < rows.length; i++) {
    const raw = String(rows[i][1] || '').trim();
    if (!raw) continue;
    try {
      items.push(JSON.parse(raw));
    } catch (error) {
      console.warn('Skipping invalid JSON row in sheet %s at index %s', sheetName, i + 2);
    }
  }

  return items;
}

function writeUploadedJsonList_(sheetName, list) {
  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sheet = getOrCreateSheet_(spreadsheet, sheetName, UPLOADED_JSON_HEADERS);
  const lastRow = sheet.getLastRow();

  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, UPLOADED_JSON_HEADERS.length).clearContent();
  }

  const items = Array.isArray(list) ? list : [];
  if (!items.length) return;

  const now = nowIso_();
  const rows = items.map(function(item) {
    const fallbackId = Utilities.getUuid();
    const recordId = item && item.id != null ? String(item.id) : fallbackId;
    return [
      recordId,
      stringifyMaybe_(item),
      now
    ];
  });

  sheet.getRange(2, 1, rows.length, UPLOADED_JSON_HEADERS.length).setValues(rows);
}

function appendUploadedJsonList_(sheetName, list, options) {
  const items = Array.isArray(list) ? list : [];
  if (!items.length) return { added: 0, total: 0 };

  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sheet = getOrCreateSheet_(spreadsheet, sheetName, UPLOADED_JSON_HEADERS);
  const reset = !!(options && options.reset);

  if (reset) {
    const existingLastRow = sheet.getLastRow();
    if (existingLastRow > 1) {
      sheet.getRange(2, 1, existingLastRow - 1, UPLOADED_JSON_HEADERS.length).clearContent();
    }
  }

  const now = nowIso_();
  const rows = items.map(function(item) {
    const fallbackId = Utilities.getUuid();
    const recordId = item && item.id != null ? String(item.id) : fallbackId;
    return [recordId, stringifyMaybe_(item), now];
  });

  const startRow = sheet.getLastRow() + 1;
  sheet.getRange(startRow, 1, rows.length, UPLOADED_JSON_HEADERS.length).setValues(rows);
  return { added: rows.length, total: rows.length };
}

function handleLegacyPost_(data) {
  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  const sheet = spreadsheet.getActiveSheet();

  if (data && data.eventType && (data.eventType === 'sessionLogin' || data.eventType === 'sessionLogout')) {
    let logsSheet = spreadsheet.getSheetByName(SESSION_LOGS_SHEET);
    if (!logsSheet) {
      logsSheet = spreadsheet.insertSheet(SESSION_LOGS_SHEET);
      logsSheet.getRange(1, 1, 1, 9).setValues([[
        'Date (EST)', 'Agent Name', 'Agent Email', 'Event', 'Global Session ID', 'Login Method', 'Login At', 'Logout At', 'Duration (mins)'
      ]]);
    }

    const sessionId = data.sessionId || '';
    if (data.eventType === 'sessionLogin') {
      safeAppendRow(logsSheet, [
        data.loginAt || '',
        data.agentUsername || '',
        data.agentEmail || '',
        'login',
        sessionId,
        data.loginMethod || '',
        data.loginAt || '',
        '',
        ''
      ]);
      return jsonResponse_({ status: 'success' });
    }

    if (data.eventType === 'sessionLogout') {
      const lastRow = logsSheet.getLastRow();
      for (let i = lastRow; i >= 2; i--) {
        const existingSessionId = logsSheet.getRange(i, 5).getValue();
        const eventCell = logsSheet.getRange(i, 4).getValue();
        if (existingSessionId && existingSessionId.toString() === sessionId && eventCell === 'login') {
          logsSheet.getRange(i, 8).setValue(data.logoutAt || '');
          logsSheet.getRange(i, 9).setValue('');
          return jsonResponse_({ status: 'success' });
        }
      }

      safeAppendRow(logsSheet, [
        data.logoutAt || '',
        data.agentUsername || '',
        data.agentEmail || '',
        'logout',
        sessionId,
        data.loginMethod || '',
        '',
        data.logoutAt || '',
        ''
      ]);
      return jsonResponse_({ status: 'success' });
    }

    return jsonResponse_({ status: 'success' });
  }

  if (data && data.eventType === 'evaluationFormSubmission') {
    let dataSheet = spreadsheet.getSheetByName(DATA_SHEET);
    if (!dataSheet) {
      dataSheet = spreadsheet.insertSheet(DATA_SHEET);
      dataSheet.getRange(1, 1, 1, 14).setValues([[
        'Timestamp', 'Email Address', 'Message ID', 'Audit Time',
        'Issue Identification', 'Proper Resolution', 'Product Sales', 'Accuracy',
        'Workflow', 'Clarity', 'Tone',
        'Efficient Troubleshooting Miss', 'Zero Tolerance', 'Notes'
      ]]);
    }

    safeAppendRow(dataSheet, [
      data.timestamp || '',
      data.emailAddress || '',
      data.messageId || '',
      data.auditTime || '',
      data.issueIdentification || '',
      data.properResolution || '',
      data.productSales || '',
      data.accuracy || '',
      data.workflow || '',
      data.clarity || '',
      data.tone || '',
      data.efficientTroubleshootingMiss || '',
      data.zeroTolerance || '',
      data.notes || ''
    ]);

    return jsonResponse_({ status: 'success' });
  }

  const sessionKey = data.sessionId;
  let targetRow = null;
  const lastRow = sheet.getLastRow();

  for (let i = 2; i <= lastRow; i++) {
    const existingSessionId = sheet.getRange(i, 4).getValue();
    if (existingSessionId && existingSessionId.toString() === sessionKey) {
      targetRow = i;
      break;
    }
  }

  if (targetRow === null) {
    targetRow = lastRow + 1;
    sheet.getRange(targetRow, 1).setValue(String(data.timestampEST || ''));
    sheet.getRange(targetRow, 2).setValue(data.agentUsername || '');
    sheet.getRange(targetRow, 3).setValue(data.scenario || '');
    sheet.getRange(targetRow, 4).setValue(data.sessionId || '');
  }

  let messageColumn = 5;
  const possibleColumns = [5, 8];
  for (let i = 0; i < possibleColumns.length; i++) {
    const col = possibleColumns[i];
    const customerMsgValue = sheet.getRange(targetRow, col).getValue();
    if (!customerMsgValue) {
      messageColumn = col;
      break;
    }
  }

  if (messageColumn === 5) {
    const firstMsgValue = sheet.getRange(targetRow, 5).getValue();
    if (firstMsgValue) messageColumn = 8;
  }

  sheet.getRange(targetRow, messageColumn).setValue(data.customerMessage || '');
  sheet.getRange(targetRow, messageColumn + 1).setValue(data.agentResponse || '');
  sheet.getRange(targetRow, messageColumn + 2).setValue(data.sendTime || '');

  const messageNumber = messageColumn === 5 ? 1 : 2;
  return jsonResponse_({
    status: 'success',
    message: 'Data saved successfully',
    row: targetRow,
    messageNumber: messageNumber,
    sessionId: sessionKey
  });
}

function getActiveAssignmentsForSession_(rows, email, sessionId) {
  const list = [];
  for (let i = 0; i < rows.length; i++) {
    const assignment = rowToAssignmentObject_(rows[i]);
    const assignee = normalizeEmail_(assignment.assignee_email);
    const status = String(assignment.status || '').toUpperCase();
    const ownerSession = String(assignment.assigned_session_id || '');
    if (assignee === email && ownerSession === sessionId && ACTIVE_ASSIGNMENT_STATUSES[status]) {
      list.push(assignment);
    }
  }
  list.sort(function(a, b) {
    return String(a.created_at || '').localeCompare(String(b.created_at || ''));
  });
  return list;
}

function hasAssignmentForSendIdWithStatuses_(rows, sendId, statusLookup) {
  const targetSendId = String(sendId || '').trim();
  if (!targetSendId) return false;
  const statuses = statusLookup && typeof statusLookup === 'object' ? statusLookup : {};
  for (let i = 0; i < rows.length; i++) {
    const assignment = rowToAssignmentObject_(rows[i]);
    if (String(assignment.send_id || '').trim() !== targetSendId) continue;
    const status = String(assignment.status || '').toUpperCase();
    if (statuses[status]) return true;
  }
  return false;
}

function findAssignmentById_(rows, assignmentId) {
  for (let i = 0; i < rows.length; i++) {
    const assignment = rowToAssignmentObject_(rows[i]);
    if (String(assignment.assignment_id || '') === String(assignmentId || '')) {
      return { index: i, assignment: assignment };
    }
  }
  return null;
}

function findPoolIndexBySendId_(poolRows, sendId) {
  for (let i = 0; i < poolRows.length; i++) {
    if (String(poolRows[i][0] || '') === String(sendId || '')) {
      return i;
    }
  }
  return -1;
}

function findSessionIndex_(sessionsRows, sessionId) {
  for (let i = 0; i < sessionsRows.length; i++) {
    if (String(sessionsRows[i][0] || '') === String(sessionId || '')) {
      return i;
    }
  }
  return -1;
}

function getSnapshotsSheet_() {
  const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
  return getOrCreateSheet_(spreadsheet, QA_SNAPSHOTS_SHEET, SNAPSHOT_HEADERS);
}

function appendSnapshotRow_(sheet, snapshot) {
  const nextRow = sheet.getLastRow() + 1;
  sheet.getRange(nextRow, 1, 1, SNAPSHOT_HEADERS.length).setValues([[
    String(snapshot.snapshot_id || ''),
    String(snapshot.snapshot_token || ''),
    String(snapshot.assignment_id || ''),
    String(snapshot.send_id || ''),
    normalizeEmail_(snapshot.created_by_email),
    String(snapshot.created_at || ''),
    String(snapshot.expires_at || ''),
    String(snapshot.status || 'ACTIVE').toUpperCase(),
    String(snapshot.payload_json || '')
  ]]);
}

function findSnapshotRowById_(snapshotRows, snapshotId) {
  for (let i = 0; i < snapshotRows.length; i++) {
    if (String(snapshotRows[i][0] || '') === String(snapshotId || '')) {
      return { index: i, row: snapshotRows[i] };
    }
  }
  return null;
}

function rowToSnapshotObject_(row) {
  return {
    snapshot_id: String(row[0] || ''),
    snapshot_token: String(row[1] || ''),
    assignment_id: String(row[2] || ''),
    send_id: String(row[3] || ''),
    created_by_email: normalizeEmail_(row[4]),
    created_at: String(row[5] || ''),
    expires_at: String(row[6] || ''),
    status: String(row[7] || 'ACTIVE').toUpperCase(),
    payload_json: String(row[8] || '')
  };
}

function updateSnapshotStatus_(sheet, dataIndex, nextStatus) {
  const rowNumber = Number(dataIndex) + 2;
  sheet.getRange(rowNumber, 8).setValue(String(nextStatus || '').toUpperCase());
}

function normalizeSnapshotPayload_(rawPayload, assignment, nowIso) {
  const payloadMap = (rawPayload && typeof rawPayload === 'object') ? rawPayload : {};
  const scenario = cloneJsonSafe_(payloadMap.scenario, {});
  const templates = Array.isArray(payloadMap.templates) ? cloneJsonSafe_(payloadMap.templates, []) : [];
  const note = payloadMap.internal_note != null
    ? String(payloadMap.internal_note)
    : String(assignment.internal_note || '');

  if (!scenario || typeof scenario !== 'object' || Array.isArray(scenario)) {
    return null;
  }

  const snapshotScenario = cloneJsonSafe_(scenario, {});
  if (!snapshotScenario.id) {
    snapshotScenario.id = String(assignment.send_id || '');
  }

  return {
    version: 1,
    assignment_id: String(assignment.assignment_id || ''),
    send_id: String(assignment.send_id || ''),
    scenario: snapshotScenario,
    templates: Array.isArray(templates) ? templates : [],
    internal_note: note,
    created_at: String(nowIso || nowIso_())
  };
}

function normalizeScenarioSnapshotPayload_(rawPayload, nowIso) {
  const payloadMap = (rawPayload && typeof rawPayload === 'object') ? rawPayload : {};
  const scenario = cloneJsonSafe_(payloadMap.scenario, {});
  const templates = Array.isArray(payloadMap.templates) ? cloneJsonSafe_(payloadMap.templates, []) : [];
  const note = payloadMap.internal_note != null ? String(payloadMap.internal_note) : '';
  const assignmentId = payloadMap.assignment_id != null ? String(payloadMap.assignment_id) : '';
  const sendIdRaw = payloadMap.send_id != null
    ? String(payloadMap.send_id)
    : String((scenario && scenario.id) || '');

  if (!scenario || typeof scenario !== 'object' || Array.isArray(scenario)) {
    return null;
  }

  const snapshotScenario = cloneJsonSafe_(scenario, {});
  if (!snapshotScenario.id) {
    snapshotScenario.id = sendIdRaw;
  }

  return {
    version: 1,
    assignment_id: assignmentId,
    send_id: sendIdRaw,
    scenario: snapshotScenario,
    templates: Array.isArray(templates) ? templates : [],
    internal_note: note,
    created_at: String(nowIso || nowIso_())
  };
}

function buildSnapshotUrl_(baseUrl, snapshotId, snapshotToken) {
  const safeBase = String(baseUrl || '').trim();
  if (!safeBase) return '';
  const hasQuery = safeBase.indexOf('?') >= 0;
  const params = [
    'snap=' + encodeURIComponent(String(snapshotId || '')),
    'st=' + encodeURIComponent(String(snapshotToken || ''))
  ];
  return safeBase + (hasQuery ? '&' : '?') + params.join('&');
}

function addHoursToIso_(iso, hours) {
  const baseMs = parseIsoMs_(iso);
  const startMs = baseMs > 0 ? baseMs : Date.now();
  const durationMs = Math.max(0, Number(hours) || 0) * 60 * 60 * 1000;
  return new Date(startMs + durationMs).toISOString();
}

function isSnapshotExpired_(snapshot, nowMs) {
  const expiresMs = parseIsoMs_(snapshot && snapshot.expires_at);
  if (!expiresMs) return true;
  const nowValue = Number(nowMs) || Date.now();
  return nowValue >= expiresMs;
}

function isSessionActive_(session) {
  return session && String(session.state || '').toUpperCase() === 'ACTIVE';
}

function isSessionLive_(session) {
  const state = String((session && session.state) || '').toUpperCase();
  return state === 'ACTIVE' || state === 'COOLDOWN';
}

function sessionToPayload_(session) {
  const submitted = toInt_(session && session.submitted_count, 0);
  const cap = Math.max(1, toInt_(session && session.cap, SESSION_CAP));
  const state = String((session && session.state) || 'ACTIVE').toUpperCase();
  return {
    session_id: String((session && session.session_id) || ''),
    state: state,
    submitted_count: submitted,
    cap: cap,
    session_complete: false
  };
}

function rowToSessionObject_(row) {
  return {
    session_id: String(row[0] || ''),
    agent_email: normalizeEmail_(row[1]),
    state: String(row[2] || 'ACTIVE').toUpperCase(),
    submitted_count: toInt_(row[3], 0),
    cap: Math.max(1, toInt_(row[4], SESSION_CAP)),
    started_at: String(row[5] || ''),
    last_heartbeat_at: String(row[6] || ''),
    ended_at: String(row[7] || ''),
    end_reason: String(row[8] || ''),
    app_base: String(row[9] || '')
  };
}

function sessionToRow_(session) {
  return [
    String(session.session_id || ''),
    normalizeEmail_(session.agent_email),
    String(session.state || 'ACTIVE').toUpperCase(),
    toInt_(session.submitted_count, 0),
    Math.max(1, toInt_(session.cap, SESSION_CAP)),
    String(session.started_at || ''),
    String(session.last_heartbeat_at || ''),
    String(session.ended_at || ''),
    String(session.end_reason || ''),
    String(session.app_base || '')
  ];
}

function appendHistoryRow_(historyRowsToAppend, event) {
  historyRowsToAppend.push([
    nowIso_(),
    String(event.event_type || ''),
    String(event.assignment_id || ''),
    String(event.send_id || ''),
    String(event.from_status || ''),
    String(event.to_status || ''),
    normalizeEmail_(event.agent_email),
    String(event.session_id || ''),
    String(event.detail || '')
  ]);
}

function rowToAssignmentObject_(row) {
  return {
    send_id: String(row[0] || ''),
    assignee_email: normalizeEmail_(row[1]),
    status: String(row[2] || ''),
    assignment_id: String(row[3] || ''),
    created_at: String(row[4] || ''),
    updated_at: String(row[5] || ''),
    done_at: String(row[6] || ''),
    editor_token: String(row[7] || ''),
    viewer_token: String(row[8] || ''),
    form_state_json: String(row[9] || ''),
    internal_note: String(row[10] || ''),
    assigned_session_id: String(row[11] || ''),
    assigned_at: String(row[12] || '')
  };
}

function assignmentToRow_(assignment) {
  return [
    String(assignment.send_id || ''),
    normalizeEmail_(assignment.assignee_email),
    String(assignment.status || ''),
    String(assignment.assignment_id || ''),
    String(assignment.created_at || ''),
    String(assignment.updated_at || ''),
    String(assignment.done_at || ''),
    String(assignment.editor_token || ''),
    String(assignment.viewer_token || ''),
    String(assignment.form_state_json || ''),
    String(assignment.internal_note || ''),
    String(assignment.assigned_session_id || ''),
    String(assignment.assigned_at || '')
  ];
}

function tokenRoleForAssignment_(assignment, token) {
  const rawToken = String(token || '');
  if (!rawToken) return '';
  if (rawToken === String(assignment.editor_token || '')) return 'editor';
  if (rawToken === String(assignment.viewer_token || '')) return 'viewer';
  return '';
}

function withScriptLock_(fn) {
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    return fn();
  } finally {
    lock.releaseLock();
  }
}

function runScheduledSessionCleanup() {
  return withScriptLock_(function() {
    const state = loadAssignmentState_();
    const now = nowIso_();
    const cleanupResult = cleanupStaleSessions_(state, now);
    persistAssignmentState_(state);
    return {
      ok: true,
      ran_at: now,
      timeout_minutes: SESSION_TIMEOUT_MINUTES,
      timed_out_count: Math.max(0, Number(cleanupResult && cleanupResult.timed_out_count) || 0),
      released_count: Math.max(0, Number(cleanupResult && cleanupResult.released_count) || 0)
    };
  });
}

function installSessionCleanupTriggerEveryFiveMinutes() {
  const handlerName = 'runScheduledSessionCleanup';
  const triggers = ScriptApp.getProjectTriggers();
  let removedExisting = 0;

  for (let i = 0; i < triggers.length; i++) {
    const trigger = triggers[i];
    if (
      trigger.getHandlerFunction &&
      trigger.getEventType &&
      String(trigger.getHandlerFunction() || '') === handlerName &&
      String(trigger.getEventType() || '') === String(ScriptApp.EventType.CLOCK)
    ) {
      ScriptApp.deleteTrigger(trigger);
      removedExisting += 1;
    }
  }

  ScriptApp
    .newTrigger(handlerName)
    .timeBased()
    .everyMinutes(5)
    .create();

  return {
    ok: true,
    handler: handlerName,
    interval_minutes: 5,
    removed_existing: removedExisting
  };
}

function removeSessionCleanupTriggers() {
  const handlerName = 'runScheduledSessionCleanup';
  const triggers = ScriptApp.getProjectTriggers();
  let removed = 0;

  for (let i = 0; i < triggers.length; i++) {
    const trigger = triggers[i];
    if (
      trigger.getHandlerFunction &&
      trigger.getEventType &&
      String(trigger.getHandlerFunction() || '') === handlerName &&
      String(trigger.getEventType() || '') === String(ScriptApp.EventType.CLOCK)
    ) {
      ScriptApp.deleteTrigger(trigger);
      removed += 1;
    }
  }

  return {
    ok: true,
    handler: handlerName,
    removed: removed
  };
}

function getOrCreateSheet_(spreadsheet, sheetName, headers) {
  let sheet = spreadsheet.getSheetByName(sheetName);
  if (!sheet) {
    sheet = spreadsheet.insertSheet(sheetName);
  }

  if (sheet.getLastRow() < 1) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else {
    const existingHeaders = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
    let mismatch = false;
    for (let i = 0; i < headers.length; i++) {
      if (String(existingHeaders[i] || '') !== String(headers[i])) {
        mismatch = true;
        break;
      }
    }
    if (mismatch) {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
  }

  return sheet;
}

function getSheetDataRows_(sheet, expectedCols) {
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return [];
  return sheet.getRange(2, 1, lastRow - 1, expectedCols).getValues();
}

function generateOpaqueToken_() {
  return Utilities.getUuid().replace(/-/g, '') + Utilities.getUuid().replace(/-/g, '');
}

function resolveAppBaseUrl_(appBaseUrl) {
  const raw = String(appBaseUrl || '').trim();
  if (raw) return raw;
  return 'app.html';
}

function buildAssignmentUrl_(baseUrl, assignmentId, token, mode) {
  if (!baseUrl) return '';
  const hasQuery = baseUrl.indexOf('?') >= 0;
  const params = [
    'aid=' + encodeURIComponent(String(assignmentId || '')),
    'token=' + encodeURIComponent(String(token || '')),
    'mode=' + encodeURIComponent(String(mode || 'edit'))
  ];
  return baseUrl + (hasQuery ? '&' : '?') + params.join('&');
}

function getRequestAction_(e, body) {
  const fromQuery = getParam_(e, 'action');
  if (fromQuery) return fromQuery;
  return body && body.action ? String(body.action) : '';
}

function getParam_(e, key) {
  if (!e || !e.parameter) return '';
  const raw = e.parameter[key];
  return raw == null ? '' : String(raw);
}

function normalizeEmail_(email) {
  return String(email || '').trim().toLowerCase();
}

function nowIso_() {
  return new Date().toISOString();
}

function parseIsoMs_(value) {
  const t = Date.parse(String(value || ''));
  if (!isFinite(t)) return 0;
  return t;
}

function toInt_(value, fallback) {
  const n = Number(value);
  if (!isFinite(n)) return fallback;
  return Math.floor(n);
}

function stringifyMaybe_(value) {
  if (value == null) return '';
  if (typeof value === 'string') return value;
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value);
  }
}

function parseJsonSafe_(value) {
  try {
    return JSON.parse(String(value || ''));
  } catch (_) {
    return null;
  }
}

function cloneJsonSafe_(value, fallback) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_) {
    return fallback;
  }
}

function jsonResponse_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj || {}))
    .setMimeType(ContentService.MimeType.JSON);
}

function safeAppendRow(targetSheet, values) {
  if (!targetSheet || !values) return;
  const nextRow = targetSheet.getLastRow() + 1;
  const numCols = values.length;
  targetSheet.getRange(nextRow, 1, 1, numCols).setValues([values]);
}

function testFunction() {
  const testData = {
    timestampEST: '07/31/2025',
    agentUsername: 'test_agent',
    scenario: 'Scenario 1',
    customerMessage: 'Test customer message',
    agentResponse: 'Test agent response',
    sessionId: '1_test_session_123',
    sendTime: '02:30'
  };

  const testEvent = {
    postData: {
      contents: JSON.stringify(testData)
    }
  };

  const result = doPost(testEvent);
  console.log('Test result:', result.getContent());
}
