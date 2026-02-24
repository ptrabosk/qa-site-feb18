const fs = require('fs');
const path = require('path');

function loadRuntimeSendIds(fallback = []) {
  try {
    const indexPath = path.resolve(__dirname, '../../../data/scenarios/index.json');
    const indexPayload = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
    const fromById = indexPayload && indexPayload.byId ? Object.keys(indexPayload.byId) : [];
    const ids = fromById.map((v) => String(v || '').trim()).filter(Boolean);
    if (ids.length) return ids;
  } catch (_) {
    // Ignore file read/index parse errors and fall back to static ids.
  }
  return Array.isArray(fallback) ? fallback.map((v) => String(v || '').trim()).filter(Boolean) : [];
}

function createMockMultiAuditorApi(options = {}) {
  const baseAppUrl = String(options.baseAppUrl || 'http://127.0.0.1:4173/app.html');
  const targetQueueSize = Math.max(1, Number(options.targetQueueSize || 3));
  const fallbackPoolSendIds = [
    '019bebf7-17aa-48de-f000-0000f506a3fe',
    '019bd3ae-2a24-478d-f000-0000efb67321',
    '019bdd15-7512-4e9e-f000-0000d6364a39',
    '019bd742-baf8-425f-f000-00003e7e7b41',
    '019bb39c-c90b-4984-f000-0000533e26bd',
    '019bd2ea-59a8-4897-f000-0000e780a047',
    '019bb5d4-a2f2-4c77-f000-0000420de96e',
    '019be9c4-2213-4f8d-f000-000074529bc0',
  ];
  const runtimePoolSendIds = loadRuntimeSendIds(fallbackPoolSendIds);

  const poolSendIds =
    Array.isArray(options.poolSendIds) && options.poolSendIds.length
      ? options.poolSendIds.map((v) => String(v || '').trim()).filter(Boolean)
      : runtimePoolSendIds;

  const activeStatuses = new Set(['ASSIGNED', 'IN_PROGRESS']);
  const sessionsById = {};
  const assignmentsById = {};
  const assignmentOrder = [];
  const state = {
    queueCalls: 0,
    getAssignmentCalls: 0,
    saveDraftCalls: 0,
    doneCalls: 0,
    evaluationCalls: 0,
    heartbeatCalls: 0,
    nextAssignmentSeq: 1,
  };

  let poolCursor = 0;

  function jsonHeaders() {
    return {
      'content-type': 'application/json',
      'access-control-allow-origin': '*',
    };
  }

  async function fulfillJson(route, payload, status = 200) {
    await route.fulfill({
      status,
      headers: jsonHeaders(),
      body: JSON.stringify(payload || {}),
    });
  }

  function normalizeEmail(email) {
    return String(email || '')
      .trim()
      .toLowerCase();
  }

  function buildAssignmentUrls(assignment) {
    const aid = encodeURIComponent(String(assignment.assignment_id || ''));
    const token = encodeURIComponent(String(assignment.token || ''));
    return {
      edit_url: `${baseAppUrl}?aid=${aid}&token=${token}&mode=edit`,
      view_url: `${baseAppUrl}?aid=${aid}&token=${token}&mode=view`,
    };
  }

  function createSession(sessionId, email) {
    sessionsById[sessionId] = {
      session_id: sessionId,
      agent_email: normalizeEmail(email),
      state: 'ACTIVE',
      submitted_count: 0,
      cap: 20,
    };
    return sessionsById[sessionId];
  }

  function getOrCreateSession(sessionId, email) {
    const id = String(sessionId || '').trim();
    if (!id) return null;
    const normalizedEmail = normalizeEmail(email);
    const session = sessionsById[id] || createSession(id, normalizedEmail);
    if (normalizedEmail) {
      session.agent_email = normalizedEmail;
    }
    session.state = 'ACTIVE';
    return session;
  }

  function countActiveForSession(sessionId) {
    return assignmentOrder
      .map((id) => assignmentsById[id])
      .filter(Boolean)
      .filter((assignment) => String(assignment.session_id || '') === String(sessionId || ''))
      .filter((assignment) => activeStatuses.has(String(assignment.status || '').toUpperCase()))
      .length;
  }

  function topUpSessionQueue(sessionId) {
    const id = String(sessionId || '').trim();
    if (!id) return;

    while (countActiveForSession(id) < targetQueueSize && poolCursor < poolSendIds.length) {
      const sendId = String(poolSendIds[poolCursor++] || '').trim();
      if (!sendId) continue;
      const seq = state.nextAssignmentSeq++;
      const assignmentId = `aid-${seq}`;
      const assignment = {
        assignment_id: assignmentId,
        send_id: sendId,
        status: 'ASSIGNED',
        token: `token-${assignmentId}`,
        form_state_json: '',
        internal_note: '',
        session_id: id,
        created_seq: seq,
      };
      assignmentsById[assignmentId] = assignment;
      assignmentOrder.push(assignmentId);
    }
  }

  function listActiveAssignmentsForSession(sessionId) {
    const id = String(sessionId || '').trim();
    return assignmentOrder
      .map((assignmentId) => assignmentsById[assignmentId])
      .filter(Boolean)
      .filter((assignment) => String(assignment.session_id || '') === id)
      .filter((assignment) => activeStatuses.has(String(assignment.status || '').toUpperCase()))
      .sort((a, b) => Number(a.created_seq || 0) - Number(b.created_seq || 0))
      .map((assignment) => {
        const urls = buildAssignmentUrls(assignment);
        return {
          assignment_id: assignment.assignment_id,
          send_id: assignment.send_id,
          status: assignment.status,
          edit_url: urls.edit_url,
          view_url: urls.view_url,
        };
      });
  }

  function buildSessionPayload(sessionId) {
    const session =
      sessionsById[String(sessionId || '').trim()] ||
      createSession(String(sessionId || '').trim(), '');
    return {
      session_id: session.session_id,
      state: String(session.state || 'ACTIVE').toUpperCase(),
      submitted_count: Number(session.submitted_count || 0),
      cap: Number(session.cap || 20),
      session_complete: false,
    };
  }

  function findAssignment(assignmentId) {
    return assignmentsById[String(assignmentId || '').trim()] || null;
  }

  async function handleGet(route, url) {
    const action = String(url.searchParams.get('action') || '');
    if (action === 'queue') {
      state.queueCalls += 1;
      const sessionId = String(url.searchParams.get('session_id') || '').trim();
      const email = String(url.searchParams.get('email') || '').trim();
      const session = getOrCreateSession(sessionId, email);
      if (!session) return fulfillJson(route, { error: 'Missing session_id' });
      topUpSessionQueue(session.session_id);
      return fulfillJson(route, {
        assignments: listActiveAssignmentsForSession(session.session_id),
        session: buildSessionPayload(session.session_id),
      });
    }

    if (action === 'getAssignment') {
      state.getAssignmentCalls += 1;
      const assignmentId = String(url.searchParams.get('assignment_id') || '').trim();
      const token = String(url.searchParams.get('token') || '').trim();
      const sessionId = String(url.searchParams.get('session_id') || '').trim();
      const assignment = findAssignment(assignmentId);
      if (!assignment) return fulfillJson(route, { error: 'Assignment not found' });
      if (String(assignment.token || '') !== token) {
        return fulfillJson(route, { error: 'Unauthorized token' });
      }
      if (
        String(assignment.session_id || '') !== sessionId &&
        String(assignment.status || '').toUpperCase() !== 'DONE'
      ) {
        return fulfillJson(route, { error: 'Assignment is not reserved for this session' });
      }
      return fulfillJson(route, {
        assignment: {
          assignment_id: assignment.assignment_id,
          send_id: assignment.send_id,
          status: assignment.status,
          form_state_json: assignment.form_state_json || '',
          internal_note: assignment.internal_note || '',
          role: 'editor',
        },
        session: buildSessionPayload(sessionId),
      });
    }

    return fulfillJson(route, { status: 'ok' });
  }

  async function handlePost(route, url, bodyRaw) {
    const action = String(url.searchParams.get('action') || '');
    let payload = {};
    try {
      payload = bodyRaw ? JSON.parse(bodyRaw) : {};
    } catch (_) {
      payload = {};
    }

    if (!action) {
      if (String(payload.eventType || '') === 'evaluationFormSubmission') {
        state.evaluationCalls += 1;
      }
      return fulfillJson(route, { status: 'success', message: 'ok' });
    }

    if (action === 'heartbeat') {
      state.heartbeatCalls += 1;
      const sessionId = String(payload.session_id || '').trim();
      const email = String(payload.email || '').trim();
      const session = getOrCreateSession(sessionId, email);
      if (!session) return fulfillJson(route, { error: 'Missing session_id' });
      return fulfillJson(route, {
        ok: true,
        session: buildSessionPayload(session.session_id),
      });
    }

    if (action === 'saveDraft') {
      state.saveDraftCalls += 1;
      const assignment = findAssignment(payload.assignment_id);
      if (!assignment) return fulfillJson(route, { error: 'Assignment not found' });
      if (String(payload.token || '') !== String(assignment.token || '')) {
        return fulfillJson(route, { error: 'Unauthorized: editor token required' });
      }
      if (String(assignment.session_id || '') !== String(payload.session_id || '')) {
        return fulfillJson(route, { error: 'Assignment is not reserved for this session' });
      }
      assignment.form_state_json = String(payload.form_state_json || '');
      assignment.internal_note = String(payload.internal_note || '');
      assignment.status = 'IN_PROGRESS';
      return fulfillJson(route, {
        ok: true,
        session: buildSessionPayload(payload.session_id),
      });
    }

    if (action === 'done') {
      state.doneCalls += 1;
      const sessionId = String(payload.session_id || '').trim();
      const assignment = findAssignment(payload.assignment_id);
      if (!assignment) return fulfillJson(route, { error: 'Assignment not found' });
      if (String(payload.token || '') !== String(assignment.token || '')) {
        return fulfillJson(route, { error: 'Unauthorized: editor token required' });
      }
      if (String(assignment.session_id || '') !== sessionId) {
        return fulfillJson(route, { error: 'Assignment is not reserved for this session' });
      }
      const currentStatus = String(assignment.status || '').toUpperCase();
      if (!activeStatuses.has(currentStatus)) {
        return fulfillJson(route, { error: 'Assignment is not in an active state' });
      }
      assignment.status = 'DONE';
      const session = getOrCreateSession(sessionId, '');
      if (session) {
        session.submitted_count = Number(session.submitted_count || 0) + 1;
      }
      topUpSessionQueue(sessionId);
      return fulfillJson(route, {
        assignments: listActiveAssignmentsForSession(sessionId),
        session: buildSessionPayload(sessionId),
      });
    }

    if (action === 'releaseSession') {
      const sessionId = String(payload.session_id || '').trim();
      const session = sessionsById[sessionId];
      if (session) {
        session.state = 'COOLDOWN';
      }
      return fulfillJson(route, {
        ok: true,
        released_count: 0,
        session: buildSessionPayload(sessionId),
      });
    }

    return fulfillJson(route, { ok: true });
  }

  async function routeHandler(route) {
    const request = route.request();
    const url = new URL(request.url());
    if (!url.pathname.endsWith('/exec')) {
      return route.continue();
    }
    const method = String(request.method() || 'GET').toUpperCase();
    if (method === 'GET') return handleGet(route, url);
    const bodyRaw = request.postData() || '';
    return handlePost(route, url, bodyRaw);
  }

  return {
    state,
    routeHandler,
  };
}

module.exports = { createMockMultiAuditorApi };
