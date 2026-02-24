document.addEventListener('DOMContentLoaded', async () => {
    const chatMessages = document.getElementById('chatMessages');
    const internalNotesEl = document.getElementById('internalNotes');
    const logoutBtn = document.getElementById('logoutBtn');
    const logoutLoadingOverlay = document.getElementById('logoutLoadingOverlay');
    const assignmentSelect = document.getElementById('assignmentSelect');
    const snapshotShareBtn = document.getElementById('snapshotShareBtn');
    const assignmentsStatus = document.getElementById('assignmentsStatus');
    const previousConversationBtn = document.getElementById('previousConversationBtn');
    const nextConversationBtn = document.getElementById('nextConversationBtn');
    const customFormEl = document.getElementById('customForm');
    // Google Sheets integration
    const GOOGLE_SCRIPT_URL = String(
        (window.QA_CONFIG && window.QA_CONFIG.GOOGLE_SCRIPT_URL) ||
        'https://script.google.com/macros/s/AKfycbxdYddYfnFwK4nWaaMmOgzhH6wD0i3jY_1G1XM8PB4NzfJDsmxLrF8abc142KEhagfAbw/exec'
    ).trim();
    const RUNTIME_SCENARIO_INDEX_PATH = 'data/scenarios/index.json';
    const RUNTIME_TEMPLATE_INDEX_PATH = 'data/templates/index.json';
    const ASSIGNMENT_HEARTBEAT_INTERVAL_MS = 60 * 1000;
    const ASSIGNMENT_API_TIMEOUT_MS = 15000;
    const ASSIGNMENT_GET_TIMEOUT_MS = 25000;
    const ASSIGNMENT_DONE_TIMEOUT_MS = 30000;
    const ASSIGNMENT_DRAFT_TIMEOUT_MS = 25000;
    const ASSIGNMENT_HEARTBEAT_TIMEOUT_MS = 35000;
    const ASSIGNMENT_HEARTBEAT_MIN_GAP_MS = 15000;
    const ASSIGNMENT_PREFETCH_TIMEOUT_MS = 20000;
    const ASSIGNMENT_PREFETCH_CONCURRENCY = 1;
    const ASSIGNMENT_HEARTBEAT_WARN_AFTER_FAILURES = 2;
    const SUBMIT_ADVANCE_DELAY_MS = 1200;
    const SUBMIT_OUTBOX_STORAGE_KEY = 'qaSubmitOutbox_v1';
    const SUBMIT_RETRY_DELAYS_MS = [5000, 15000, 45000, 120000, 300000];
    const DEBUG_MODE = !!(
        (window.QA_CONFIG && window.QA_CONFIG.DEBUG === true) ||
        new URLSearchParams(window.location.search).has('debug')
    );
    const IGNORED_SEND_IDS = new Set(
        (((window.QA_CONFIG && window.QA_CONFIG.IGNORED_SEND_IDS) || []))
            .map(v => String(v || '').trim())
            .filter(Boolean)
    );
    // Current scenario data
    let currentScenario = null;
    let scenarioData = null;
    let allScenariosData = null;
    let templatesData = [];
    let assignmentQueue = [];
    let assignmentContext = null;
    let draftSaveTimer = null;
    let monolithicScenariosLoaded = false;
    let runtimeScenariosIndex = null;
    let runtimeScenariosIndexPromise = null;
    let runtimeScenarioChunkCache = {};
    let runtimeScenarioChunkPromises = {};
    let runtimeScenariosUnavailable = false;
    let runtimeTemplatesIndex = null;
    let runtimeTemplatesIndexPromise = null;
    let runtimeTemplateGlobalTemplates = [];
    let runtimeTemplateGlobalLoaded = false;
    let runtimeTemplateCompanyCache = {};
    let runtimeTemplateFilePromises = {};
    let runtimeTemplatesUnavailable = false;
    let templateSearchSourceTemplates = [];
    let templateSearchBound = false;
    let assignmentSessionState = null;
    let assignmentHeartbeatTimer = null;
    let assignmentHeartbeatWarningShown = false;
    let assignmentHeartbeatConsecutiveFailures = 0;
    let assignmentHeartbeatInFlight = false;
    let assignmentLastHeartbeatAt = 0;
    let isExplicitLogoutInProgress = false;
    let pendingLogoutReleasePayload = null;
    let isSnapshotMode = false;
    let snapshotContext = null;
    let submitOutboxTimer = null;
    let submitOutboxProcessing = false;
    let pendingDoneAssignmentIds = new Set();
    let locallySkippedAssignmentIds = new Set();
    let assignmentWindowPrefetchPromises = {};
    let assignmentPayloadCache = {};
    let assignmentPayloadPromises = {};
    let assignmentPayloadPromiseTimeoutMs = {};
    let assignmentPayloadCacheSessionId = '';
    let assignmentPrefetchQueue = [];
    let assignmentPrefetchQueueSet = {};
    let assignmentPrefetchActiveCount = 0;
    let assignmentNavigationInProgress = false;
    let pendingAssignmentNavigationDirection = 0;
    let snapshotCreateInFlight = false;
    let uploadedScenariosCache = [];
    let uploadedScenariosLoaded = false;
    let uploadedScenariosLoadPromise = null;

    if (snapshotShareBtn) {
        snapshotShareBtn.addEventListener('click', async () => {
            await createSnapshotAndCopyLink();
        });
    }

    function debugLog(...args) {
        if (!DEBUG_MODE) return;
        const normalized = args.map((value) => {
            if (value && typeof value === 'object') {
                try {
                    return JSON.stringify(value);
                } catch (_) {
                    return '[unserializable object]';
                }
            }
            return value;
        });
        console.log('[QA DEBUG]', ...normalized);
    }

    if (customFormEl) {
        // Hard guard: never allow native form navigation.
        customFormEl.setAttribute('action', 'javascript:void(0)');
        customFormEl.setAttribute('method', 'post');
        customFormEl.addEventListener('submit', (event) => {
            if (event && typeof event.preventDefault === 'function') {
                event.preventDefault();
            }
            debugLog('native_submit_blocked');
        }, true);
    }

    function isElementVisible(el) {
        if (!el) return false;
        return el.offsetParent !== null;
    }

    function showTransientTopNotice(message, isError) {
        const text = String(message || '').trim();
        if (!text) return;
        const notice = document.createElement('div');
        notice.textContent = text;
        notice.style.cssText = [
            'position: fixed',
            'top: 12px',
            'left: 50%',
            'transform: translateX(-50%)',
            'z-index: 9999',
            `background: ${isError ? '#ffe6e8' : '#ebfff0'}`,
            `color: ${isError ? '#9b111e' : '#1f6a3f'}`,
            `border: 1px solid ${isError ? '#e59aa3' : '#90d7a9'}`,
            'border-radius: 8px',
            'padding: 10px 14px',
            'font-size: 13px',
            'max-width: 70vw',
            'box-shadow: 0 4px 10px rgba(0, 0, 0, 0.12)'
        ].join(';');
        document.body.appendChild(notice);
        setTimeout(() => {
            if (notice && notice.parentNode) {
                notice.parentNode.removeChild(notice);
            }
        }, 3500);
    }

    function isAssignmentLocallySkipped(assignmentId) {
        const id = String(assignmentId || '').trim();
        return !!(id && locallySkippedAssignmentIds.has(id));
    }

    function markAssignmentLocallySkipped(assignmentId, reason) {
        const id = String(assignmentId || '').trim();
        if (!id) return;
        locallySkippedAssignmentIds.add(id);
        debugLog('Marked assignment as locally skipped', { assignmentId: id, reason: String(reason || '') });
    }

    function pruneLocallySkippedAssignments(queue) {
        const keepIds = new Set((Array.isArray(queue) ? queue : [])
            .map(item => String((item && item.assignment_id) || '').trim())
            .filter(Boolean));
        if (!keepIds.size) return;
        Array.from(locallySkippedAssignmentIds).forEach((id) => {
            if (!keepIds.has(id)) {
                locallySkippedAssignmentIds.delete(id);
            }
        });
    }

    async function refreshAssignmentQueue() {
        if (isSnapshotMode) {
            throw new Error('Snapshot view is read-only.');
        }
        if (!canUseAssignmentMode()) {
            throw new Error('Assignment mode requires email login.');
        }
        const email = getLoggedInEmail();
        const sessionId = getAssignmentSessionId({ createIfMissing: true });
        if (!sessionId) throw new Error('Missing assignment session id.');
        const response = await fetchAssignmentGet('queue', {
            email,
            app_base: getCurrentAppBaseUrl(),
            session_id: sessionId
        });
        applyAssignmentSessionState(response && response.session, { silent: true });
        const assignments = Array.isArray(response.assignments) ? response.assignments : [];
        debugLog('Queue refreshed', {
            email,
            requestedSessionId: sessionId,
            returnedSessionId: response && response.session ? String(response.session.session_id || '') : '',
            total: assignments.length
        });
        pruneLocallySkippedAssignments(assignments);
        renderAssignmentQueue(assignments);
        pruneAssignmentResponseCacheToQueue(assignments);
        prefetchAssignmentDetailsInBackground(assignments);
        if (assignmentContext && assignmentContext.assignment_id && assignments.length) {
            prefetchAssignmentWindowInBackground(assignmentContext.assignment_id);
        }
        return assignments;
    }

    async function resolveScenarioKeyForSendId(sendId, scenariosOverride, options = {}) {
        const target = String(sendId || '').trim();
        if (!target) return '';
        const allowMonolithFallback = options.allowMonolithFallback !== false;

        const scenarioIndex = await loadRuntimeScenariosIndex();
        if (scenarioIndex && scenarioIndex.byId && scenarioIndex.byId[target]) {
            return String(scenarioIndex.byId[target]);
        }

        const candidateScenarios = scenariosOverride || allScenariosData || {};
        const directMatch = findScenarioBySendId(candidateScenarios, target);
        if (directMatch && directMatch.scenarioKey) {
            return String(directMatch.scenarioKey);
        }

        if (!allowMonolithFallback) {
            debugLog('Scenario key not found in runtime index/cache', { sendId: target });
            return '';
        }

        const fullScenarios = await loadScenariosDataMonolith();
        const fallbackMatch = findScenarioBySendId(fullScenarios || {}, target);
        return fallbackMatch && fallbackMatch.scenarioKey ? String(fallbackMatch.scenarioKey) : '';
    }

    function hasRuntimeScenarioForSendId(sendId) {
        const target = String(sendId || '').trim();
        if (!target) return false;
        const index = runtimeScenariosIndex;
        return !!(index && index.byId && index.byId[target]);
    }

    function isCsvScenarioMode() {
        return false;
    }

    // Scenario progression system
    function getCurrentUnlockedScenario() {
        const unlockedScenario = localStorage.getItem('unlockedScenario');
        return unlockedScenario ? parseInt(unlockedScenario) : 1; // Default to scenario 1
    }
    
    function unlockNextScenario() {
        const currentUnlocked = getCurrentUnlockedScenario();
        const nextScenario = currentUnlocked + 1;
        localStorage.setItem('unlockedScenario', nextScenario);
        console.log(`Unlocked scenario ${nextScenario}`);
        return nextScenario;
    }
    
    function canAccessScenario(scenarioNumber) {
        if (isAdminUser()) return true;
        if (isCsvScenarioMode()) return true;
        const currentUnlocked = getCurrentUnlockedScenario();
        const requestedScenario = parseInt(scenarioNumber);
        
        // Can only access the current unlocked scenario (no going back)
        return requestedScenario === currentUnlocked;
    }

    function isAdminUser() {
        const agentName = String(localStorage.getItem('agentName') || '').trim().toLowerCase();
        return agentName === 'admin';
    }
    
    function getScenarioNumberFromUrl() {
        const params = new URLSearchParams(window.location.search);
        const value = params.get('scenario');
        return value ? String(value) : null;
    }

    function getScenarioIdFromUrl() {
        const params = new URLSearchParams(window.location.search);
        const value = params.get('sid');
        return value ? String(value).trim() : '';
    }

    function getPageModeFromUrl() {
        const params = new URLSearchParams(window.location.search);
        const mode = String(params.get('mode') || 'edit').toLowerCase();
        return mode === 'view' ? 'view' : 'edit';
    }

    function findScenarioKeyById(scenarios, scenarioId) {
        const target = String(scenarioId || '').trim();
        if (!target) return '';
        const entries = Object.entries(scenarios || {});
        for (let i = 0; i < entries.length; i++) {
            const [key, scenario] = entries[i];
            if (String((scenario && scenario.id) || '').trim() === target) {
                return key;
            }
        }
        return '';
    }

    function resolveRequestedScenarioKey(scenarios) {
        const sid = getScenarioIdFromUrl();
        if (sid) {
            const byId = findScenarioKeyById(scenarios, sid);
            if (byId) return byId;
        }

        const byNumber = getScenarioNumberFromUrl();
        if (byNumber && scenarios && scenarios[byNumber]) {
            return byNumber;
        }

        const stored = localStorage.getItem('currentScenarioNumber');
        if (stored && scenarios && scenarios[stored]) {
            return stored;
        }

        return '';
    }

    function buildScenarioUrl(scenarioKey, scenariosOverride) {
        const key = String(scenarioKey || '').trim();
        if (!key) return 'app.html';

        const scenarios = scenariosOverride || allScenariosData || {};
        const scenario = scenarios && scenarios[key] ? scenarios[key] : null;
        const scenarioId = scenario && scenario.id ? String(scenario.id).trim() : '';

        const params = new URLSearchParams();
        params.set('scenario', key);
        if (scenarioId) params.set('sid', scenarioId);
        params.set('mode', getPageModeFromUrl());
        return `app.html?${params.toString()}`;
    }

    function setCurrentScenarioNumber(value) {
        if (value) {
            localStorage.setItem('currentScenarioNumber', String(value));
        }
    }

    function getCurrentScenarioNumber() {
        return getScenarioNumberFromUrl() ||
            localStorage.getItem('currentScenarioNumber') ||
            '1';
    }

    function getAssignmentParamsFromUrl() {
        const params = new URLSearchParams(window.location.search);
        const aid = params.get('aid');
        const token = params.get('token');
        const mode = (params.get('mode') || 'edit').toLowerCase();
        return {
            aid: aid ? String(aid) : '',
            token: token ? String(token) : '',
            mode: mode === 'view' ? 'view' : 'edit'
        };
    }

    function getAssignmentParamsFromHref(urlValue) {
        if (!urlValue) return null;
        try {
            const resolved = new URL(String(urlValue), window.location.href);
            const params = new URLSearchParams(resolved.search);
            const aid = String(params.get('aid') || '').trim();
            const token = String(params.get('token') || '').trim();
            if (!aid || !token) return null;
            const modeRaw = String(params.get('mode') || 'edit').toLowerCase();
            return {
                aid,
                token,
                mode: modeRaw === 'view' ? 'view' : 'edit',
                href: `app.html?${params.toString()}`
            };
        } catch (_) {
            return null;
        }
    }

    function getSnapshotParamsFromUrl() {
        const params = new URLSearchParams(window.location.search);
        const snapshotId = String(params.get('snap') || '').trim();
        const snapshotToken = String(params.get('st') || '').trim();
        return {
            snapshotId,
            snapshotToken
        };
    }

    function isSnapshotLinkActive() {
        const snapshotParams = getSnapshotParamsFromUrl();
        return !!(snapshotParams.snapshotId && snapshotParams.snapshotToken);
    }

    function getLoggedInEmail() {
        return String(localStorage.getItem('agentEmail') || '').trim().toLowerCase();
    }

    function setAssignmentsStatus(message, isError) {
        const text = String(message || '');
        debugLog('Status', { text, isError: !!isError });
        if (!assignmentsStatus) {
            if (text) {
                if (isError) console.error(text);
                else console.log(text);
            }
            return;
        }
        assignmentsStatus.textContent = text;
        assignmentsStatus.style.color = isError ? '#b00020' : '#4a4a4a';
        if (isError && text && !isElementVisible(assignmentsStatus)) {
            showTransientTopNotice(text, true);
        }
    }

    function createAssignmentSessionId() {
        return `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    }

    function getAssignmentSessionId(options = {}) {
        const createIfMissing = !!options.createIfMissing;
        let sessionId = String(localStorage.getItem('assignmentSessionId') || '').trim();
        if (!sessionId && createIfMissing) {
            sessionId = createAssignmentSessionId();
            localStorage.setItem('assignmentSessionId', sessionId);
        }
        return sessionId;
    }

    function clearAssignmentSessionId() {
        localStorage.removeItem('assignmentSessionId');
        assignmentPayloadCacheSessionId = '';
        clearAssignmentResponseCaches();
    }

    function canUseAssignmentMode() {
        return !!getLoggedInEmail();
    }

    function setAssignmentSessionUiLocked(isLocked) {
        if (assignmentSelect) assignmentSelect.disabled = !!isLocked;
    }

    function readSubmitOutboxJobs() {
        try {
            const raw = localStorage.getItem(SUBMIT_OUTBOX_STORAGE_KEY);
            if (!raw) return [];
            const parsed = JSON.parse(raw);
            if (!Array.isArray(parsed)) return [];
            return parsed
                .filter(job => job && typeof job === 'object')
                .map(job => ({
                    job_id: String(job.job_id || ''),
                    assignment_id: String(job.assignment_id || ''),
                    token: String(job.token || ''),
                    session_id: String(job.session_id || ''),
                    app_base: String(job.app_base || ''),
                    payload: job.payload && typeof job.payload === 'object' ? job.payload : {},
                    created_at: String(job.created_at || ''),
                    attempts: Number(job.attempts || 0),
                    next_retry_at: Number(job.next_retry_at || 0),
                    state: String(job.state || 'pending'),
                    evaluation_sent: !!job.evaluation_sent,
                    last_error: String(job.last_error || '')
                }))
                .filter(job => job.job_id && job.assignment_id && job.token && job.session_id && job.payload);
        } catch (_) {
            return [];
        }
    }

    function filterOutboxJobsForSession(jobs, sessionId) {
        const sid = String(sessionId || '').trim();
        if (!sid) return [];
        return (Array.isArray(jobs) ? jobs : []).filter((job) => String(job && job.session_id || '').trim() === sid);
    }

    function writeSubmitOutboxJobs(jobs) {
        const list = Array.isArray(jobs) ? jobs : [];
        localStorage.setItem(SUBMIT_OUTBOX_STORAGE_KEY, JSON.stringify(list));
    }

    function refreshPendingDoneAssignmentsFromOutbox(jobs) {
        const nextSet = new Set();
        (Array.isArray(jobs) ? jobs : []).forEach((job) => {
            const assignmentId = String(job && job.assignment_id || '').trim();
            const state = String(job && job.state || '').toLowerCase();
            if (!assignmentId) return;
            if (state === 'done') return;
            nextSet.add(assignmentId);
        });
        pendingDoneAssignmentIds = nextSet;
    }

    function isAssignmentPendingDone(assignmentId) {
        const id = String(assignmentId || '').trim();
        if (!id) return false;
        return pendingDoneAssignmentIds.has(id);
    }

    function getSubmitRetryDelayMs(attempts) {
        const n = Math.max(1, Number(attempts) || 1);
        const idx = Math.min(n - 1, SUBMIT_RETRY_DELAYS_MS.length - 1);
        return SUBMIT_RETRY_DELAYS_MS[idx];
    }

    function clearSubmitOutboxTimer() {
        if (submitOutboxTimer) {
            clearTimeout(submitOutboxTimer);
            submitOutboxTimer = null;
        }
    }

    function scheduleSubmitOutboxProcessing(delayMs) {
        const delay = Math.max(0, Number(delayMs) || 0);
        clearSubmitOutboxTimer();
        submitOutboxTimer = setTimeout(() => {
            submitOutboxTimer = null;
            processSubmitOutboxQueue().catch((error) => {
                console.warn('Submit outbox processing failed:', error);
            });
        }, delay);
    }

    async function submitEvaluationFormPayload(payload) {
        const res = await fetch(GOOGLE_SCRIPT_URL, {
            method: 'POST',
            mode: 'cors',
            headers: { 'Content-Type': 'text/plain' },
            body: JSON.stringify(payload || {})
        });

        let success = false;
        let serverMsg = '';
        try {
            const json = await res.json();
            success = res.ok && json && json.status === 'success';
            serverMsg = (json && json.message) ? json.message : '';
        } catch (parseErr) {
            try {
                const txt = await res.text();
                serverMsg = txt || '';
            } catch (_) {}
            success = res.ok;
        }

        if (!success) {
            throw new Error(serverMsg || `Evaluation submission failed (${res.status})`);
        }
    }

    async function processSubmitOutboxQueue() {
        if (submitOutboxProcessing || isSnapshotMode || !canUseAssignmentMode()) return;
        submitOutboxProcessing = true;
        try {
            const activeSessionId = getAssignmentSessionId();
            let jobs = filterOutboxJobsForSession(readSubmitOutboxJobs(), activeSessionId);
            writeSubmitOutboxJobs(jobs);
            if (!jobs.length) {
                refreshPendingDoneAssignmentsFromOutbox([]);
                return;
            }

            const now = Date.now();
            let nextRetryAt = null;
            let hadRetryableFailure = false;
            let hadTerminalFailure = false;

            for (let i = 0; i < jobs.length; i++) {
                const job = jobs[i];
                const state = String(job.state || '').toLowerCase();
                if (state === 'done') continue;

                const nextAt = Number(job.next_retry_at || 0);
                if (nextAt > now) {
                    nextRetryAt = nextRetryAt == null ? nextAt : Math.min(nextRetryAt, nextAt);
                    continue;
                }

                try {
                    if (!job.evaluation_sent) {
                        await submitEvaluationFormPayload(job.payload);
                        job.evaluation_sent = true;
                    }
                    const doneRes = await fetchAssignmentPost('done', {
                        assignment_id: job.assignment_id,
                        token: job.token,
                        session_id: job.session_id,
                        app_base: job.app_base || getCurrentAppBaseUrl()
                    }, { timeoutMs: ASSIGNMENT_DONE_TIMEOUT_MS });
                    applyAssignmentSessionState(doneRes && doneRes.session, { silent: true });
                    const nextQueue = Array.isArray(doneRes.assignments) ? doneRes.assignments : [];
                    renderAssignmentQueue(nextQueue);
                    selectCurrentAssignmentInQueue();
                    if (assignmentContext && assignmentContext.assignment_id) {
                        prefetchAssignmentWindowInBackground(assignmentContext.assignment_id);
                    }

                    job.state = 'done';
                    job.last_error = '';
                    debugLog('outbox_done_success', {
                        assignmentId: String(job.assignment_id || ''),
                        attempts: Number(job.attempts || 0)
                    });
                } catch (error) {
                    const errorText = String((error && error.message) || error || 'Unknown background submit error');
                    const lowerError = errorText.toLowerCase();
                    const isAlreadyDone =
                        lowerError.includes('assignment is not in an active state') ||
                        lowerError.includes('already done');
                    if (isAlreadyDone) {
                        job.state = 'done';
                        job.last_error = '';
                        debugLog('outbox_done_success', {
                            assignmentId: String(job.assignment_id || ''),
                            attempts: Number(job.attempts || 0),
                            dedupedByState: true
                        });
                        continue;
                    }
                    const isTerminal =
                        lowerError.includes('not reserved for this session') ||
                        lowerError.includes('assignment not found') ||
                        lowerError.includes('session not found') ||
                        lowerError.includes('invalid token');

                    if (isTerminal) {
                        hadTerminalFailure = true;
                        job.state = 'failed_terminal';
                        job.last_error = errorText;
                        job.next_retry_at = 0;
                    } else {
                        hadRetryableFailure = true;
                        job.attempts = Math.max(0, Number(job.attempts || 0)) + 1;
                        job.state = 'retrying';
                        job.last_error = errorText;
                        job.next_retry_at = Date.now() + getSubmitRetryDelayMs(job.attempts);
                        debugLog('outbox_done_retry', {
                            assignmentId: String(job.assignment_id || ''),
                            attempts: Number(job.attempts || 0),
                            nextRetryAt: Number(job.next_retry_at || 0),
                            error: errorText
                        });
                        nextRetryAt = nextRetryAt == null ? job.next_retry_at : Math.min(nextRetryAt, job.next_retry_at);
                    }
                }
            }

            jobs = jobs.filter((job) => {
                const state = String(job.state || '').toLowerCase();
                return state !== 'done' && state !== 'failed_terminal';
            });
            writeSubmitOutboxJobs(jobs);
            refreshPendingDoneAssignmentsFromOutbox(jobs);

            if (jobs.length > 0) {
                if (hadRetryableFailure) {
                    setAssignmentsStatus('Background sync pending. Retrying automatically.', true);
                }
                const soonest = nextRetryAt != null
                    ? Math.max(250, nextRetryAt - Date.now())
                    : 250;
                scheduleSubmitOutboxProcessing(soonest);
            } else if (hadTerminalFailure) {
                setAssignmentsStatus('Some background submissions were dropped because assignment ownership changed.', true);
            }
        } finally {
            submitOutboxProcessing = false;
        }
    }

    function queueSubmitOutboxJob(jobInput) {
        const nowIso = new Date().toISOString();
        const nextJob = {
            job_id: `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`,
            assignment_id: String(jobInput && jobInput.assignment_id || '').trim(),
            token: String(jobInput && jobInput.token || '').trim(),
            session_id: String(jobInput && jobInput.session_id || '').trim(),
            app_base: String(jobInput && jobInput.app_base || '').trim(),
            payload: jobInput && typeof jobInput.payload === 'object' ? jobInput.payload : {},
            created_at: nowIso,
            attempts: 0,
            next_retry_at: Date.now(),
            state: 'pending',
            evaluation_sent: false,
            last_error: ''
        };
        if (!nextJob.assignment_id || !nextJob.token || !nextJob.session_id) {
            throw new Error('Cannot queue submit job without assignment context.');
        }

        const existingJobs = filterOutboxJobsForSession(readSubmitOutboxJobs(), nextJob.session_id);
        const jobs = existingJobs.filter((job) => String(job && job.assignment_id || '').trim() !== nextJob.assignment_id);
        const dedupedCount = Math.max(0, existingJobs.length - jobs.length);
        jobs.unshift(nextJob);
        writeSubmitOutboxJobs(jobs);
        refreshPendingDoneAssignmentsFromOutbox(jobs);
        debugLog('outbox_job_enqueued', {
            assignmentId: String(nextJob.assignment_id || ''),
            sessionId: String(nextJob.session_id || ''),
            queueSize: jobs.length,
            dedupedCount
        });
        scheduleSubmitOutboxProcessing(0);
        return nextJob;
    }

    function getAssignmentUrlFromQueueItem(item, options = {}) {
        const entry = item && typeof item === 'object' ? item : {};
        const prefersView = !!options.prefersView;
        const editUrl = String(entry.edit_url || '').trim();
        const viewUrl = String(entry.view_url || '').trim();
        return prefersView ? (viewUrl || editUrl) : (editUrl || viewUrl);
    }

    function getAssignmentResponseCacheKey(params) {
        if (!params || typeof params !== 'object') return '';
        const aid = String(params.aid || '').trim();
        const token = String(params.token || '').trim();
        if (!aid || !token) return '';
        return `${aid}::${token}`;
    }

    function clearAssignmentResponseCaches() {
        assignmentPayloadCache = {};
        assignmentPayloadPromises = {};
        assignmentPayloadPromiseTimeoutMs = {};
        assignmentPrefetchQueue = [];
        assignmentPrefetchQueueSet = {};
        assignmentPrefetchActiveCount = 0;
    }

    function getCachedAssignmentResponse(params) {
        const cacheKey = getAssignmentResponseCacheKey(params);
        if (!cacheKey) return null;
        return assignmentPayloadCache[cacheKey] || null;
    }

    function setCachedAssignmentResponse(params, response) {
        const cacheKey = getAssignmentResponseCacheKey(params);
        if (!cacheKey || !response || typeof response !== 'object') return;
        assignmentPayloadCache[cacheKey] = response;
    }

    function pruneAssignmentResponseCacheToQueue(queue) {
        const keepKeys = new Set(
            (Array.isArray(queue) ? queue : [])
                .map((item) => getAssignmentUrlFromQueueItem(item, { prefersView: false }))
                .concat((Array.isArray(queue) ? queue : []).map((item) => getAssignmentUrlFromQueueItem(item, { prefersView: true })))
                .map((url) => getAssignmentParamsFromHref(url || ''))
                .map((params) => getAssignmentResponseCacheKey(params || {}))
                .filter(Boolean)
        );

        Object.keys(assignmentPayloadCache).forEach((cacheKey) => {
            if (!keepKeys.has(cacheKey)) {
                delete assignmentPayloadCache[cacheKey];
            }
        });
    }

    async function fetchAssignmentResponseForParams(params, sessionId, options = {}) {
        const useCache = options.useCache !== false;
        const timeoutMs = Number(options.timeoutMs) > 0
            ? Number(options.timeoutMs)
            : ASSIGNMENT_GET_TIMEOUT_MS;
        const cacheKey = getAssignmentResponseCacheKey(params);
        if (!cacheKey) throw new Error('Missing assignment cache key.');

        if (useCache && assignmentPayloadCache[cacheKey]) {
            return assignmentPayloadCache[cacheKey];
        }
        const existingPromise = assignmentPayloadPromises[cacheKey];
        const existingTimeoutMs = Number(assignmentPayloadPromiseTimeoutMs[cacheKey]) || 0;
        if (existingPromise && (!existingTimeoutMs || existingTimeoutMs >= timeoutMs)) {
            return existingPromise;
        }
        if (existingPromise && existingTimeoutMs && existingTimeoutMs < timeoutMs) {
            debugLog('Escalating assignment fetch timeout for interactive open', {
                aid: String(params.aid || ''),
                fromTimeoutMs: existingTimeoutMs,
                toTimeoutMs: timeoutMs
            });
        }

        const requestPromise = (async () => {
            const response = await fetchAssignmentGet('getAssignment', {
                assignment_id: params.aid,
                token: params.token,
                session_id: sessionId
            }, { timeoutMs });
            setCachedAssignmentResponse(params, response);
            return response;
        })();
        assignmentPayloadPromises[cacheKey] = requestPromise;
        assignmentPayloadPromiseTimeoutMs[cacheKey] = timeoutMs;

        try {
            return await requestPromise;
        } finally {
            if (assignmentPayloadPromises[cacheKey] === requestPromise) {
                delete assignmentPayloadPromises[cacheKey];
                delete assignmentPayloadPromiseTimeoutMs[cacheKey];
            }
        }
    }

    function getOrderedAssignmentPrefetchItems(queue) {
        const list = Array.isArray(queue) ? queue.filter(Boolean) : [];
        if (!list.length) return [];

        const currentId = String((assignmentContext && assignmentContext.assignment_id) || '').trim();
        const currentIndex = currentId
            ? list.findIndex((item) => String((item && item.assignment_id) || '').trim() === currentId)
            : -1;
        if (currentIndex < 0) return list;

        const ordered = [];
        const seen = {};
        const pushItem = (item) => {
            const id = String((item && item.assignment_id) || '').trim();
            if (!id || seen[id]) return;
            seen[id] = true;
            ordered.push(item);
        };

        // Priority: next, previous, then all remaining.
        if (list.length > 1) {
            pushItem(list[(currentIndex + 1) % list.length]);
            pushItem(list[(currentIndex - 1 + list.length) % list.length]);
        }
        for (let i = 0; i < list.length; i++) {
            pushItem(list[i]);
        }
        return ordered;
    }

    function startAssignmentPrefetchWorkers() {
        if (isSnapshotMode || !canUseAssignmentMode()) return;
        while (assignmentPrefetchActiveCount < ASSIGNMENT_PREFETCH_CONCURRENCY && assignmentPrefetchQueue.length) {
            const task = assignmentPrefetchQueue.shift();
            if (!task || !task.cacheKey) continue;
            delete assignmentPrefetchQueueSet[task.cacheKey];
            const activeSessionId = getAssignmentSessionId();
            if (!activeSessionId || String(task.sessionId || '') !== String(activeSessionId)) {
                continue;
            }
            assignmentPrefetchActiveCount += 1;

            fetchAssignmentResponseForParams(task.params, task.sessionId, {
                useCache: true,
                timeoutMs: ASSIGNMENT_PREFETCH_TIMEOUT_MS
            }).catch((error) => {
                debugLog('Assignment details prefetch failed', {
                    aid: String((task.params && task.params.aid) || ''),
                    error: String((error && error.message) || error || '')
                });
            }).finally(() => {
                assignmentPrefetchActiveCount = Math.max(0, assignmentPrefetchActiveCount - 1);
                startAssignmentPrefetchWorkers();
            });
        }
    }

    function prefetchAssignmentDetailsInBackground(queue) {
        if (isSnapshotMode || !canUseAssignmentMode()) return;
        const sessionId = getAssignmentSessionId();
        if (!sessionId) return;
        const list = getOrderedAssignmentPrefetchItems(queue);
        list.forEach((item) => {
            const url = getAssignmentUrlFromQueueItem(item, { prefersView: false });
            const params = getAssignmentParamsFromHref(url || '');
            const cacheKey = getAssignmentResponseCacheKey(params || {});
            if (!params || !cacheKey || assignmentPayloadCache[cacheKey] || assignmentPayloadPromises[cacheKey] || assignmentPrefetchQueueSet[cacheKey]) {
                return;
            }
            assignmentPrefetchQueueSet[cacheKey] = true;
            assignmentPrefetchQueue.push({ cacheKey, params, sessionId });
        });
        startAssignmentPrefetchWorkers();
    }

    function getNextAssignmentParamsFromQueue(queue, options = {}) {
        const list = (Array.isArray(queue) ? queue : [])
            .filter(item => !isAssignmentLocallySkipped(item && item.assignment_id));
        if (!list.length) return null;
        const excludeSet = new Set((Array.isArray(options.excludeAssignmentIds) ? options.excludeAssignmentIds : [])
            .map(id => String(id || '').trim())
            .filter(Boolean));
        const prefersView = !!options.prefersView;
        for (let i = 0; i < list.length; i++) {
            const item = list[i] || {};
            const assignmentId = String(item.assignment_id || '').trim();
            if (!assignmentId || excludeSet.has(assignmentId) || isAssignmentPendingDone(assignmentId) || isAssignmentLocallySkipped(assignmentId)) {
                continue;
            }
            const url = getAssignmentUrlFromQueueItem(item, { prefersView });
            if (!url) continue;
            const params = getAssignmentParamsFromHref(url);
            if (params && params.aid && params.token) {
                return params;
            }
        }
        return null;
    }

    function applyAssignmentSessionState(sessionLike, options = {}) {
        if (!sessionLike || typeof sessionLike !== 'object') return;
        assignmentSessionState = sessionLike;
        const sessionId = String(sessionLike.session_id || '').trim();
        if (sessionId) {
            if (assignmentPayloadCacheSessionId && assignmentPayloadCacheSessionId !== sessionId) {
                clearAssignmentResponseCaches();
            }
            assignmentPayloadCacheSessionId = sessionId;
            localStorage.setItem('assignmentSessionId', sessionId);
        }

        setAssignmentSessionUiLocked(false);
        if (!options.silent) {
            setAssignmentsStatus('', false);
        }
        if (assignmentContext) {
            const forceView = assignmentContext.role === 'viewer' || assignmentContext.mode === 'view';
            setAssignmentReadOnlyState(forceView);
        }
    }

    function getCurrentAppBaseUrl() {
        const origin = window.location.origin || '';
        const path = window.location.pathname || '/app.html';
        return `${origin}${path}`;
    }

    function normalizeRuntimePath(pathValue) {
        return String(pathValue || '').trim().replace(/\\/g, '/').replace(/^\.\//, '');
    }

    function buildDemoScenarioFallback() {
        return {
            '1': normalizeScenarioRecord({
                companyName: 'Demo Company',
                agentName: localStorage.getItem('agentName') || 'Agent',
                customerPhone: '(000) 000-0000',
                customerMessage: 'Welcome! Start the conversation here.',
                notes: {
                    important: ['Run with a local server to load full scenarios.json']
                },
                rightPanel: { source: { label: 'Source', value: 'Local Demo', date: '' } }
            }, {}, '1')
        };
    }

    async function loadScenariosDataMonolith() {
        if (monolithicScenariosLoaded && allScenariosData && Object.keys(allScenariosData).length) {
            return allScenariosData;
        }

        if (window.location.protocol === 'file:') {
            allScenariosData = buildDemoScenarioFallback();
            monolithicScenariosLoaded = true;
            return allScenariosData;
        }

        try {
            const response = await fetch('scenarios.json');
            if (!response.ok) throw new Error(`scenarios.json load failed (${response.status})`);
            const data = await response.json();
            const scenarios = coerceScenariosPayloadToMap(data);
            allScenariosData = scenarios || {};
            monolithicScenariosLoaded = true;
            return allScenariosData;
        } catch (error) {
            console.error('Error loading scenarios data:', error);
            return allScenariosData;
        }
    }

    async function loadTemplatesDataMonolith() {
        if (Array.isArray(templatesData) && templatesData.length) {
            return templatesData;
        }
        try {
            const response = await fetch('templates.json', { method: 'GET' });
            if (!response.ok) throw new Error(`templates.json load failed (${response.status})`);
            const data = await response.json();
            templatesData = Array.isArray(data.templates) ? data.templates : [];
            return templatesData;
        } catch (error) {
            console.error('Error loading templates.json fallback:', error);
            return templatesData;
        }
    }

    async function loadRuntimeScenariosIndex() {
        if (runtimeScenariosUnavailable) return null;
        if (runtimeScenariosIndex) return runtimeScenariosIndex;
        if (runtimeScenariosIndexPromise) return runtimeScenariosIndexPromise;

        runtimeScenariosIndexPromise = (async () => {
            try {
                const response = await fetch(RUNTIME_SCENARIO_INDEX_PATH, { method: 'GET' });
                if (!response.ok) throw new Error(`Scenario runtime index unavailable (${response.status})`);
                const data = await response.json();
                const hasShape = data && typeof data === 'object' &&
                    Array.isArray(data.order) &&
                    data.byKey && typeof data.byKey === 'object';
                if (!hasShape) throw new Error('Scenario runtime index has invalid shape.');
                runtimeScenariosIndex = data;
                return runtimeScenariosIndex;
            } catch (error) {
                runtimeScenariosUnavailable = true;
                console.warn('Falling back to monolithic scenarios:', error);
                return null;
            } finally {
                runtimeScenariosIndexPromise = null;
            }
        })();
        return runtimeScenariosIndexPromise;
    }

    async function loadRuntimeScenarioChunk(chunkPath) {
        const normalizedPath = normalizeRuntimePath(chunkPath);
        if (!normalizedPath) return {};
        if (runtimeScenarioChunkCache[normalizedPath]) return runtimeScenarioChunkCache[normalizedPath];
        if (runtimeScenarioChunkPromises[normalizedPath]) return runtimeScenarioChunkPromises[normalizedPath];

        runtimeScenarioChunkPromises[normalizedPath] = (async () => {
            const response = await fetch(normalizedPath, { method: 'GET' });
            if (!response.ok) throw new Error(`Scenario chunk load failed (${response.status}): ${normalizedPath}`);
            const data = await response.json();
            const scenariosRaw = (data && typeof data === 'object' && data.scenarios && typeof data.scenarios === 'object')
                ? data.scenarios
                : {};
            const normalizedScenarios = {};
            Object.keys(scenariosRaw).forEach((scenarioKey) => {
                normalizedScenarios[String(scenarioKey)] = normalizeScenarioRecord(scenariosRaw[scenarioKey], {}, String(scenarioKey));
            });
            runtimeScenarioChunkCache[normalizedPath] = normalizedScenarios;
            return normalizedScenarios;
        })();

        try {
            return await runtimeScenarioChunkPromises[normalizedPath];
        } finally {
            delete runtimeScenarioChunkPromises[normalizedPath];
        }
    }

    async function ensureScenariosLoaded(keys, options = {}) {
        const requestedKeys = Array.from(new Set((Array.isArray(keys) ? keys : []).map(k => String(k || '').trim()).filter(Boolean)));
        if (!requestedKeys.length) return allScenariosData || {};
        const allowMonolithFallback = options.allowMonolithFallback !== false;
        allScenariosData = allScenariosData || {};

        const missingKeys = requestedKeys.filter(k => !allScenariosData[k]);
        if (!missingKeys.length) return allScenariosData;

        if (window.location.protocol === 'file:') {
            if (!Object.keys(allScenariosData).length) {
                allScenariosData = buildDemoScenarioFallback();
            }
            return allScenariosData;
        }

        const scenarioIndex = await loadRuntimeScenariosIndex();
        if (scenarioIndex) {
            try {
                const chunkFiles = Array.from(new Set(
                    missingKeys
                        .map((scenarioKey) => scenarioIndex.byKey && scenarioIndex.byKey[scenarioKey] ? normalizeRuntimePath(scenarioIndex.byKey[scenarioKey].chunkFile) : '')
                        .filter(Boolean)
                ));
                const chunkMaps = await Promise.all(chunkFiles.map(chunkFile => loadRuntimeScenarioChunk(chunkFile)));
                for (let i = 0; i < chunkMaps.length; i++) {
                    const chunkMap = chunkMaps[i] || {};
                    Object.keys(chunkMap).forEach((scenarioKey) => {
                        allScenariosData[scenarioKey] = chunkMap[scenarioKey];
                    });
                }
            } catch (error) {
                runtimeScenariosUnavailable = true;
                console.warn('Scenario chunk load failed, switching to monolithic mode:', error);
            }
        }

        const stillMissing = requestedKeys.filter(k => !allScenariosData[k]);
        if (stillMissing.length && allowMonolithFallback) {
            await loadScenariosDataMonolith();
        }
        return allScenariosData || {};
    }

    async function loadRuntimeTemplatesIndex() {
        if (runtimeTemplatesUnavailable) return null;
        if (runtimeTemplatesIndex) return runtimeTemplatesIndex;
        if (runtimeTemplatesIndexPromise) return runtimeTemplatesIndexPromise;

        runtimeTemplatesIndexPromise = (async () => {
            try {
                const response = await fetch(RUNTIME_TEMPLATE_INDEX_PATH, { method: 'GET' });
                if (!response.ok) throw new Error(`Template runtime index unavailable (${response.status})`);
                const data = await response.json();
                const hasShape = data && typeof data === 'object' &&
                    data.companies && typeof data.companies === 'object' &&
                    typeof data.globalFile === 'string';
                if (!hasShape) throw new Error('Template runtime index has invalid shape.');
                runtimeTemplatesIndex = data;
                return runtimeTemplatesIndex;
            } catch (error) {
                runtimeTemplatesUnavailable = true;
                console.warn('Falling back to monolithic templates:', error);
                return null;
            } finally {
                runtimeTemplatesIndexPromise = null;
            }
        })();
        return runtimeTemplatesIndexPromise;
    }

    async function loadRuntimeTemplateFile(pathValue) {
        const normalizedPath = normalizeRuntimePath(pathValue);
        if (!normalizedPath) return null;
        if (runtimeTemplateFilePromises[normalizedPath]) return runtimeTemplateFilePromises[normalizedPath];

        runtimeTemplateFilePromises[normalizedPath] = (async () => {
            const response = await fetch(normalizedPath, { method: 'GET' });
            if (!response.ok) throw new Error(`Template bundle load failed (${response.status}): ${normalizedPath}`);
            return response.json();
        })();

        try {
            return await runtimeTemplateFilePromises[normalizedPath];
        } finally {
            delete runtimeTemplateFilePromises[normalizedPath];
        }
    }

    async function ensureRuntimeTemplateGlobalLoaded() {
        if (runtimeTemplateGlobalLoaded) return true;
        const templateIndex = await loadRuntimeTemplatesIndex();
        if (!templateIndex) return false;
        const globalFile = normalizeRuntimePath(templateIndex.globalFile);
        if (!globalFile) return false;
        try {
            const globalData = await loadRuntimeTemplateFile(globalFile);
            runtimeTemplateGlobalTemplates = Array.isArray(globalData && globalData.templates) ? globalData.templates : [];
            runtimeTemplateGlobalLoaded = true;
            return true;
        } catch (error) {
            runtimeTemplatesUnavailable = true;
            console.warn('Template global bundle failed, switching to monolithic mode:', error);
            return false;
        }
    }

    async function ensureRuntimeTemplateCompanyLoaded(companyKey) {
        const normalizedCompany = normalizeName(companyKey);
        if (!normalizedCompany) return true;
        if (runtimeTemplateCompanyCache[normalizedCompany]) return true;

        const templateIndex = await loadRuntimeTemplatesIndex();
        if (!templateIndex) return false;

        const relativePath = templateIndex.companies ? templateIndex.companies[normalizedCompany] : '';
        if (!relativePath) {
            runtimeTemplateCompanyCache[normalizedCompany] = [];
            return true;
        }

        try {
            const companyData = await loadRuntimeTemplateFile(relativePath);
            runtimeTemplateCompanyCache[normalizedCompany] = Array.isArray(companyData && companyData.templates) ? companyData.templates : [];
            return true;
        } catch (error) {
            runtimeTemplatesUnavailable = true;
            console.warn('Template company bundle failed, switching to monolithic mode:', error);
            return false;
        }
    }

    async function ensureTemplatesLoadedForScenarioKeys(scenarioKeys) {
        const keys = Array.from(new Set((Array.isArray(scenarioKeys) ? scenarioKeys : []).map(k => String(k || '').trim()).filter(Boolean)));
        if (!keys.length) return;

        if (window.location.protocol === 'file:') {
            await loadTemplatesDataMonolith();
            return;
        }

        const templateIndex = await loadRuntimeTemplatesIndex();
        if (!templateIndex) {
            await loadTemplatesDataMonolith();
            return;
        }

        const globalLoaded = await ensureRuntimeTemplateGlobalLoaded();
        if (!globalLoaded) {
            await loadTemplatesDataMonolith();
            return;
        }

        const companyKeys = Array.from(new Set(keys
            .map((scenarioKey) => {
                const scenario = allScenariosData && allScenariosData[scenarioKey];
                return normalizeName(scenario && scenario.companyName);
            })
            .filter(Boolean)));

        const companyLoadResults = await Promise.all(companyKeys.map(companyKey => ensureRuntimeTemplateCompanyLoaded(companyKey)));
        if (companyLoadResults.some(result => !result)) {
            runtimeTemplatesUnavailable = true;
        }

        if (runtimeTemplatesUnavailable) {
            await loadTemplatesDataMonolith();
        }
    }

    function getCenteredWindowItems(list, currentIndex, windowSize) {
        const safeList = Array.isArray(list) ? list : [];
        if (!safeList.length) return [];
        if (safeList.length <= windowSize) return safeList.slice();
        const max = Math.floor(windowSize / 2);
        const selected = [];
        for (let offset = -max; offset <= max; offset++) {
            const idx = (currentIndex + offset + safeList.length) % safeList.length;
            selected.push(safeList[idx]);
        }
        return selected;
    }

    async function fetchJsonWithTimeout(url, options = {}, timeoutMs = ASSIGNMENT_API_TIMEOUT_MS) {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
        try {
            const start = Date.now();
            const response = await fetch(url, Object.assign({}, options, { signal: controller.signal }));
            const elapsedMs = Date.now() - start;
            debugLog('HTTP', {
                method: String(options && options.method || 'GET'),
                url,
                status: response.status,
                elapsedMs
            });
            return response;
        } catch (error) {
            if (error && error.name === 'AbortError') {
                throw new Error(`Request timed out after ${timeoutMs}ms`);
            }
            throw error;
        } finally {
            clearTimeout(timeoutId);
        }
    }

    async function fetchAssignmentGet(action, queryParams, options = {}) {
        const params = new URLSearchParams({ action });
        Object.keys(queryParams || {}).forEach((key) => {
            if (queryParams[key] != null && queryParams[key] !== '') {
                params.set(key, String(queryParams[key]));
            }
        });
        const url = `${GOOGLE_SCRIPT_URL}?${params.toString()}`;
        const timeoutMs = Number(options.timeoutMs) > 0
            ? Number(options.timeoutMs)
            : ASSIGNMENT_API_TIMEOUT_MS;
        const res = await fetchJsonWithTimeout(url, {
            method: 'GET',
            mode: 'cors'
        }, timeoutMs);
        const json = await res.json().catch(() => ({}));
        if (!res.ok || (json && json.error)) {
            throw new Error((json && json.error) ? json.error : `Request failed (${res.status})`);
        }
        return json;
    }

    async function fetchAssignmentPost(action, payload, options = {}) {
        const params = new URLSearchParams({ action });
        const url = `${GOOGLE_SCRIPT_URL}?${params.toString()}`;
        const timeoutMs = Number(options.timeoutMs) > 0
            ? Number(options.timeoutMs)
            : ASSIGNMENT_API_TIMEOUT_MS;
        const res = await fetchJsonWithTimeout(url, {
            method: 'POST',
            mode: 'cors',
            headers: { 'Content-Type': 'text/plain' },
            body: JSON.stringify(payload || {})
        }, timeoutMs);
        const json = await res.json().catch(() => ({}));
        if (!res.ok || (json && json.error)) {
            throw new Error((json && json.error) ? json.error : `Request failed (${res.status})`);
        }
        return json;
    }

    function stopAssignmentHeartbeat() {
        if (assignmentHeartbeatTimer) {
            clearInterval(assignmentHeartbeatTimer);
            assignmentHeartbeatTimer = null;
        }
    }

    async function sendAssignmentHeartbeat(options = {}) {
        if (isSnapshotMode) return;
        if (!canUseAssignmentMode()) return;
        const email = getLoggedInEmail();
        const sessionId = getAssignmentSessionId();
        if (!email || !sessionId) return;
        const force = !!(options && options.force);
        const nowMs = Date.now();
        if (!force && assignmentLastHeartbeatAt && (nowMs - assignmentLastHeartbeatAt) < ASSIGNMENT_HEARTBEAT_MIN_GAP_MS) {
            return;
        }
        if (assignmentHeartbeatInFlight) return;
        assignmentHeartbeatInFlight = true;
        try {
            const response = await fetchAssignmentPost('heartbeat', {
                email,
                session_id: sessionId,
                client_ts: new Date().toISOString()
            }, { timeoutMs: ASSIGNMENT_HEARTBEAT_TIMEOUT_MS });
            applyAssignmentSessionState(response && response.session, { silent: true });
            assignmentHeartbeatConsecutiveFailures = 0;
            assignmentHeartbeatWarningShown = false;
            assignmentLastHeartbeatAt = Date.now();
        } catch (error) {
            console.warn('Assignment heartbeat failed:', error);
            assignmentHeartbeatConsecutiveFailures += 1;
            if (!assignmentHeartbeatWarningShown && assignmentHeartbeatConsecutiveFailures >= ASSIGNMENT_HEARTBEAT_WARN_AFTER_FAILURES) {
                setAssignmentsStatus('Assignment heartbeat warning. Your queue is still open; keep working.', true);
                assignmentHeartbeatWarningShown = true;
            }
        } finally {
            assignmentHeartbeatInFlight = false;
        }
    }

    function startAssignmentHeartbeat() {
        if (isSnapshotMode) return;
        if (!canUseAssignmentMode()) return;
        if (!assignmentSessionState || !assignmentSessionState.session_id) return;
        const sessionId = getAssignmentSessionId();
        if (!sessionId) return;
        stopAssignmentHeartbeat();
        assignmentHeartbeatConsecutiveFailures = 0;
        assignmentHeartbeatWarningShown = false;
        sendAssignmentHeartbeat({ force: true });
        assignmentHeartbeatTimer = setInterval(() => {
            sendAssignmentHeartbeat();
        }, ASSIGNMENT_HEARTBEAT_INTERVAL_MS);
    }

    async function releaseAssignmentSession(reason) {
        if (isSnapshotMode) return { ok: false, released_count: 0 };
        const email = getLoggedInEmail();
        const sessionId = getAssignmentSessionId();
        if (!email || !sessionId) return { ok: false, released_count: 0 };
        try {
            const response = await fetchAssignmentPost('releaseSession', {
                email,
                session_id: sessionId,
                reason: String(reason || 'manual')
            });
            applyAssignmentSessionState(response && response.session, { silent: true });
            return response;
        } catch (error) {
            console.warn('releaseSession failed:', error);
            return { ok: false, released_count: 0 };
        }
    }

    function sendBeaconReleaseSession(reason, payloadOverride) {
        if (!navigator.sendBeacon) return;
        const override = payloadOverride && typeof payloadOverride === 'object' ? payloadOverride : null;
        const email = override
            ? String(override.email || '').trim().toLowerCase()
            : String(localStorage.getItem('agentEmail') || '').trim().toLowerCase();
        const sessionId = override
            ? String(override.session_id || '').trim()
            : String(localStorage.getItem('assignmentSessionId') || '').trim();
        if (!email || !sessionId) return;
        const payload = JSON.stringify({
            email,
            session_id: sessionId,
            reason: String(reason || 'logout')
        });
        const params = new URLSearchParams({ action: 'releaseSession' });
        const endpoint = `${GOOGLE_SCRIPT_URL}?${params.toString()}`;
        const blob = new Blob([payload], { type: 'text/plain' });
        navigator.sendBeacon(endpoint, blob);
    }

    function updateSnapshotShareButtonVisibility() {
        if (!snapshotShareBtn) return;
        const isEditableAssignment = !!(
            assignmentContext &&
            assignmentContext.assignment_id &&
            assignmentContext.role === 'editor'
        );
        const canCreateScenarioSnapshot = !!scenarioData;
        const canUse = !isSnapshotMode && (isEditableAssignment || canCreateScenarioSnapshot);
        snapshotShareBtn.style.display = isSnapshotMode ? 'none' : 'inline-flex';
        snapshotShareBtn.disabled = !canUse;
        snapshotShareBtn.title = isEditableAssignment
            ? 'Create assignment snapshot link'
            : 'Create snapshot link';
    }

    async function copyTextToClipboard(text) {
        const value = String(text || '');
        if (!value) return false;
        if (navigator.clipboard && typeof navigator.clipboard.writeText === 'function') {
            try {
                await navigator.clipboard.writeText(value);
                return true;
            } catch (_) {}
        }

        try {
            const temp = document.createElement('textarea');
            temp.value = value;
            temp.style.position = 'fixed';
            temp.style.opacity = '0';
            temp.style.pointerEvents = 'none';
            document.body.appendChild(temp);
            temp.focus();
            temp.select();
            const ok = document.execCommand('copy');
            document.body.removeChild(temp);
            return !!ok;
        } catch (_) {
            return false;
        }
    }

    function buildSnapshotPayloadForShare() {
        if (!scenarioData || typeof scenarioData !== 'object') {
            throw new Error('Scenario data is not available yet.');
        }

        const scenarioClone = JSON.parse(JSON.stringify(scenarioData));
        const templatesForScenario = getTemplatesForScenario(scenarioData);
        const templatesClone = JSON.parse(JSON.stringify(Array.isArray(templatesForScenario) ? templatesForScenario : []));
        const internalNote = internalNotesEl ? String(internalNotesEl.value || '') : '';

        return {
            version: 1,
            assignment_id: String((assignmentContext && assignmentContext.assignment_id) || ''),
            send_id: String((assignmentContext && assignmentContext.send_id) || (scenarioClone && scenarioClone.id) || ''),
            scenario: scenarioClone,
            templates: templatesClone,
            internal_note: internalNote,
            created_at: new Date().toISOString()
        };
    }

    function compactSnapshotPayload(payload, maxChars = 43000) {
        const source = payload && typeof payload === 'object' ? payload : {};
        const sizeOf = (value) => {
            try {
                return JSON.stringify(value).length;
            } catch (_) {
                return Number.MAX_SAFE_INTEGER;
            }
        };
        const clone = (value, fallback) => {
            try {
                return JSON.parse(JSON.stringify(value));
            } catch (_) {
                return fallback;
            }
        };
        const compactTemplateList = (list, maxItems, maxContentChars) => {
            const sourceList = Array.isArray(list) ? list : [];
            return sourceList.slice(0, Math.max(0, Number(maxItems) || 0)).map((template) => {
                const t = template && typeof template === 'object' ? template : {};
                const nextTemplate = {
                    name: String(t.name || '').trim(),
                    shortcut: String(t.shortcut || '').trim(),
                    content: String(t.content || ''),
                    companyName: String(t.companyName || '').trim()
                };
                const cap = Math.max(0, Number(maxContentChars) || 0);
                if (cap > 0 && nextTemplate.content.length > cap) {
                    nextTemplate.content = `${nextTemplate.content.slice(0, cap)}...`;
                }
                return nextTemplate;
            });
        };
        const compactHistoryList = (list, maxItems) => {
            const sourceList = Array.isArray(list) ? list : [];
            return sourceList.slice(0, Math.max(0, Number(maxItems) || 0)).map((item) => {
                const raw = item && typeof item === 'object' ? item : {};
                const itemText = String(raw.item || raw.product_name || raw.name || '').trim();
                const linkText = String(raw.link || raw.product_link || raw.url || '').trim();
                const timeText = String(raw.timeAgo || raw.view_date || raw.date || '').trim();
                return {
                    item: itemText.slice(0, 180),
                    link: linkText.slice(0, 260),
                    timeAgo: timeText.slice(0, 80)
                };
            });
        };
        const compactConversationText = (conversation, maxLen) => (
            (Array.isArray(conversation) ? conversation : []).map((msg) => {
                const safeMsg = msg && typeof msg === 'object' ? Object.assign({}, msg) : {};
                const text = String(safeMsg.content || safeMsg.message || '');
                const limit = Math.max(0, Number(maxLen) || 0);
                if (limit > 0 && text.length > limit) {
                    safeMsg.content = `${text.slice(0, limit)}...`;
                    if (Object.prototype.hasOwnProperty.call(safeMsg, 'message')) {
                        safeMsg.message = safeMsg.content;
                    }
                }
                return safeMsg;
            })
        );

        let next = clone(source, {});
        if (sizeOf(next) <= maxChars) return next;

        // Keep the snapshot lean, but preserve browsing history and templates.
        const scenario = next.scenario && typeof next.scenario === 'object' ? next.scenario : {};
        const rightPanel = scenario.rightPanel && typeof scenario.rightPanel === 'object' ? scenario.rightPanel : {};
        const keepRightPanel = clone(rightPanel, {});
        const keepSource = clone(rightPanel.source || {}, {});
        const keepCustomer = clone(rightPanel.customer || {}, {});
        const keepGuidelines = clone(rightPanel.guidelines || {}, {});

        next.scenario = {
            ...clone(scenario, {}),
            id: scenario.id || next.send_id || '',
            companyName: scenario.companyName || '',
            companyWebsite: scenario.companyWebsite || '',
            has_shopify: scenarioHasShopify(scenario),
            agentName: scenario.agentName || '',
            customerPhone: scenario.customerPhone || '',
            conversation: Array.isArray(scenario.conversation) ? clone(scenario.conversation, []) : [],
            notes: scenario.notes && typeof scenario.notes === 'object' ? clone(scenario.notes, {}) : {},
            rightPanel: {
                ...keepRightPanel,
                source: keepSource,
                customer: keepCustomer,
                guidelines: keepGuidelines
            }
        };
        next.templates = Array.isArray(next.templates) ? clone(next.templates, []) : [];
        if (sizeOf(next) <= maxChars) return next;

        // First trim conversation count, then message length.
        const fullConversation = Array.isArray(next.scenario.conversation) ? next.scenario.conversation : [];
        [120, 80, 50, 30, 20, 10].forEach((keepCount) => {
            if (sizeOf(next) <= maxChars) return;
            if (fullConversation.length > keepCount) {
                next.scenario.conversation = clone(fullConversation.slice(Math.max(0, fullConversation.length - keepCount)), []);
            }
        });
        if (sizeOf(next) <= maxChars) return next;

        [650, 450, 300, 200, 140].forEach((maxLen) => {
            if (sizeOf(next) <= maxChars) return;
            next.scenario.conversation = compactConversationText(next.scenario.conversation, maxLen);
        });
        if (sizeOf(next) <= maxChars) return next;

        // Compact templates but keep them present.
        const templateItemCaps = [80, 50, 30, 20, 10, 5];
        const templateContentCaps = [800, 500, 300, 180, 120, 80];
        for (let i = 0; i < templateItemCaps.length; i++) {
            if (sizeOf(next) <= maxChars) break;
            next.templates = compactTemplateList(next.templates, templateItemCaps[i], templateContentCaps[i]);
            if (next.scenario && next.scenario.rightPanel && typeof next.scenario.rightPanel === 'object') {
                next.scenario.rightPanel.templates = clone(next.templates, []);
            }
        }
        if (sizeOf(next) <= maxChars) return next;

        // Compact browsing history while keeping a usable trail.
        ['browsingHistory', 'browsing_history', 'last5Products', 'last_5_products'].forEach((historyKey) => {
            if (sizeOf(next) <= maxChars) return;
            const historyList = next.scenario &&
                next.scenario.rightPanel &&
                Array.isArray(next.scenario.rightPanel[historyKey])
                ? next.scenario.rightPanel[historyKey]
                : [];
            if (!historyList.length) return;
            [20, 12, 8, 5, 3].forEach((cap) => {
                if (sizeOf(next) <= maxChars) return;
                next.scenario.rightPanel[historyKey] = compactHistoryList(historyList, cap);
            });
        });
        if (sizeOf(next) <= maxChars) return next;

        // Final trim for very large snapshots.
        if (next.scenario && next.scenario.notes && typeof next.scenario.notes === 'object') {
            next.scenario.notes = {};
        }
        if (sizeOf(next) <= maxChars) return next;
        if (
            next.scenario &&
            next.scenario.rightPanel &&
            next.scenario.rightPanel.guidelines &&
            typeof next.scenario.rightPanel.guidelines === 'object'
        ) {
            next.scenario.rightPanel.guidelines = {};
        }
        return next;
    }

    function formatSnapshotExpiry(expiryIso) {
        const t = Date.parse(String(expiryIso || ''));
        if (!Number.isFinite(t)) return '';
        return new Date(t).toLocaleString();
    }

    async function createSnapshotAndCopyLink() {
        if (isSnapshotMode) return;
        if (snapshotCreateInFlight) {
            setAssignmentsStatus('Snapshot is already being created...', false);
            return;
        }
        snapshotCreateInFlight = true;
        if (snapshotShareBtn) snapshotShareBtn.disabled = true;
        try {
            debugLog('Snapshot creation requested', {
                hasAssignmentContext: !!assignmentContext,
                isEditorAssignment: !!(assignmentContext && assignmentContext.role === 'editor'),
                hasScenarioData: !!scenarioData
            });
            setAssignmentsStatus('Creating snapshot link...', false);
            const rawPayload = buildSnapshotPayloadForShare();
            const payload = compactSnapshotPayload(rawPayload, 43000);
            const canUseAssignmentEndpoint = !!(
                assignmentContext &&
                assignmentContext.assignment_id &&
                assignmentContext.role === 'editor' &&
                assignmentContext.token
            );

            let response = null;
            if (canUseAssignmentEndpoint) {
                const sessionId = getAssignmentSessionId();
                if (!sessionId) throw new Error('Missing assignment session id.');
                response = await fetchAssignmentPost('createSnapshot', {
                    assignment_id: assignmentContext.assignment_id,
                    token: assignmentContext.token,
                    session_id: sessionId,
                    agent_email: getLoggedInEmail(),
                    app_base: getCurrentAppBaseUrl(),
                    snapshot_payload: payload
                });
            } else {
                response = await fetchAssignmentPost('createScenarioSnapshot', {
                    app_base: getCurrentAppBaseUrl(),
                    agent_email: getLoggedInEmail(),
                    agent_name: localStorage.getItem('agentName') || '',
                    snapshot_payload: payload
                });
            }

            const shareUrl = String((response && response.share_url) || '').trim();
            if (!shareUrl) throw new Error('Snapshot URL was not returned.');

            const copied = await copyTextToClipboard(shareUrl);
            if (copied) {
                const successMessage = `Snapshot link copied. Expires ${formatSnapshotExpiry(response && response.expires_at)}.`;
                setAssignmentsStatus(successMessage, false);
                if (!isElementVisible(assignmentsStatus)) {
                    showTransientTopNotice(successMessage, false);
                }
            } else {
                const fallbackMessage = `Snapshot created but copy failed. Link: ${shareUrl}`;
                setAssignmentsStatus(fallbackMessage, true);
                showTransientTopNotice('Snapshot created. Copy failed, link printed in console.', true);
                console.log('Snapshot link:', shareUrl);
            }
        } catch (error) {
            const errorMessage = `Snapshot failed: ${error.message || error}`;
            setAssignmentsStatus(errorMessage, true);
            showTransientTopNotice(errorMessage, true);
            debugLog('Snapshot creation failed', String((error && error.message) || error || ''));
        } finally {
            snapshotCreateInFlight = false;
            if (snapshotShareBtn) snapshotShareBtn.disabled = false;
        }
    }

    function enterSnapshotModeUi() {
        isSnapshotMode = true;
        stopAssignmentHeartbeat();
        assignmentQueue = [];
        assignmentContext = null;
        assignmentSessionState = null;
        setAssignmentSessionUiLocked(false);
        document.body.classList.remove('assignment-view-only');
        document.body.classList.add('snapshot-share-view');
        updateSnapshotShareButtonVisibility();
    }

    function setSnapshotErrorState(message) {
        const text = String(message || 'This snapshot link is invalid or expired.');
        if (chatMessages) {
            chatMessages.innerHTML = '';
            addSystemStatusMessage(text);
        }
        if (internalNotesEl) {
            internalNotesEl.value = '';
            internalNotesEl.readOnly = true;
            internalNotesEl.disabled = false;
        }
        setAssignmentsStatus(text, true);
    }

    async function loadSnapshotFromLink(snapshotId, snapshotToken) {
        enterSnapshotModeUi();
        try {
            const response = await fetchAssignmentGet('getSnapshot', {
                snapshot_id: snapshotId,
                snapshot_token: snapshotToken
            });
            const snapshot = response && response.snapshot ? response.snapshot : null;
            if (!snapshot || !snapshot.payload || typeof snapshot.payload !== 'object') {
                throw new Error('Snapshot payload is missing.');
            }

            const payload = snapshot.payload;
            const scenarioRaw = payload.scenario && typeof payload.scenario === 'object' ? payload.scenario : {};
            const snapshotScenario = normalizeScenarioRecord(scenarioRaw, {}, 'snapshot');
            const payloadTemplates = Array.isArray(payload.templates) ? payload.templates : [];
            if (!snapshotScenario.rightPanel || typeof snapshotScenario.rightPanel !== 'object') {
                snapshotScenario.rightPanel = {};
            }
            const scenarioTemplates = Array.isArray(snapshotScenario.rightPanel.templates)
                ? snapshotScenario.rightPanel.templates
                : [];
            let templates = payloadTemplates.length ? payloadTemplates : scenarioTemplates;
            if (!templates.length) {
                try {
                    templates = await loadTemplatesData();
                    debugLog('Snapshot templates loaded from fallback source', { count: templates.length });
                } catch (fallbackError) {
                    debugLog('Snapshot template fallback failed', String((fallbackError && fallbackError.message) || fallbackError || ''));
                }
            }
            snapshotScenario.rightPanel.templates = templates;
            if (!snapshotScenario.id) {
                snapshotScenario.id = String(payload.send_id || '');
            }

            const hasBrowsingHistory = !!(
                (Array.isArray(snapshotScenario.rightPanel.browsingHistory) && snapshotScenario.rightPanel.browsingHistory.length) ||
                (Array.isArray(snapshotScenario.rightPanel.browsing_history) && snapshotScenario.rightPanel.browsing_history.length) ||
                (Array.isArray(snapshotScenario.rightPanel.last5Products) && snapshotScenario.rightPanel.last5Products.length) ||
                (Array.isArray(snapshotScenario.rightPanel.last_5_products) && snapshotScenario.rightPanel.last_5_products.length)
            );
            if (!hasBrowsingHistory) {
                const snapshotSendId = String(payload.send_id || snapshotScenario.id || '').trim();
                if (snapshotSendId) {
                    try {
                        const snapshotScenarioKey = await resolveScenarioKeyForSendId(snapshotSendId, allScenariosData);
                        if (snapshotScenarioKey) {
                            await ensureScenariosLoaded([snapshotScenarioKey]);
                            const fallbackScenario = allScenariosData && allScenariosData[snapshotScenarioKey];
                            const fallbackRightPanel = fallbackScenario && fallbackScenario.rightPanel && typeof fallbackScenario.rightPanel === 'object'
                                ? fallbackScenario.rightPanel
                                : null;
                            if (fallbackRightPanel) {
                                snapshotScenario.rightPanel = Object.assign({}, fallbackRightPanel, snapshotScenario.rightPanel);
                                snapshotScenario.rightPanel.templates = templates;
                                debugLog('Snapshot browsing history restored from scenario source', {
                                    sendId: snapshotSendId,
                                    scenarioKey: snapshotScenarioKey
                                });
                            }
                        }
                    } catch (historyFallbackError) {
                        debugLog('Snapshot browsing history fallback failed', String((historyFallbackError && historyFallbackError.message) || historyFallbackError || ''));
                    }
                }
            }

            snapshotContext = {
                snapshot_id: String(snapshot.snapshot_id || ''),
                expires_at: String(snapshot.expires_at || '')
            };
            templatesData = templates;
            allScenariosData = { snapshot: snapshotScenario };
            loadScenarioContent('snapshot', allScenariosData);

            if (internalNotesEl) {
                internalNotesEl.value = String(payload.internal_note || '');
                internalNotesEl.readOnly = true;
                internalNotesEl.disabled = false;
            }
            setAssignmentsStatus(`Snapshot view${snapshotContext.expires_at ? ` (expires ${formatSnapshotExpiry(snapshotContext.expires_at)})` : ''}.`, false);
            return true;
        } catch (error) {
            console.error('Snapshot load failed:', error);
            setSnapshotErrorState(error && error.message ? error.message : 'This snapshot link is invalid or expired.');
            return false;
        }
    }

    function renderAssignmentQueue(assignments) {
        const rawQueue = Array.isArray(assignments) ? assignments : [];
        const buildQueue = (applyLocalSkipFilter) => {
            const seenAssignmentIds = new Set();
            const seenSendIds = new Set();
            let dedupedCountLocal = 0;
            const nextQueue = rawQueue.filter((item) => {
                const entry = item || {};
                const assignmentId = String(entry.assignment_id || '').trim();
                const sendId = String(entry.send_id || '').trim();
                if (assignmentId && isAssignmentPendingDone(assignmentId)) {
                    return false;
                }
                if (assignmentId && seenAssignmentIds.has(assignmentId)) {
                    dedupedCountLocal++;
                    return false;
                }
                if (sendId && seenSendIds.has(sendId)) {
                    dedupedCountLocal++;
                    return false;
                }
                if (IGNORED_SEND_IDS.has(sendId)) {
                    markAssignmentLocallySkipped(assignmentId, 'ignored_send_id');
                    return false;
                }
                if (assignmentId) seenAssignmentIds.add(assignmentId);
                if (sendId) seenSendIds.add(sendId);
                if (applyLocalSkipFilter) {
                    return !isAssignmentLocallySkipped(assignmentId);
                }
                return true;
            });
            return { nextQueue, dedupedCountLocal };
        };

        let { nextQueue, dedupedCountLocal } = buildQueue(true);
        if (!nextQueue.length && rawQueue.length && locallySkippedAssignmentIds.size) {
            debugLog('Recovered assignment queue after clearing local skips', {
                rawTotal: rawQueue.length,
                locallySkipped: locallySkippedAssignmentIds.size
            });
            locallySkippedAssignmentIds.clear();
            const rebuilt = buildQueue(false);
            nextQueue = rebuilt.nextQueue;
            dedupedCountLocal = rebuilt.dedupedCountLocal;
        }
        assignmentQueue = nextQueue;
        debugLog('Rendered assignment queue', {
            total: rawQueue.length,
            active: assignmentQueue.length,
            locallySkipped: locallySkippedAssignmentIds.size,
            deduped: dedupedCountLocal
        });
        pruneAssignmentResponseCacheToQueue(assignmentQueue);
        prefetchAssignmentDetailsInBackground(assignmentQueue);
        if (!assignmentSelect) return;

        assignmentSelect.innerHTML = '';
        if (!assignmentQueue.length) {
            const emptyOption = document.createElement('option');
            emptyOption.value = '';
            emptyOption.textContent = 'No active assignments';
            assignmentSelect.appendChild(emptyOption);
            return;
        }

        assignmentQueue.forEach((assignment) => {
            const option = document.createElement('option');
            option.value = assignment.assignment_id || '';
            option.textContent = `${assignment.send_id || assignment.assignment_id} (${assignment.status || 'ASSIGNED'})`;
            option.dataset.editUrl = assignment.edit_url || '';
            option.dataset.viewUrl = assignment.view_url || '';
            assignmentSelect.appendChild(option);
        });
    }

    function selectCurrentAssignmentInQueue() {
        if (!assignmentContext || !assignmentSelect) return;
        const currentAid = assignmentContext.assignment_id;
        if (!currentAid) return;
        for (let i = 0; i < assignmentSelect.options.length; i++) {
            if (assignmentSelect.options[i].value === currentAid) {
                assignmentSelect.selectedIndex = i;
                return;
            }
        }
    }

    async function openSelectedAssignmentFromList() {
        if (isSnapshotMode) return;
        if (!assignmentSelect || !assignmentSelect.value) return;
        if (isAssignmentPendingDone(assignmentSelect.value)) {
            setAssignmentsStatus('That conversation is still syncing in the background.', true);
            return;
        }
        const selectedOption = assignmentSelect.options[assignmentSelect.selectedIndex];
        if (!selectedOption) return;
        const prefersViewUrl = !!(assignmentContext && (assignmentContext.role === 'viewer' || assignmentContext.mode === 'view'));
        const url = getAssignmentUrlFromQueueItem({
            edit_url: selectedOption.dataset.editUrl || '',
            view_url: selectedOption.dataset.viewUrl || ''
        }, { prefersView: prefersViewUrl });
        if (!url) return;
        const opened = await openAssignmentInPageByUrl(url, {
            updateHistory: true,
            replaceHistory: false,
            refreshQueue: false
        });
        if (!opened) {
            setAssignmentsStatus('Unable to open selected assignment in-page.', true);
        }
    }

    function findScenarioBySendId(scenarios, sendId) {
        const target = String(sendId || '').trim();
        const entries = Object.entries(scenarios || {});
        for (let i = 0; i < entries.length; i++) {
            const [scenarioKey, scenario] = entries[i];
            if (String((scenario && scenario.id) || '').trim() === target) {
                return { scenarioKey, scenario };
            }
        }
        return null;
    }

    function assignmentNotesStorageKey() {
        if (!assignmentContext || !assignmentContext.assignment_id) return '';
        return `internalNotes_assignment_${assignmentContext.assignment_id}`;
    }

    function assignmentFormStateStorageKey() {
        if (!assignmentContext || !assignmentContext.assignment_id) return '';
        return `customFormState_assignment_${assignmentContext.assignment_id}`;
    }

    function buildAssignmentContextRecord(assignment, params, scenarioKey) {
        return {
            assignment_id: assignment.assignment_id,
            send_id: assignment.send_id,
            role: assignment.role === 'viewer' ? 'viewer' : 'editor',
            mode: (params && params.mode) ? params.mode : getPageModeFromUrl(),
            token: (params && params.token) ? params.token : '',
            status: assignment.status || '',
            scenarioKey: String(scenarioKey || ''),
            form_state_json: assignment.form_state_json || '',
            internal_note: assignment.internal_note || ''
        };
    }

    function buildAssignmentPageUrl(params, scenarioKey) {
        const query = new URLSearchParams();
        query.set('aid', String(params && params.aid ? params.aid : ''));
        query.set('token', String(params && params.token ? params.token : ''));
        query.set('mode', String(params && params.mode ? params.mode : 'edit'));
        if (scenarioKey) {
            query.set('scenario', String(scenarioKey));
            const scenario = allScenariosData && allScenariosData[String(scenarioKey)];
            const scenarioId = scenario && scenario.id ? String(scenario.id).trim() : '';
            if (scenarioId) {
                query.set('sid', scenarioId);
            }
        }
        return `app.html?${query.toString()}`;
    }

    async function getScenarioKeysForAssignmentWindow(targetAssignmentId) {
        const queue = (Array.isArray(assignmentQueue) ? assignmentQueue : [])
            .filter(item => !isAssignmentPendingDone(item && item.assignment_id))
            .filter(item => !isAssignmentLocallySkipped(item && item.assignment_id));
        if (!queue.length) return [];
        const targetId = String(targetAssignmentId || (assignmentContext && assignmentContext.assignment_id) || '').trim();
        const currentIndex = targetId
            ? queue.findIndex(item => String((item && item.assignment_id) || '') === targetId)
            : 0;
        const resolvedIndex = currentIndex >= 0 ? currentIndex : 0;
        const windowAssignments = getCenteredWindowItems(queue, resolvedIndex, 5);
        const sendIds = windowAssignments
            .map((assignmentItem) => {
                const item = assignmentItem || {};
                const assignmentId = String(item.assignment_id || '').trim();
                let sendId = String(item.send_id || '').trim();
                if (!sendId && assignmentContext && assignmentId && String(assignmentContext.assignment_id) === assignmentId) {
                    sendId = String(assignmentContext.send_id || '').trim();
                }
                return sendId;
            })
            .filter(Boolean);

        const keys = await Promise.all(sendIds.map(sendId => resolveScenarioKeyForSendId(sendId, allScenariosData, {
            allowMonolithFallback: false
        })));
        return Array.from(new Set(keys.map(key => String(key || '').trim()).filter(Boolean)));
    }

    async function prefetchAssignmentWindow(targetAssignmentId) {
        const dedupeKey = String(targetAssignmentId || '').trim() || '__default__';
        if (assignmentWindowPrefetchPromises[dedupeKey]) {
            return assignmentWindowPrefetchPromises[dedupeKey];
        }
        assignmentWindowPrefetchPromises[dedupeKey] = (async () => {
            const scenarioKeys = await getScenarioKeysForAssignmentWindow(targetAssignmentId);
            if (!scenarioKeys.length) return;
            await ensureScenariosLoaded(scenarioKeys, { allowMonolithFallback: false });
            await ensureTemplatesLoadedForScenarioKeys(scenarioKeys);
        })();

        try {
            await assignmentWindowPrefetchPromises[dedupeKey];
        } finally {
            delete assignmentWindowPrefetchPromises[dedupeKey];
        }
    }

    function prefetchAssignmentWindowInBackground(targetAssignmentId) {
        prefetchAssignmentWindow(targetAssignmentId).catch((error) => {
            console.warn('Assignment prefetch window failed:', error);
        });
    }

    function queueBackgroundSkipInvalidAssignment(assignment, params, reason) {
        const sessionId = getAssignmentSessionId({ createIfMissing: true });
        const assignmentId = String(assignment && assignment.assignment_id || '').trim();
        const token = String(params && params.token || '').trim();
        if (!sessionId || !assignmentId || !token) return;
        fetchAssignmentPost('skipAssignment', {
            assignment_id: assignmentId,
            token,
            session_id: sessionId,
            app_base: getCurrentAppBaseUrl(),
            reason: String(reason || 'missing_scenario')
        }).then((skipRes) => {
            applyAssignmentSessionState(skipRes && skipRes.session, { silent: true });
            const nextQueue = Array.isArray(skipRes && skipRes.assignments) ? skipRes.assignments : [];
            pruneLocallySkippedAssignments(nextQueue);
            renderAssignmentQueue(nextQueue);
        }).catch((error) => {
            debugLog('skipAssignment backend call failed (background mode)', {
                assignmentId,
                error: String((error && error.message) || error || '')
            });
            // Try queue refresh so user can continue even if skip endpoint had a transient failure.
            refreshAssignmentQueue().catch(() => {});
        });
    }

    async function skipInvalidAssignmentAndRefresh(assignment, params, reason, options = {}) {
        const sessionId = getAssignmentSessionId({ createIfMissing: true });
        if (!sessionId) throw new Error('Missing assignment session id.');
        const assignmentId = String(assignment && assignment.assignment_id || '').trim();
        const token = String(params && params.token || '').trim();
        if (!assignmentId || !token) {
            throw new Error('Cannot skip assignment without assignment id and token.');
        }
        markAssignmentLocallySkipped(assignmentId, reason || 'missing_scenario');
        const waitForServer = options.waitForServer === true;
        if (!waitForServer) {
            const fastQueue = (Array.isArray(assignmentQueue) ? assignmentQueue : [])
                .filter(item => String((item && item.assignment_id) || '').trim() !== assignmentId);
            renderAssignmentQueue(fastQueue);
            queueBackgroundSkipInvalidAssignment(assignment, params, reason);
            return fastQueue;
        }
        try {
            const skipRes = await fetchAssignmentPost('skipAssignment', {
                assignment_id: assignmentId,
                token,
                session_id: sessionId,
                app_base: getCurrentAppBaseUrl(),
                reason: String(reason || 'missing_scenario')
            });
            applyAssignmentSessionState(skipRes && skipRes.session, { silent: true });
            const nextQueue = Array.isArray(skipRes && skipRes.assignments) ? skipRes.assignments : [];
            pruneLocallySkippedAssignments(nextQueue);
            renderAssignmentQueue(nextQueue);
            return nextQueue;
        } catch (error) {
            debugLog('skipAssignment backend call failed; using local skip fallback', {
                assignmentId,
                error: String((error && error.message) || error || '')
            });
            const fallbackQueue = (Array.isArray(assignmentQueue) ? assignmentQueue : [])
                .filter(item => String((item && item.assignment_id) || '').trim() !== assignmentId);
            renderAssignmentQueue(fallbackQueue);
            return fallbackQueue;
        }
    }

    async function applyAssignmentContextToUi(options = {}) {
        if (!assignmentContext || !assignmentContext.scenarioKey) return;

        await ensureScenariosLoaded([assignmentContext.scenarioKey], { allowMonolithFallback: false });
        await ensureTemplatesLoadedForScenarioKeys([assignmentContext.scenarioKey]);
        if (!allScenariosData || !allScenariosData[assignmentContext.scenarioKey]) {
            throw new Error(`Scenario ${assignmentContext.scenarioKey} is unavailable in runtime chunks.`);
        }

        setCurrentScenarioNumber(assignmentContext.scenarioKey);
        loadScenarioContent(assignmentContext.scenarioKey, allScenariosData || {});

        const customForm = document.getElementById('customForm');
        const formStatusEl = document.getElementById('formStatus');
        const serverFormState = parseStoredFormState(assignmentContext.form_state_json);
        const localFormStateKey = assignmentFormStateStorageKey();
        const localFormState = localFormStateKey ? parseStoredFormState(localStorage.getItem(localFormStateKey)) : null;
        applyDefaultCustomFormState(customForm);
        applyCustomFormState(customForm, serverFormState || localFormState);
        if (formStatusEl) {
            formStatusEl.textContent = '';
            formStatusEl.style.color = '';
        }

        if (internalNotesEl) {
            const notesKey = assignmentNotesStorageKey();
            const localNote = notesKey ? (localStorage.getItem(notesKey) || '') : '';
            internalNotesEl.value = assignmentContext.internal_note || localNote || '';
        }

        const forceView = assignmentContext.role === 'viewer' || assignmentContext.mode === 'view';
        setAssignmentReadOnlyState(forceView);
        setAssignmentsStatus(
            forceView
                ? `Opened ${assignmentContext.send_id} in view-only mode.`
                : `Opened ${assignmentContext.send_id} in editor mode.`,
            false
        );
        startAssignmentHeartbeat();
        selectCurrentAssignmentInQueue();
        updateSnapshotShareButtonVisibility();
        prefetchAssignmentDetailsInBackground(assignmentQueue);
        prefetchAssignmentWindowInBackground(assignmentContext.assignment_id);

        if (options.updateHistory) {
            const method = options.replaceHistory ? 'replaceState' : 'pushState';
            const nextUrl = buildAssignmentPageUrl(options.params || {}, assignmentContext.scenarioKey);
            window.history[method](
                {
                    aid: String((options.params && options.params.aid) || ''),
                    token: String((options.params && options.params.token) || ''),
                    mode: String((options.params && options.params.mode) || assignmentContext.mode || 'edit')
                },
                '',
                nextUrl
            );
        }
    }

    async function openAssignmentInPage(params, options = {}) {
        if (!params || !params.aid || !params.token) return false;
        if (!canUseAssignmentMode()) {
            setAssignmentsStatus('Assignment mode requires email login.', true);
            return false;
        }
        const openStartMs = Date.now();
        try {
            if (options.refreshQueue) {
                await refreshAssignmentQueue().catch(() => []);
            }
            const maxAttempts = Math.max(
                1,
                Number(options.maxAttempts) || (Array.isArray(assignmentQueue) && assignmentQueue.length ? assignmentQueue.length : 5)
            );
            let currentParams = {
                aid: String(params.aid || ''),
                token: String(params.token || ''),
                mode: String(params.mode || 'edit')
            };

            for (let attempt = 0; attempt < maxAttempts; attempt++) {
                const sessionId = getAssignmentSessionId({ createIfMissing: true });
                if (!sessionId) throw new Error('Missing assignment session id.');
                let response = null;
                try {
                    response = await fetchAssignmentResponseForParams(currentParams, sessionId, { useCache: true });
                } catch (fetchError) {
                    const message = String((fetchError && fetchError.message) || fetchError || '');
                    const recoverableReservationError =
                        message.toLowerCase().includes('not reserved for this session') ||
                        message.toLowerCase().includes('session not found');
                    if (recoverableReservationError) {
                        debugLog('Assignment/session mismatch detected; refreshing queue', {
                            aid: currentParams.aid,
                            message
                        });
                        const refreshedQueue = await refreshAssignmentQueue().catch(() => []);
                        const nextParamsFromRefreshedQueue = getNextAssignmentParamsFromQueue(refreshedQueue, {
                            excludeAssignmentIds: [],
                            prefersView: currentParams.mode === 'view'
                        });
                        if (nextParamsFromRefreshedQueue) {
                            currentParams = nextParamsFromRefreshedQueue;
                            continue;
                        }
                    }
                    throw fetchError;
                }
                debugLog('openAssignment attempt', {
                    attempt: attempt + 1,
                    maxAttempts,
                    aid: currentParams.aid,
                    mode: currentParams.mode
                });
                applyAssignmentSessionState(response && response.session, { silent: true });
                const assignment = response && response.assignment ? response.assignment : null;
                if (!assignment) throw new Error('Assignment payload is missing.');
                if (IGNORED_SEND_IDS.has(String(assignment.send_id || '').trim())) {
                    const nextQueue = await skipInvalidAssignmentAndRefresh(
                        assignment,
                        currentParams,
                        'ignored_send_id'
                    );
                    const nextParams = getNextAssignmentParamsFromQueue(nextQueue, {
                        excludeAssignmentIds: [assignment.assignment_id],
                        prefersView: currentParams.mode === 'view'
                    });
                    if (!nextParams) {
                        setAssignmentsStatus('No valid assignment available after removing ignored send_id.', true);
                        return false;
                    }
                    currentParams = nextParams;
                    continue;
                }

                if (isAssignmentPendingDone(assignment.assignment_id)) {
                    const nextPendingParams = getNextAssignmentParamsFromQueue(assignmentQueue, {
                        excludeAssignmentIds: [assignment.assignment_id],
                        prefersView: currentParams.mode === 'view'
                    });
                    if (!nextPendingParams) {
                        setAssignmentsStatus('Submission is still syncing in background. Please wait a moment.', true);
                        return false;
                    }
                    currentParams = nextPendingParams;
                    continue;
                }

                const scenarioKey = await resolveScenarioKeyForSendId(
                    assignment.send_id,
                    allScenariosData,
                    { allowMonolithFallback: true }
                );
                if (!scenarioKey) {
                    const missingScenarioMessage = `Scenario for send_id ${assignment.send_id} was not found in runtime chunks.`;
                    if (assignment.role === 'editor') {
                        const nextQueue = await skipInvalidAssignmentAndRefresh(
                            assignment,
                            currentParams,
                            'missing_scenario'
                        );
                        const nextParams = getNextAssignmentParamsFromQueue(nextQueue, {
                            excludeAssignmentIds: [assignment.assignment_id],
                            prefersView: currentParams.mode === 'view'
                        });
                        if (!nextParams) {
                            assignmentContext = null;
                            updateSnapshotShareButtonVisibility();
                            setAssignmentsStatus(
                                'Assigned items are out of sync with scenarios. Queue refreshed; no valid conversation available.',
                                true
                            );
                            return false;
                        }
                        currentParams = nextParams;
                        continue;
                    }
                    markAssignmentLocallySkipped(assignment.assignment_id, 'missing_scenario_view_only');
                    const nextParams = getNextAssignmentParamsFromQueue(assignmentQueue, {
                        excludeAssignmentIds: [assignment.assignment_id],
                        prefersView: currentParams.mode === 'view'
                    });
                    if (!nextParams) {
                        assignmentContext = null;
                        updateSnapshotShareButtonVisibility();
                        setAssignmentsStatus(
                            missingScenarioMessage,
                            true
                        );
                        return false;
                    }
                    currentParams = nextParams;
                    continue;
                }

                assignmentContext = buildAssignmentContextRecord(assignment, currentParams, scenarioKey);
                await applyAssignmentContextToUi({
                    updateHistory: !!options.updateHistory,
                    replaceHistory: !!options.replaceHistory,
                    params: currentParams
                });
                debugLog('openAssignment success', {
                    aid: assignment.assignment_id,
                    send_id: assignment.send_id,
                    elapsedMs: Date.now() - openStartMs
                });
                return true;
            }

            throw new Error('No valid assignment could be opened from the current queue.');
        } catch (error) {
            console.error('Assignment open failed:', error);
            setAssignmentsStatus(`Assignment error: ${error.message || error}`, true);
            return false;
        }
    }

    async function openAssignmentInPageByUrl(url, options = {}) {
        const params = getAssignmentParamsFromHref(url);
        if (!params) return false;
        return openAssignmentInPage(params, options);
    }

    function setAssignmentReadOnlyState(isReadOnly) {
        if (isSnapshotMode) return;
        const effectiveReadOnly = !!isReadOnly;
        const isAssignmentViewMode = !!(
            isReadOnly &&
            assignmentContext &&
            (assignmentContext.role === 'viewer' || assignmentContext.mode === 'view')
        );
        document.body.classList.toggle('assignment-view-only', isAssignmentViewMode);
        setAssignmentSessionUiLocked(false);

        const customForm = document.getElementById('customForm');
        const formSubmitBtn = document.getElementById('formSubmitBtn');
        const clearFormBtn = document.getElementById('clearFormBtn');
        if (customForm) {
            const controls = customForm.querySelectorAll('input, select, textarea, button');
            controls.forEach((el) => {
                if (el.id === 'clearFormBtn') return;
                el.disabled = effectiveReadOnly;
            });
        }
        if (formSubmitBtn) formSubmitBtn.disabled = effectiveReadOnly;
        if (clearFormBtn) clearFormBtn.disabled = effectiveReadOnly;
        if (internalNotesEl) internalNotesEl.disabled = effectiveReadOnly;
        if (previousConversationBtn) previousConversationBtn.disabled = effectiveReadOnly;
        if (nextConversationBtn) nextConversationBtn.disabled = effectiveReadOnly;
    }

    function parseStoredFormState(raw) {
        if (!raw) return null;
        try {
            return JSON.parse(raw);
        } catch (_) {
            return null;
        }
    }

    function applyDefaultCustomFormState(customForm) {
        if (!customForm) return;
        const checkboxInputs = customForm.querySelectorAll('input[type="checkbox"]');
        checkboxInputs.forEach((cb) => {
            cb.checked = true;
            cb.defaultChecked = true;
        });

        const zeroToleranceSelect = customForm.querySelector('#zeroTolerance');
        if (zeroToleranceSelect && zeroToleranceSelect.options.length) {
            zeroToleranceSelect.selectedIndex = 0;
        }

        const notesField = customForm.querySelector('#notes');
        if (notesField) {
            notesField.value = '';
        }
    }

    function applyCustomFormState(customForm, parsedState) {
        if (!customForm || !parsedState || typeof parsedState !== 'object') return;
        const formElements = customForm.elements;
        for (let i = 0; i < formElements.length; i++) {
            const el = formElements[i];
            if (el.type === 'checkbox') {
                const key = `${el.name}::${el.value}`;
                if (Object.prototype.hasOwnProperty.call(parsedState, key)) {
                    el.checked = !!parsedState[key];
                }
            } else if (el.tagName === 'SELECT' || el.tagName === 'TEXTAREA' || el.type === 'text') {
                const key = el.name || el.id;
                if (Object.prototype.hasOwnProperty.call(parsedState, key)) {
                    el.value = parsedState[key];
                }
            }
        }
    }

    function collectCustomFormState(customForm) {
        const state = {};
        if (!customForm) return state;
        const formElements = customForm.elements;
        for (let i = 0; i < formElements.length; i++) {
            const el = formElements[i];
            if (!el.name && !el.id) continue;
            if (el.type === 'checkbox') {
                state[`${el.name}::${el.value}`] = el.checked;
            } else if (el.tagName === 'SELECT' || el.tagName === 'TEXTAREA' || el.type === 'text') {
                const key = el.name || el.id;
                state[key] = el.value;
            }
        }
        return state;
    }

    async function saveAssignmentDraft(customForm) {
        if (isSnapshotMode) return;
        if (!assignmentContext || assignmentContext.role !== 'editor') return;
        if (!assignmentContext.assignment_id || !assignmentContext.token) return;
        const sessionId = getAssignmentSessionId();
        if (!sessionId) return;

        const formState = collectCustomFormState(customForm);
        const notesValue = internalNotesEl ? internalNotesEl.value : '';
        const formStateRaw = JSON.stringify(formState);

        const formKey = assignmentFormStateStorageKey();
        if (formKey) localStorage.setItem(formKey, formStateRaw);
        const notesKey = assignmentNotesStorageKey();
        if (notesKey) localStorage.setItem(notesKey, notesValue || '');

        const response = await fetchAssignmentPost('saveDraft', {
            assignment_id: assignmentContext.assignment_id,
            token: assignmentContext.token,
            session_id: sessionId,
            form_state_json: formStateRaw,
            internal_note: notesValue
        }, { timeoutMs: ASSIGNMENT_DRAFT_TIMEOUT_MS });
        applyAssignmentSessionState(response && response.session, { silent: true });
    }

    function scheduleAssignmentDraftSave(customForm) {
        if (isSnapshotMode) return;
        if (!assignmentContext || assignmentContext.role !== 'editor') return;
        if (draftSaveTimer) {
            clearTimeout(draftSaveTimer);
        }
        draftSaveTimer = setTimeout(async () => {
            try {
                await saveAssignmentDraft(customForm);
                setAssignmentsStatus('Draft saved.', false);
            } catch (error) {
                console.error('Draft save failed:', error);
                setAssignmentsStatus(`Draft save failed: ${error.message || error}`, true);
            }
        }, 1200);
    }

    function getCompanyInitial(companyName) {
        const name = String(companyName || '').trim();
        return name ? name.charAt(0).toUpperCase() : '';
    }

    function mapMessageTypeToRole(messageType) {
        const type = String(messageType || '').trim().toLowerCase();
        if (type === 'subscriber' || type === 'customer' || type === 'user') return 'customer';
        if (type === 'agent') return 'agent';
        if (type === 'assistant' || type === 'support' || type === 'csr' || type === 'rep') return 'agent';
        if (type === 'template' || type === 'escalation') return 'system';
        if (type === 'system') return 'system';
        return '';
    }

    function normalizeMessageMedia(media) {
        const cleanMediaUrl = (value) => {
            let url = String(value || '').trim();
            url = url.replace(/\\"/g, '"').replace(/\\'/g, "'");
            if (
                url.length >= 2 &&
                ((url.startsWith('"') && url.endsWith('"')) || (url.startsWith("'") && url.endsWith("'")))
            ) {
                url = url.slice(1, -1).trim();
            }
            url = url.replace(/;(name|filename|type)\s*=\s*%22([^%]*)%22/gi, ';$1="$2"');
            if (!url) return '';
            if (/(smil(\.xml)?|application\/smil)/i.test(url)) return '';
            if (/;(?:name|filename|type)\s*=\s*"[^"]*$/i.test(url)) {
                url = `${url}"`;
            }
            return url;
        };
        const extractMediaUrls = (value) => {
            const text = String(value || '').trim();
            if (!text) return [];
            try {
                const parsed = JSON.parse(text);
                if (Array.isArray(parsed)) {
                    return parsed.map(cleanMediaUrl).filter(Boolean);
                }
            } catch (_) {
                // Fall through to URL extraction
            }
            const searchText = text.replace(/\\"/g, '"').replace(/\\'/g, "'");
            const matches = searchText.match(/https?:\/\/[^\s;"'<>]+(?:\s*;\s*[a-z0-9_-]+\s*=\s*(?:"[^"]*"|'[^']*'|[^;\s"'<>]+))*/gi);
            if (!matches || !matches.length) return [];
            return matches.map(cleanMediaUrl).filter(Boolean);
        };
        if (!Array.isArray(media)) return [];
        return media
            .map(item => {
                if (typeof item === 'string') return extractMediaUrls(item);
                if (item && typeof item === 'object' && typeof item.url === 'string') return extractMediaUrls(item.url);
                return [];
            })
            .flat()
            .filter(Boolean);
    }

    function normalizeConversationMessage(message) {
        if (!message || typeof message !== 'object') return null;

        const explicitRole = String(message.role || '').trim().toLowerCase();
        const messageType = String(message.message_type || '').trim().toLowerCase();
        const mappedRole = mapMessageTypeToRole(messageType);
        const agentIdentifier = String(
            message.agent != null
                ? message.agent
                : (message.agent_id != null ? message.agent_id : (message.agentId != null ? message.agentId : ''))
        ).trim();
        const role = explicitRole || mappedRole || (agentIdentifier ? 'agent' : '');
        if (!role) return null;

        const contentRaw = message.content != null ? message.content : message.message_text;
        let content = typeof contentRaw === 'string' ? contentRaw : (contentRaw == null ? '' : String(contentRaw));
        const contentTrimmed = content.trim();
        if (messageType === 'template' && contentTrimmed && !/^template used:/i.test(contentTrimmed)) {
            content = `Template Used: ${contentTrimmed}`;
        } else if (messageType === 'escalation' && contentTrimmed && !/^conversation escalated:/i.test(contentTrimmed)) {
            content = `Conversation escalated: ${contentTrimmed}`;
        }
        if (!content.trim()) return null;

        const normalized = { role, content };
        if (messageType) normalized.message_type = messageType;
        if (agentIdentifier) normalized.agent = agentIdentifier;
        const media = normalizeMessageMedia(message.media || message.message_media);
        if (media.length) normalized.media = media;
        const dateTimeValue = String(
            message.date_time != null
                ? message.date_time
                : (message.dateTime != null ? message.dateTime : (message.timestamp != null ? message.timestamp : (message.created_at != null ? message.created_at : '')))
        ).trim();
        if (dateTimeValue) normalized.date_time = dateTimeValue;

        const id = String(message.id || message.message_id || '').trim();
        if (id) normalized.id = id;

        return normalized;
    }

    function isConversationMessageObject(item) {
        if (!item || typeof item !== 'object') return false;
        return (
            Object.prototype.hasOwnProperty.call(item, 'message_text') ||
            Object.prototype.hasOwnProperty.call(item, 'message_type') ||
            Object.prototype.hasOwnProperty.call(item, 'content') ||
            Object.prototype.hasOwnProperty.call(item, 'role')
        );
    }

    function isMessageArray(value) {
        return Array.isArray(value) && value.length > 0 && value.every(isConversationMessageObject);
    }

    function normalizeConversationList(list) {
        if (!Array.isArray(list)) return [];
        return list
            .map(normalizeConversationMessage)
            .filter(Boolean);
    }

    function normalizeScenarioRecord(rawScenario, defaults, scenarioKey) {
        const scenarioObject = Array.isArray(rawScenario)
            ? { conversation: rawScenario }
            : ((rawScenario && typeof rawScenario === 'object') ? rawScenario : {});

        const scenarioNotes = (scenarioObject.notes || scenarioObject.guidelines) || {};
        const defaultNotes = (defaults.notes || defaults.guidelines) || {};
        const mergedScenario = {
            ...defaults,
            ...scenarioObject,
            guidelines: {
                ...(defaults.guidelines || {}),
                ...(scenarioObject.guidelines || {})
            },
            notes: {
                ...defaultNotes,
                ...scenarioNotes
            },
            rightPanel: {
                ...(defaults.rightPanel || {}),
                ...(scenarioObject.rightPanel || {})
            }
        };

        mergedScenario.has_shopify = scenarioHasShopify(mergedScenario);

        const preloadedConversation = Array.isArray(scenarioObject.conversation)
            ? scenarioObject.conversation
            : (Array.isArray(scenarioObject.messages) ? scenarioObject.messages : []);
        if (preloadedConversation.length) {
            mergedScenario.conversation = preloadedConversation;
        }

        mergedScenario.conversation = buildConversationFromScenario(mergedScenario);
        if (!mergedScenario.customerMessage) {
            const firstCustomer = mergedScenario.conversation.find(m => m && m.role === 'customer' && m.content);
            if (firstCustomer) mergedScenario.customerMessage = firstCustomer.content;
        }
        if (!mergedScenario.agentName) mergedScenario.agentName = '';
        if (!mergedScenario.companyName) mergedScenario.companyName = `Scenario ${scenarioKey}`;
        mergedScenario.agentInitial = getCompanyInitial(mergedScenario.companyName);
        return mergedScenario;
    }

    function coerceScenariosPayloadToMap(data) {
        const scenarios = {};
        const defaults = (data && typeof data === 'object' && !Array.isArray(data)) ? (data.defaults || {}) : {};

        const addScenario = (key, rawScenario) => {
            scenarios[String(key)] = normalizeScenarioRecord(rawScenario, defaults, String(key));
        };

        if (data && typeof data === 'object' && !Array.isArray(data) && data.scenarios && !Array.isArray(data.scenarios)) {
            Object.keys(data.scenarios).forEach(scenarioKey => {
                addScenario(scenarioKey, data.scenarios[scenarioKey]);
            });
            return scenarios;
        }

        const asArray = Array.isArray(data)
            ? data
            : ((data && typeof data === 'object' && Array.isArray(data.scenarios)) ? data.scenarios : null);

        if (!asArray) return scenarios;

        if (isMessageArray(asArray)) {
            addScenario('1', { conversation: asArray });
            return scenarios;
        }

        asArray.forEach((item, index) => {
            const key = String(index + 1);
            addScenario(key, item);
        });
        return scenarios;
    }

    function buildConversationFromScenario(scenario) {
        if (!scenario) return [];
        if (Array.isArray(scenario) && scenario.length) {
            return normalizeConversationList(scenario);
        }
        if (Array.isArray(scenario.conversation) && scenario.conversation.length) {
            return normalizeConversationList(scenario.conversation);
        }
        if (Array.isArray(scenario.messages) && scenario.messages.length) {
            return normalizeConversationList(scenario.messages);
        }
        const messages = [];
        const entries = Object.entries(scenario);
        entries.forEach(([key, value]) => {
            if (!value || typeof value !== 'string') return;
            if (/^SystemMessage\d+$/i.test(key)) {
                messages.push({ role: 'system', content: value });
            } else if (/^customerMessage\d*$/i.test(key)) {
                messages.push({ role: 'customer', content: value });
            } else if (/^AgentMessage\d+$/i.test(key)) {
                messages.push({ role: 'agent', content: value });
            }
        });
        return normalizeConversationList(messages);
    }

    function getFirstCustomerMessageFromScenario(scenario, conversation) {
        if (scenario && typeof scenario.customerMessage === 'string' && scenario.customerMessage.trim()) {
            return scenario.customerMessage;
        }
        const conv = Array.isArray(conversation) ? conversation : [];
        const firstCustomer = conv.find(m => m && m.role === 'customer' && m.content);
        return firstCustomer ? firstCustomer.content : '';
    }

    function normalizeScenarioLabelList(value) {
        if (Array.isArray(value)) {
            return value
                .map(item => String(item || '').trim())
                .filter(item => item && item !== '[object Object]')
                .map(item => item.replace(/^['"]|['"]$/g, ''))
                .filter(Boolean);
        }
        if (value && typeof value === 'object') {
            const objValues = Object.values(value)
                .map(item => String(item || '').trim())
                .filter(item => item && item !== '[object Object]')
                .map(item => item.replace(/^['"]|['"]$/g, ''))
                .filter(Boolean);
            return objValues;
        }
        if (value == null) return [];
        const text = String(value).trim();
        if (!text) return [];
        return text
            .split(/[\n,|;]+/)
            .map(item => item.trim().replace(/^['"]|['"]$/g, ''))
            .filter(Boolean);
    }

    function normalizeName(value) {
        return String(value || '').trim().toLowerCase();
    }

    function isGlobalTemplate(template) {
        return !normalizeName(template && template.companyName);
    }

    function scenarioHasShopify(scenario) {
        const rawFlag = scenario && Object.prototype.hasOwnProperty.call(scenario, 'has_shopify')
            ? scenario.has_shopify
            : scenario && scenario.hasShopify;
        const explicit = rawFlag === true || String(rawFlag || '').trim().toLowerCase() === 'true';
        if (explicit) return true;
        if (!scenario || typeof scenario !== 'object') return false;

        // Runtime chunk data can lag behind scenarios.json; infer from Shopify domains in scenario payload.
        const stack = [scenario];
        const seen = new Set();
        while (stack.length) {
            const current = stack.pop();
            if (!current) continue;
            if (typeof current === 'string') {
                if (current.toLowerCase().includes('.myshopify.com')) return true;
                continue;
            }
            if (Array.isArray(current)) {
                for (let i = 0; i < current.length; i++) stack.push(current[i]);
                continue;
            }
            if (typeof current === 'object') {
                if (seen.has(current)) continue;
                seen.add(current);
                Object.values(current).forEach(value => stack.push(value));
            }
        }
        return false;
    }

    function formatDollarAmount(rawValue) {
        if (rawValue == null) return '';
        const text = String(rawValue).trim();
        if (!text) return '';
        if (text.startsWith('$')) return text;
        return `$${text}`;
    }

    function firstNonEmptyValue(candidates) {
        const list = Array.isArray(candidates) ? candidates : [];
        for (let i = 0; i < list.length; i++) {
            const value = list[i];
            if (value == null) continue;
            const text = String(value).trim();
            if (text) return text;
        }
        return '';
    }

    function appendUploadedScenarios(scenarios, uploadedList) {
        const uploaded = Array.isArray(uploadedList) ? uploadedList : [];
        if (!uploaded.length) return scenarios;
        const keys = Object.keys(scenarios).map(k => parseInt(k, 10)).filter(n => !isNaN(n));
        let nextKey = keys.length ? Math.max(...keys) + 1 : 1;
        const existingIds = new Set(
            Object.values(scenarios)
                .map(item => String(item && item.id != null ? item.id : '').trim())
                .filter(Boolean)
        );
        uploaded.forEach(item => {
            const itemId = String(item && item.id != null ? item.id : '').trim();
            if (itemId && existingIds.has(itemId)) return;
            scenarios[String(nextKey)] = normalizeScenarioRecord(item, {}, String(nextKey));
            if (itemId) existingIds.add(itemId);
            nextKey++;
        });
        return scenarios;
    }

    async function loadTemplatesData() {
        if (window.location.protocol === 'file:') {
            return loadTemplatesDataMonolith();
        }

        const templateIndex = await loadRuntimeTemplatesIndex();
        if (templateIndex) {
            const globalLoaded = await ensureRuntimeTemplateGlobalLoaded();
            if (globalLoaded) {
                templatesData = Array.isArray(runtimeTemplateGlobalTemplates) ? runtimeTemplateGlobalTemplates.slice() : [];
                return templatesData;
            }
        }

        return loadTemplatesDataMonolith();
    }

    async function loadUploadedScenarios() {
        if (uploadedScenariosLoaded) {
            return Array.isArray(uploadedScenariosCache) ? uploadedScenariosCache : [];
        }
        if (uploadedScenariosLoadPromise) return uploadedScenariosLoadPromise;
        uploadedScenariosLoadPromise = (async () => {
            if (!GOOGLE_SCRIPT_URL) {
                uploadedScenariosLoaded = true;
                uploadedScenariosCache = [];
                return uploadedScenariosCache;
            }
            try {
                const url = `${GOOGLE_SCRIPT_URL}?action=getUploadedScenarios`;
                const response = await fetchJsonWithTimeout(url, { method: 'GET' }, ASSIGNMENT_GET_TIMEOUT_MS);
                if (!response.ok) throw new Error(`uploaded scenarios request failed (${response.status})`);
                const data = await response.json();
                uploadedScenariosCache = Array.isArray(data && data.scenarios) ? data.scenarios : [];
            } catch (error) {
                console.warn('Failed to load uploaded scenarios:', error);
                uploadedScenariosCache = [];
            } finally {
                uploadedScenariosLoaded = true;
                uploadedScenariosLoadPromise = null;
            }
            return uploadedScenariosCache;
        })();
        return uploadedScenariosLoadPromise;
    }

    function resolveScenarioKeyFromRuntimeIndex(runtimeIndex) {
        if (!runtimeIndex || typeof runtimeIndex !== 'object') return '';
        const byId = runtimeIndex.byId && typeof runtimeIndex.byId === 'object' ? runtimeIndex.byId : {};
        const byKey = runtimeIndex.byKey && typeof runtimeIndex.byKey === 'object' ? runtimeIndex.byKey : {};
        const order = Array.isArray(runtimeIndex.order)
            ? runtimeIndex.order.map(v => String(v || '').trim()).filter(Boolean)
            : [];

        const sid = getScenarioIdFromUrl();
        if (sid && byId[sid]) return String(byId[sid]);

        const fromUrl = getScenarioNumberFromUrl();
        if (fromUrl && byKey[fromUrl]) return fromUrl;

        const stored = String(localStorage.getItem('currentScenarioNumber') || '').trim();
        if (stored && byKey[stored]) return stored;

        return order.length ? order[0] : '';
    }

    function getTemplatesForScenario(scenario) {
        if (runtimeTemplatesIndex && runtimeTemplateGlobalLoaded && !runtimeTemplatesUnavailable) {
            const companyKey = normalizeName(scenario && scenario.companyName);
            const companyTemplates = companyKey && Array.isArray(runtimeTemplateCompanyCache[companyKey])
                ? runtimeTemplateCompanyCache[companyKey]
                : [];
            const globalTemplates = Array.isArray(runtimeTemplateGlobalTemplates) ? runtimeTemplateGlobalTemplates : [];
            const runtimeTemplates = companyTemplates.concat(globalTemplates);
            if (runtimeTemplates.length) {
                return runtimeTemplates;
            }
        }

        const sourceTemplates = Array.isArray(templatesData) && templatesData.length
            ? templatesData
            : (scenario.rightPanel && Array.isArray(scenario.rightPanel.templates) ? scenario.rightPanel.templates : []);

        if (!sourceTemplates.length) return [];

        const companyKey = normalizeName(scenario.companyName);
        const matching = [];
        const global = [];

        sourceTemplates.forEach(template => {
            const templateCompany = normalizeName(template && template.companyName);
            if (!templateCompany) {
                global.push(template);
            } else if (templateCompany === companyKey) {
                matching.push(template);
            }
        });

        return matching.concat(global);
    }

    
    // Load scenarios data
    async function loadScenariosData() {
        const mergeUploaded = async (baseScenarios) => {
            const target = (baseScenarios && typeof baseScenarios === 'object') ? baseScenarios : {};
            const uploaded = await loadUploadedScenarios();
            appendUploadedScenarios(target, uploaded);
            allScenariosData = target;
            return allScenariosData;
        };

        if (window.location.protocol === 'file:') {
            const scenarios = await loadScenariosDataMonolith();
            return mergeUploaded(scenarios);
        }

        const runtimeIndex = await loadRuntimeScenariosIndex();
        if (runtimeIndex) {
            const preferredScenarioKey = resolveScenarioKeyFromRuntimeIndex(runtimeIndex);
            if (preferredScenarioKey) {
                await ensureScenariosLoaded([preferredScenarioKey]);
                return mergeUploaded(allScenariosData || {});
            }
        }

        const fallbackScenarios = await loadScenariosDataMonolith();
        return mergeUploaded(fallbackScenarios);
    }
    
    // Helper function to get display name and icon for guideline categories
    function getCategoryInfo(categoryKey) {
        const categoryMap = {
            'send_to_cs': { 
                display: 'SEND TO CS', 
                icon: 'mail' 
            },
            'escalate': { 
                display: 'ESCALATE', 
                icon: 'arrow-up-circle' 
            },
            'tone': { 
                display: 'TONE', 
                icon: 'message-square' 
            },
            'templates': { 
                display: 'TEMPLATES', 
                icon: 'zap' 
            },
            'dos_and_donts': { 
                display: 'DOs AND DON\'Ts', 
                icon: 'check-square' 
            },
            'drive_to_purchase': { 
                display: 'DRIVE TO PURCHASE', 
                icon: 'shopping-cart' 
            },
            'promo_and_exclusions': { 
                display: 'PROMO & PROMO EXCLUSIONS', 
                icon: 'gift' 
            },
            'important': { 
                display: 'IMPORTANT', 
                icon: 'alert-circle' 
            }
        };
        
        // Return category info if found, otherwise default
        return categoryMap[categoryKey.toLowerCase()] || { 
            display: categoryKey.toUpperCase(), 
            icon: 'info' 
        };
    }

    function renderConversationMessages(conversation, scenario) {
        if (!chatMessages) return;
        chatMessages.innerHTML = '';
        const specialAgentIds = new Set(['62551', '38773']);

        const lastAgentConversationIndex = (() => {
            for (let i = conversation.length - 1; i >= 0; i--) {
                if (conversation[i] && conversation[i].role === 'agent' && conversation[i].content) return i;
            }
            return -1;
        })();

        const getPriorCustomerMessage = (index) => {
            for (let i = index - 1; i >= 0; i--) {
                const msg = conversation[i];
                if (msg && msg.role === 'customer' && msg.content) return msg.content;
            }
            return 'N/A';
        };

        const submitSelectedAgentMessage = async (message, index, checkbox) => {
            const agentUsername = localStorage.getItem('agentName') || 'Unknown Agent';
            const scenarioLabel = `Scenario ${currentScenario}`;
            const customerContext = getPriorCustomerMessage(index);
            const messageId = String((message && message.id) || '').trim();
            const uniquePart = messageId || String(index + 1);
            const sessionIdOverride = `${currentScenario}_selected_agent_${uniquePart}`;
            const timerValue = getCurrentTimerTime();
            checkbox.disabled = true;
            const ok = await sendToGoogleSheetsWithTimer(
                agentUsername,
                scenarioLabel,
                customerContext,
                message.content,
                timerValue,
                {
                    sessionIdOverride,
                    messageId
                }
            );
            if (!ok) {
                checkbox.disabled = false;
                checkbox.checked = false;
            }
        };

        const trimTrailingLinkPunctuation = (value) => {
            let text = String(value || '');
            if (!text) return { link: '', trailing: '' };
            let trailing = '';
            while (/[.,!?;:]+$/.test(text)) {
                trailing = text.slice(-1) + trailing;
                text = text.slice(0, -1);
            }
            while (/[)\]]$/.test(text)) {
                const closer = text.slice(-1);
                const opener = closer === ')' ? '(' : '[';
                const openCount = (text.match(new RegExp(`\\${opener}`, 'g')) || []).length;
                const closeCount = (text.match(new RegExp(`\\${closer}`, 'g')) || []).length;
                if (closeCount > openCount) {
                    trailing = closer + trailing;
                    text = text.slice(0, -1);
                    continue;
                }
                break;
            }
            return { link: text, trailing };
        };

        const appendLinkifiedText = (element, textValue) => {
            if (!element) return;
            const text = String(textValue || '');
            const linkPattern = /(?:https?:\/\/[^\s<>"']+|(?:[a-z0-9][a-z0-9.-]*\.attn\.tv(?:\/[^\s<>"']*)?))/gi;
            let cursor = 0;
            let match = null;

            while ((match = linkPattern.exec(text)) !== null) {
                const start = match.index;
                const rawMatch = String(match[0] || '');
                if (start > cursor) {
                    element.appendChild(document.createTextNode(text.slice(cursor, start)));
                }

                const parts = trimTrailingLinkPunctuation(rawMatch);
                const linkText = String(parts.link || '').trim();
                const trailingText = String(parts.trailing || '');

                if (linkText) {
                    const anchor = document.createElement('a');
                    anchor.className = 'message-inline-link';
                    anchor.href = /^https?:\/\//i.test(linkText) ? linkText : `https://${linkText}`;
                    anchor.target = '_blank';
                    anchor.rel = 'noopener';
                    anchor.textContent = linkText;
                    element.appendChild(anchor);
                } else {
                    element.appendChild(document.createTextNode(rawMatch));
                }

                if (trailingText) {
                    element.appendChild(document.createTextNode(trailingText));
                }
                cursor = start + rawMatch.length;
            }

            if (cursor < text.length) {
                element.appendChild(document.createTextNode(text.slice(cursor)));
            }
        };

        const appendMedia = (container, mediaList) => {
            if (!Array.isArray(mediaList) || mediaList.length === 0) return;
            const mediaWrap = document.createElement('div');
            mediaWrap.className = 'message-media-list';
            let remainingMessagePhotoSlots = 1;
            mediaList.forEach(mediaUrl => {
                const url = String(mediaUrl || '').trim();
                if (!url) return;
                const lower = url.toLowerCase();
                const isLikelyNonImage = /\.(mp4|mov|avi|webm|m3u8|mp3|wav|pdf|docx?|xlsx?|zip|rar)(\?|#|$)/.test(lower) ||
                    /(smil(\.xml)?|application\/smil)/.test(lower);
                const isImage = !isLikelyNonImage;
                if (isImage) {
                    if (remainingMessagePhotoSlots <= 0) return;
                    const img = document.createElement('img');
                    img.src = url;
                    img.alt = 'Message media';
                    img.loading = 'lazy';
                    img.style.maxWidth = '220px';
                    img.style.borderRadius = '8px';
                    img.style.display = 'block';
                    img.style.marginTop = '6px';
                    img.style.cursor = 'pointer';
                    img.addEventListener('click', () => {
                        window.open(url, '_blank', 'noopener');
                    });
                    img.onerror = () => {
                        const link = document.createElement('a');
                        link.href = url;
                        link.target = '_blank';
                        link.rel = 'noopener';
                        link.textContent = url;
                        link.style.display = 'block';
                        link.style.marginTop = '6px';
                        if (img.parentNode) img.parentNode.replaceChild(link, img);
                    };
                    mediaWrap.appendChild(img);
                    remainingMessagePhotoSlots -= 1;
                    return;
                }
                const link = document.createElement('a');
                link.href = url;
                link.target = '_blank';
                link.rel = 'noopener';
                link.textContent = url;
                link.style.display = 'block';
                link.style.marginTop = '6px';
                mediaWrap.appendChild(link);
            });
            if (mediaWrap.childNodes.length > 0) {
                container.appendChild(mediaWrap);
            }
        };

        const appendMessageDateTime = (messageEl, message) => {
            if (!messageEl || !message || typeof message !== 'object') return;
            const contentText = String(message.content || '').trim();
            if (/^(template used:|conversation escalated:)/i.test(contentText)) return;
            const dateTime = String(message.date_time || '').trim().replace(/\.\d{3}$/, '');
            if (!dateTime) return;
            const agentId = String((message.agent || message.agent_id || message.agentId || '')).trim();
            const rawType = String(message.message_type || '').trim().toLowerCase();
            let messageTypeLabel = rawType || String(message.role || '').trim().toLowerCase();
            if (messageTypeLabel === 'agent') {
                messageTypeLabel = specialAgentIds.has(agentId) ? 'AI' : 'agent';
            }
            const metaText = messageTypeLabel ? `${dateTime} from ${messageTypeLabel}` : dateTime;
            const dateEl = document.createElement('span');
            dateEl.className = 'message-date-time';
            dateEl.textContent = metaText;
            messageEl.classList.add('has-date-time');
            messageEl.appendChild(dateEl);
        };

        const adjustDateTimeMetaBubbleSizing = () => {
            if (!chatMessages) return;
            const bubbles = chatMessages.querySelectorAll('.message.has-date-time');
            bubbles.forEach((bubble) => {
                if (!(bubble instanceof HTMLElement)) return;
                const dateEl = bubble.querySelector('.message-date-time');
                if (!(dateEl instanceof HTMLElement)) return;

                // Reset dynamic sizing before re-measuring.
                bubble.style.minWidth = '';
                bubble.style.width = '';
                bubble.style.maxWidth = '';
                bubble.style.marginLeft = '';
                bubble.style.marginRight = '';

                const baseWidth = Math.ceil(bubble.getBoundingClientRect().width);
                const requiredWidth = Math.ceil(dateEl.scrollWidth + 32); // 16px inset on both sides
                if (requiredWidth <= baseWidth) return;

                const growBy = requiredWidth - baseWidth;
                bubble.style.maxWidth = 'none';
                bubble.style.width = `${requiredWidth}px`;
                bubble.style.minWidth = `${requiredWidth}px`;

                if (bubble.classList.contains('received')) {
                    // Customer: grow naturally to the right from left alignment.
                    bubble.style.marginLeft = '';
                    bubble.style.marginRight = '';
                } else if (bubble.classList.contains('sent')) {
                    // System/agent: keep inner/left edge stable and grow outward (right).
                    bubble.style.marginRight = `-${growBy}px`;
                }
            });
        };

        if (!Array.isArray(conversation) || conversation.length === 0) {
            const fallbackMessage = document.createElement('div');
            fallbackMessage.className = 'message received';
            const fallbackContent = document.createElement('div');
            fallbackContent.className = 'message-content';
            const fallbackParagraph = document.createElement('p');
            appendLinkifiedText(fallbackParagraph, scenario.customerMessage || '');
            fallbackContent.appendChild(fallbackParagraph);
            fallbackMessage.appendChild(fallbackContent);
            chatMessages.appendChild(fallbackMessage);
            return;
        }

        conversation.forEach((message, index) => {
            if (!message || !message.content) return;
            if (message.role === 'system') {
                const systemText = String(message.content || '').trim();
                const isCenteredSystemNote = /^(template used:|conversation escalated:|escalation notes?:)/i.test(systemText);
                const systemMessage = document.createElement('div');
                systemMessage.className = `message sent system-message${isCenteredSystemNote ? ' center-system-note' : ''}`;
                const systemContent = document.createElement('div');
                systemContent.className = 'message-content';
                const systemParagraph = document.createElement('p');
                appendLinkifiedText(systemParagraph, message.content);
                systemContent.appendChild(systemParagraph);
                systemMessage.appendChild(systemContent);
                appendMessageDateTime(systemMessage, message);
                appendMedia(systemContent, message.media);
                chatMessages.appendChild(systemMessage);
                return;
            }

            const isAgent = message.role === 'agent';
            const agentIdentifier = String((message && (message.agent || message.agent_id || message.agentId)) || '').trim();
            const isSpecialAgent = isAgent && specialAgentIds.has(agentIdentifier);
            const wrapper = document.createElement('div');
            wrapper.className = `message ${isAgent ? 'sent' : 'received'}`;
            if (isSpecialAgent) {
                wrapper.classList.add('special-agent-message');
            }

            const content = document.createElement('div');
            content.className = 'message-content';
            const p = document.createElement('p');
            appendLinkifiedText(p, message.content);
            content.appendChild(p);
            appendMedia(content, message.media);

            if (isAgent && !isSpecialAgent && index !== lastAgentConversationIndex) {
                wrapper.classList.add('has-agent-selector');
                const selectorWrap = document.createElement('label');
                selectorWrap.className = 'agent-message-selector';

                const selectorInput = document.createElement('input');
                selectorInput.type = 'checkbox';
                selectorInput.className = 'agent-message-selector-input';
                selectorInput.setAttribute('aria-label', 'Send this agent message to sheet');

                selectorInput.addEventListener('change', async () => {
                    if (!selectorInput.checked) return;
                    const confirmed = window.confirm('Send this message to the sheet?');
                    if (!confirmed) {
                        selectorInput.checked = false;
                        return;
                    }
                    await submitSelectedAgentMessage(message, index, selectorInput);
                });

                selectorWrap.appendChild(selectorInput);
                wrapper.appendChild(selectorWrap);
            }

            wrapper.appendChild(content);
            appendMessageDateTime(wrapper, message);
            chatMessages.appendChild(wrapper);
        });

        requestAnimationFrame(adjustDateTimeMetaBubbleSizing);
    }

    function scrollChatToBottomAfterRender() {
        if (!chatMessages) return;
        requestAnimationFrame(() => {
            chatMessages.scrollTop = chatMessages.scrollHeight;
        });
    }
    
    // Load scenario content into the page
    function loadScenarioContent(scenarioNumber, data) {
        const scenario = data[scenarioNumber];
        if (!scenario) {
            console.error('Scenario not found:', scenarioNumber);
            return;
        }
        
        console.log('Loading scenario:', scenarioNumber, scenario);
        
        // Update page title
        document.title = `Training - Scenario ${scenarioNumber}`;
        
        // Build conversation from scenario mapping or preloaded array
        let conversation = buildConversationFromScenario(scenario);
        scenario.conversation = conversation;

        // Update company info with error checking
        const companyLink = document.getElementById('companyNameLink');
        const shopifyBadge = document.getElementById('companyShopifyBadge');
        const agentElement = document.getElementById('agentName');
        const messageToneElement = document.getElementById('messageTone');
        const blocklistedWordsRow = document.getElementById('blocklistedWordsRow');
        const escalationPreferencesRow = document.getElementById('escalationPreferencesRow');
        const phoneElement = document.getElementById('customerPhone');
        const messageElement = document.getElementById('customerMessage');
        
        if (companyLink) {
            companyLink.textContent = scenario.companyName;
            const websiteRaw = (scenario.companyWebsite || (scenario.rightPanel && scenario.rightPanel.source && scenario.rightPanel.source.value) || '').trim();
            const hasWebsite = websiteRaw && websiteRaw.toLowerCase() !== 'n/a';
            if (hasWebsite) {
                const url = /^https?:\/\//i.test(websiteRaw) ? websiteRaw : `https://${websiteRaw}`;
                companyLink.href = url;
                companyLink.target = '_blank';
                companyLink.rel = 'noopener';
                companyLink.classList.remove('is-disabled');
            } else {
                companyLink.removeAttribute('href');
                companyLink.removeAttribute('target');
                companyLink.removeAttribute('rel');
                companyLink.classList.add('is-disabled');
            }
        } else {
            console.error('companyNameLink element not found');
        }
        if (shopifyBadge) {
            const hasShopify = scenarioHasShopify(scenario);
            shopifyBadge.style.display = hasShopify ? 'inline-flex' : 'none';
        }
        
        if (agentElement) {
            agentElement.textContent = scenario.agentName || '';
        }
        else console.error('agentName element not found');

        if (messageToneElement) {
            const tone = String(scenario.messageTone || '').trim();
            messageToneElement.textContent = tone;
            messageToneElement.style.display = tone ? 'inline-block' : 'none';
        } else {
            console.error('messageTone element not found');
        }

        const renderBadges = (rowElement, items) => {
            if (!rowElement) return;
            rowElement.innerHTML = '';
            if (!items.length) {
                rowElement.style.display = 'none';
                return;
            }
            items.forEach(item => {
                const badge = document.createElement('span');
                badge.className = 'agent-badge';
                badge.textContent = item;
                rowElement.appendChild(badge);
            });
            rowElement.style.display = 'flex';
        };

        const blocklistedWords = normalizeScenarioLabelList(
            scenario.blocklisted_words != null ? scenario.blocklisted_words : scenario.blocklistedWords
        );
        const escalationPreferences = normalizeScenarioLabelList(
            scenario.escalation_preferences != null ? scenario.escalation_preferences : scenario.escalationPreferences
        );

        renderBadges(blocklistedWordsRow, blocklistedWords);
        renderBadges(escalationPreferencesRow, escalationPreferences);
        
        if (phoneElement) phoneElement.textContent = scenario.customerPhone || '';
        else console.error('customerPhone element not found');
        
        if (messageElement) {
            messageElement.textContent = getFirstCustomerMessageFromScenario(scenario, conversation);
        }

        // Render conversation and always land on the latest message.
        renderConversationMessages(Array.isArray(conversation) ? conversation : [], scenario);
        scrollChatToBottomAfterRender();
        
        // Update guidelines dynamically
        const guidelinesContainer = document.getElementById('dynamic-guidelines-container');
        const notesData = scenario.notes || scenario.guidelines;
        if (guidelinesContainer && notesData) {
            guidelinesContainer.innerHTML = '';
            
            // Create categories dynamically based on scenario data
            Object.keys(notesData).forEach(categoryKey => {
                const categoryData = notesData[categoryKey];
                if (Array.isArray(categoryData) && categoryData.length > 0) {
                    // Get category display info
                    const categoryInfo = getCategoryInfo(categoryKey);
                    
                    // Create category section
                    const categorySection = document.createElement('div');
                    categorySection.className = 'guidelines-section';
                    
                    // Create category header
                    const categoryHeader = document.createElement('div');
                    categoryHeader.className = 'guidelines-header';
                    
                    // Create icon element
                    const iconElement = document.createElement('i');
                    iconElement.setAttribute('data-feather', categoryInfo.icon);
                    iconElement.className = 'icon-small';
                    
                    // Create category title
                    const titleElement = document.createElement('span');
                    titleElement.textContent = categoryInfo.display;
                    
                    // Assemble header
                    categoryHeader.appendChild(iconElement);
                    categoryHeader.appendChild(titleElement);
                    
                    // Create guidelines list
                    const guidelinesList = document.createElement('ul');
                    guidelinesList.className = 'guidelines-list';
                    
                    // Add guidelines items
                    categoryData.forEach(item => {
                        const li = document.createElement('li');
                        const text = String(item || '').trim();
                        const match = text.match(/^\*\*(.*)\*\*$/);
                        if (match) {
                            const strong = document.createElement('strong');
                            strong.textContent = match[1];
                            li.appendChild(strong);
                        } else {
                            li.textContent = text;
                        }
                        guidelinesList.appendChild(li);
                    });
                    
                    // Assemble category section
                    categorySection.appendChild(categoryHeader);
                    categorySection.appendChild(guidelinesList);
                    
                    // Add to container
                    guidelinesContainer.appendChild(categorySection);
                }
            });
        }
        
        // Update right panel content
        loadRightPanelContent(scenario);
        
        // Store current scenario data
        currentScenario = scenarioNumber;
        scenarioData = scenario;
        
        // Re-initialize Feather icons after DOM changes
        if (typeof feather !== 'undefined') {
            feather.replace();
        }

        // Load internal notes for this scenario
        if (internalNotesEl) {
            const assignmentKey = assignmentNotesStorageKey();
            const scenarioKey = `internalNotes_scenario_${scenarioNumber}`;
            const fallback = localStorage.getItem(scenarioKey) || '';
            const saved = assignmentKey ? (localStorage.getItem(assignmentKey) || fallback) : fallback;
            internalNotesEl.value = saved;
        }
        updateSnapshotShareButtonVisibility();
    }
    
    // Render dynamic Promotions/Gifts from scenarios.json if provided
    function renderPromotions(promotions) {
        const container = document.getElementById('promotionsContainer');
        if (!container) return 0;

        // Allow single object or array
        const items = Array.isArray(promotions) ? promotions : [promotions];
        let rendered = 0;

        items.forEach(promo => {
            if (!promo) return;
            const contentLines = (() => {
                if (Array.isArray(promo.content)) {
                    return promo.content.map(line => String(line || '').trim()).filter(Boolean);
                }
                if (typeof promo.content === 'string') {
                    return promo.content.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
                }
                return [];
            })();
            if (!contentLines.length) return;

            const section = document.createElement('div');
            section.className = 'promotions-section';

            const header = document.createElement('div');
            header.className = 'promotions-header';

            const icon = document.createElement('div');
            icon.className = 'promotions-icon';
            icon.innerHTML = '<i data-feather="gift"></i>';

            const info = document.createElement('div');
            info.className = 'promotions-info';

            const titleRow = document.createElement('div');
            titleRow.className = 'promotions-title';

            const titleSpan = document.createElement('span');
            titleSpan.textContent = promo.title || 'Promotion';
            titleRow.appendChild(titleSpan);

            // Active badge if active_status truthy or equals "active"
            const status = (promo.active_status ?? '').toString().toLowerCase();
            if (promo.active_status === true || status === 'active' || status === 'true' || status === '1') {
                const badge = document.createElement('div');
                badge.className = 'active-badge';
                badge.textContent = 'Active';
                titleRow.appendChild(badge);
            }

            const desc = document.createElement('div');
            desc.className = 'promotions-description';

            contentLines.forEach(line => {
                const p = document.createElement('p');
                p.textContent = `• ${line}`;
                desc.appendChild(p);
            });

            info.appendChild(titleRow);
            info.appendChild(desc);
            header.appendChild(icon);
            header.appendChild(info);
            section.appendChild(header);
            container.appendChild(section);
            rendered += 1;
        });

        if (rendered > 0 && typeof feather !== 'undefined') {
            feather.replace();
        }
        return rendered;
    }

    // Function to load right panel dynamic content
    function loadRightPanelContent(scenario) {
        const promosContainer = document.getElementById('promotionsContainer');
        if (promosContainer) {
            promosContainer.innerHTML = '';
            promosContainer.style.display = 'none';
        }
        if (!scenario.rightPanel) return;

        // Render promotions dynamically, supporting multiple keys: promotions, promotions_2, promotions_3, ...
        const rightPanel = scenario.rightPanel || {};
        const promoKeys = Object.keys(rightPanel)
            .filter(k => /^promotions(_\d+)?$/.test(k))
            .sort((a, b) => {
                const na = a === 'promotions' ? 1 : parseInt(a.split('_')[1] || '0', 10);
                const nb = b === 'promotions' ? 1 : parseInt(b.split('_')[1] || '0', 10);
                return na - nb;
            });
        let renderedPromotions = 0;
        if (promoKeys.length > 0) {
            promoKeys.forEach(key => {
                const block = rightPanel[key];
                if (block) {
                    renderedPromotions += renderPromotions(block);
                }
            });
        }
        if (promosContainer) {
            promosContainer.style.display = renderedPromotions > 0 ? '' : 'none';
        }
        
        // Update source information
        if (scenario.rightPanel.source) {
            const sourceLabel = document.getElementById('sourceLabel');
            const sourceValue = document.getElementById('sourceValue');
            const sourceDate = document.getElementById('sourceDate');
            const sourceBlock = document.getElementById('sourceBlock');
            const sourceValueText = String(scenario.rightPanel.source.value || '').trim();
            const hideWebsiteSource = scenario.rightPanel.source.label === 'Website' && sourceValueText;
            
            if (sourceLabel) sourceLabel.textContent = scenario.rightPanel.source.label;
            if (sourceValue) sourceValue.textContent = scenario.rightPanel.source.value;
            if (sourceDate) sourceDate.textContent = scenario.rightPanel.source.date;
            if (sourceBlock) {
                sourceBlock.style.display = hideWebsiteSource ? 'none' : '';
            }
        }
        
        // Update browsing history (support multiple key shapes + show explicit empty state)
        const historyContainer = document.getElementById('browsingHistory');
        if (historyContainer) {
            historyContainer.innerHTML = '';
            const rawHistory =
                (Array.isArray(scenario.rightPanel.browsingHistory) && scenario.rightPanel.browsingHistory) ||
                (Array.isArray(scenario.rightPanel.browsing_history) && scenario.rightPanel.browsing_history) ||
                (Array.isArray(scenario.rightPanel.last5Products) && scenario.rightPanel.last5Products) ||
                (Array.isArray(scenario.rightPanel.last_5_products) && scenario.rightPanel.last_5_products) ||
                [];

            const normalizedHistory = rawHistory
                .map(historyItem => {
                    if (!historyItem || typeof historyItem !== 'object') return null;
                    const itemText =
                        String(historyItem.item || historyItem.product_name || historyItem.name || '').trim();
                    const itemLink =
                        String(historyItem.link || historyItem.product_link || historyItem.url || '').trim();
                    const timeAgo =
                        String(historyItem.timeAgo || historyItem.view_date || historyItem.date || '').trim();
                    if (!itemText && !itemLink) return null;
                    return { itemText: itemText || itemLink, itemLink, timeAgo };
                })
                .filter(Boolean);

            if (!normalizedHistory.length) {
                const empty = document.createElement('li');
                empty.textContent = 'No browsing history for this customer.';
                empty.style.color = '#8a8a8a';
                historyContainer.appendChild(empty);
            } else {
                normalizedHistory.forEach(historyItem => {
                    const li = document.createElement('li');
                    const itemEl = historyItem.itemLink ? document.createElement('a') : document.createElement('span');
                    itemEl.textContent = historyItem.itemText;
                    if (historyItem.itemLink && itemEl.tagName.toLowerCase() === 'a') {
                        itemEl.href = historyItem.itemLink;
                        itemEl.target = '_blank';
                        itemEl.rel = 'noopener';
                    }
                    li.appendChild(itemEl);

                    if (historyItem.timeAgo) {
                        const time = document.createElement('span');
                        time.className = 'time-ago';
                        time.textContent = historyItem.timeAgo;
                        li.appendChild(time);
                    }

                    const icon = document.createElement('i');
                    icon.setAttribute('data-feather', 'eye');
                    icon.className = 'icon-small';
                    li.appendChild(icon);

                    historyContainer.appendChild(li);
                });
            }
        }

        // Orders (expandable). Hide section when not present or empty.
        const ordersSection = document.getElementById('ordersSection');
        const ordersList = document.getElementById('ordersList');
        if (ordersSection && ordersList) {
            ordersList.innerHTML = '';
            const orders = Array.isArray(scenario.rightPanel.orders) ? scenario.rightPanel.orders : [];
            if (orders.length > 0) {
                orders.forEach(order => {
                    const li = document.createElement('li');

                    const details = document.createElement('details');
                    details.className = 'order-details';

                    const summary = document.createElement('summary');
                    summary.className = 'order-summary';

                    const summaryLeft = document.createElement('span');
                    summaryLeft.className = 'order-summary-left';

                    const resolvedOrderLink = order && (order.link || order.order_status_url || order.statusUrl)
                        ? String(order.link || order.order_status_url || order.statusUrl).trim()
                        : '';
                    const orderLabel = document.createElement(resolvedOrderLink ? 'a' : 'span');
                    const rawOrderNumber = order && order.orderNumber != null ? String(order.orderNumber).trim() : '';
                    const cleanOrderNumber = rawOrderNumber.replace(/^#+\s*/, '');
                    orderLabel.textContent = cleanOrderNumber ? `# ${cleanOrderNumber}` : 'Order';
                    if (resolvedOrderLink && orderLabel.tagName.toLowerCase() === 'a') {
                        orderLabel.href = resolvedOrderLink;
                        orderLabel.target = '_blank';
                        orderLabel.rel = 'noopener';
                    }

                    const orderDate = document.createElement('span');
                    orderDate.className = 'time-ago';
                    orderDate.textContent = firstNonEmptyValue([
                        order && order.orderDate,
                        order && order.order_date,
                        order && order.date
                    ]);

                    summaryLeft.appendChild(orderLabel);
                    summaryLeft.appendChild(orderDate);

                    const arrow = document.createElement('span');
                    arrow.className = 'order-chevron-text';
                    arrow.setAttribute('aria-hidden', 'true');

                    summary.appendChild(summaryLeft);
                    summary.appendChild(arrow);
                    details.appendChild(summary);

                    const body = document.createElement('div');
                    body.className = 'order-body';

                    const orderDateTime = firstNonEmptyValue([
                        order && order.date_time,
                        order && order.dateTime,
                        order && order.created_at,
                        order && order.createdAt
                    ]);
                    if (orderDateTime) {
                        const createdRow = document.createElement('div');
                        createdRow.className = 'order-meta-row';
                        const createdLabel = document.createElement('strong');
                        createdLabel.textContent = 'Created';
                        const createdValue = document.createElement('span');
                        createdValue.className = 'order-meta-value';
                        createdValue.textContent = orderDateTime;
                        createdRow.appendChild(createdLabel);
                        createdRow.appendChild(createdValue);
                        body.appendChild(createdRow);
                    }

                    const summaryTitle = document.createElement('div');
                    summaryTitle.className = 'order-section-title';
                    summaryTitle.textContent = 'Order summary';
                    body.appendChild(summaryTitle);

                    const products = Array.isArray(order && order.items) ? order.items : [];
                    products.forEach(product => {
                        const row = document.createElement('div');
                        row.className = 'order-product-row';

                        const nameWrap = document.createElement('div');
                        nameWrap.className = 'order-product-name-wrap';

                        const resolvedProductLink = product && (product.productLink || product.product_link)
                            ? String(product.productLink || product.product_link)
                            : '';
                        const productName = document.createElement(resolvedProductLink ? 'a' : 'span');
                        productName.textContent = product && product.name ? product.name : '';
                        if (resolvedProductLink && productName.tagName.toLowerCase() === 'a') {
                            productName.href = resolvedProductLink;
                            productName.target = '_blank';
                            productName.rel = 'noopener';
                        }
                        nameWrap.appendChild(productName);

                        const productQtyRaw = firstNonEmptyValue([
                            product && product.qty,
                            product && product.quantity,
                            product && product.qnty
                        ]);
                        if (productQtyRaw) {
                            const qty = document.createElement('span');
                            qty.className = 'order-product-qty';
                            qty.textContent = `Qty: ${productQtyRaw}`;
                            nameWrap.appendChild(qty);
                        }

                        const productPrice = document.createElement('span');
                        const productPriceRaw = (product && product.price != null) ? String(product.price).trim() : '';
                        productPrice.textContent = formatDollarAmount(productPriceRaw);

                        row.appendChild(nameWrap);
                        row.appendChild(productPrice);
                        body.appendChild(row);
                    });

                    const discountRaw = firstNonEmptyValue([
                        order && order.discount,
                        order && order.order_discount,
                        order && order.orderDiscount,
                        order && order.total_discount,
                        order && order.totalDiscount
                    ]);
                    if (discountRaw) {
                        const discountText = String(discountRaw).trim();
                        const discountRow = document.createElement('div');
                        discountRow.className = 'order-total-row order-total-row--discount';
                        const discountLabel = document.createElement('strong');
                        discountLabel.textContent = 'Total discount';
                        const discountValue = document.createElement('strong');
                        const cleanDiscount = discountText.replace(/^\-\s*/, '');
                        const formattedDiscount = formatDollarAmount(cleanDiscount);
                        discountValue.textContent = discountText.startsWith('-') ? `- ${formattedDiscount}` : formattedDiscount;
                        discountRow.appendChild(discountLabel);
                        discountRow.appendChild(discountValue);
                        body.appendChild(discountRow);
                    }

                    const couponRaw = firstNonEmptyValue([
                        order && order.coupon,
                        order && order.coupon_code,
                        order && order.couponCode
                    ]);
                    if (couponRaw) {
                        const couponRow = document.createElement('div');
                        couponRow.className = 'order-total-row';
                        const couponLabel = document.createElement('strong');
                        couponLabel.textContent = 'Coupon used';
                        const couponValue = document.createElement('span');
                        couponValue.textContent = couponRaw;
                        couponRow.appendChild(couponLabel);
                        couponRow.appendChild(couponValue);
                        body.appendChild(couponRow);
                    }

                    const subtotalRow = document.createElement('div');
                    subtotalRow.className = 'order-total-row';
                    const subtotalLabel = document.createElement('strong');
                    subtotalLabel.textContent = 'Order subtotal';
                    const subtotalValue = document.createElement('strong');
                    const orderSubtotalRaw = firstNonEmptyValue([
                        order && order.subtotal,
                        order && order.order_subtotal,
                        order && order.orderSubtotal,
                        order && order.total
                    ]);
                    subtotalValue.textContent = formatDollarAmount(orderSubtotalRaw);
                    subtotalRow.appendChild(subtotalLabel);
                    subtotalRow.appendChild(subtotalValue);
                    body.appendChild(subtotalRow);

                    details.appendChild(body);
                    li.appendChild(details);
                    ordersList.appendChild(li);
                });
                ordersSection.style.display = '';
            } else {
                ordersSection.style.display = 'none';
            }
        }
        
        // Update template items
        const templatesForScenario = getTemplatesForScenario(scenario);
        initializeTemplateSearch(templatesForScenario);
        resetTemplateSearch();
    }
    
    // Helper function to convert timestamp to EST - just date
    function toESTTimestamp() {
        const now = new Date();
        // Convert to EST (Eastern Time) and format as MM/DD/YYYY only
        const options = {
            timeZone: 'America/New_York',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit'
        };
        
        const estDate = now.toLocaleDateString('en-US', options);
        return estDate; // Returns in format: "MM/DD/YYYY"
    }

    // Helper: EST datetime format like "1/30/2025 13:24:49" (no comma, month/day numeric)
    function toESTDateTimeNoComma() {
        const now = new Date();
        const str = now.toLocaleString('en-US', {
            timeZone: 'America/New_York',
            year: 'numeric',
            month: 'numeric', // no leading zero
            day: 'numeric',   // no leading zero
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
        });
        // Many browsers include a comma between date and time; remove it
        return str.replace(", ", " ");
    }

    // Helper function to get current timer time
    function getCurrentTimerTime() {
        const timerElement = document.getElementById('sessionTimer');
        return timerElement ? timerElement.textContent : '00:00';
    }

    // Function to send data to Google Sheets with custom timer value
    async function sendToGoogleSheetsWithTimer(agentUsername, scenario, customerMessage, agentResponse, timerValue, options = {}) {
        let data = null;
        try {
            // Create a unique session ID per scenario that persists throughout the session
            let scenarioSessionId = options.sessionIdOverride || localStorage.getItem(`scenarioSession_${currentScenario}`);
            if (!scenarioSessionId) {
                scenarioSessionId = `${currentScenario}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
                localStorage.setItem(`scenarioSession_${currentScenario}`, scenarioSessionId);
            }
            
            data = {
                timestampEST: toESTTimestamp(),
                agentUsername: agentUsername,
                scenario: scenario,
                customerMessage: customerMessage,
                agentResponse: agentResponse,
                sessionId: scenarioSessionId, // Use scenario-specific session ID
                sendTime: (options.sendTimeOverride || timerValue), // Use the provided timer value instead of getCurrentTimerTime()
                messageId: options.messageId || ''
            };
            
            console.log('Sending to Google Sheets:', data);
            
            const response = await fetch(GOOGLE_SCRIPT_URL, {
                method: 'POST',
                mode: 'cors',
                headers: {
                    'Content-Type': 'text/plain',
                },
                body: JSON.stringify(data)
            });
            
            if (response.ok) {
                console.log('Successfully sent to Google Sheets');
                const result = await response.json();
                console.log('Sheet response:', result);
                return true;
            } else {
                console.error('Failed to send to Google Sheets:', response.status);
                return false;
            }
        } catch (error) {
            console.error('Error sending to Google Sheets:', error);
            // Store locally if Google Sheets fails
            try {
                const failedData = JSON.parse(localStorage.getItem('failedSheetData') || '[]');
                // Guard: data might be undefined if error occurred before it was built
                const safeData = typeof data === 'object' && data ? data : {
                    timestampEST: toESTTimestamp(),
                    agentUsername,
                    scenario,
                    customerMessage,
                    agentResponse,
                    sessionId: localStorage.getItem(`scenarioSession_${currentScenario}`) || 'unknown',
                    sendTime: timerValue || getCurrentTimerTime(),
                    messageId: (options && options.messageId) || ''
                };
                failedData.push(safeData);
                localStorage.setItem('failedSheetData', JSON.stringify(failedData));
            } catch (e) {
                console.error('Failed to persist failedSheetData:', e);
            }
            return false;
        }
    }
    
    // Get the last customer message for context
    function getLastCustomerMessage() {
        const customerMessages = document.querySelectorAll('.message.received .message-content p');
        if (customerMessages.length > 0) {
            return customerMessages[customerMessages.length - 1].textContent;
        }
        return 'No customer message found';
    }
    
    // Set the agent name from localStorage if available
    const agentName = localStorage.getItem('agentName');
    if (agentName) {
        const agentNameElements = document.querySelectorAll('.agent-name');
        agentNameElements.forEach(element => {
            element.innerHTML = agentName + ' <i data-feather="chevron-down" class="icon-small"></i>';
        });
        
        // Re-initialize Feather icons after DOM changes
        if (typeof feather !== 'undefined') {
            feather.replace();
        }
    }
    
    // Check if user is logged in (redirect to login if not). Only enforce on http/https to avoid file:// loops
    const isHttpProtocol = window.location.protocol === 'http:' || window.location.protocol === 'https:';
    if (isHttpProtocol && !isSnapshotLinkActive() && !localStorage.getItem('agentName') && !window.location.href.includes('login.html') && 
        !window.location.href.includes('index.html')) {
        window.location.href = 'index.html';
    }
    
    // Add logout functionality
    function setLogoutLoadingState(isActive) {
        if (!logoutLoadingOverlay) return;
        const active = !!isActive;
        logoutLoadingOverlay.hidden = !active;
        logoutLoadingOverlay.setAttribute('aria-hidden', active ? 'false' : 'true');
    }

    function sendSessionLogout() {
        try {
            const sessionId = localStorage.getItem('globalSessionId');
            const agentName = localStorage.getItem('agentName') || 'Unknown Agent';
            const agentEmail = localStorage.getItem('agentEmail') || '';
            const loginMethod = localStorage.getItem('loginMethod') || 'unknown';
            const payload = {
                eventType: 'sessionLogout',
                agentUsername: agentName,
                agentEmail,
                sessionId,
                loginMethod,
                logoutAt: new Date().toLocaleString('en-US', {
                    timeZone: 'America/New_York',
                    year: 'numeric', month: '2-digit', day: '2-digit',
                    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
                }),
                logoutAtMs: Date.now()
            };
            const body = JSON.stringify(payload);
            if (navigator.sendBeacon) {
                const blob = new Blob([body], { type: 'text/plain' });
                navigator.sendBeacon(GOOGLE_SCRIPT_URL, blob);
            } else {
                fetch(GOOGLE_SCRIPT_URL, { method: 'POST', mode: 'cors', headers: { 'Content-Type': 'text/plain' }, body });
            }
        } catch (_) {}
    }

    if (logoutBtn) {
        logoutBtn.addEventListener('click', async () => {
            if (isExplicitLogoutInProgress) return;
            isExplicitLogoutInProgress = true;
            logoutBtn.disabled = true;
            setLogoutLoadingState(true);
            pendingLogoutReleasePayload = {
                email: getLoggedInEmail(),
                session_id: getAssignmentSessionId()
            };
            stopAssignmentHeartbeat();
            try {
                await releaseAssignmentSession('logout').catch(() => ({ ok: false }));
                sendSessionLogout();
                clearSubmitOutboxTimer();
                writeSubmitOutboxJobs([]);
                refreshPendingDoneAssignmentsFromOutbox([]);
                assignmentSessionState = null;
                clearAssignmentSessionId();
                localStorage.removeItem('agentName');
                localStorage.removeItem('agentEmail');
                localStorage.removeItem('sessionStartTime');
                localStorage.removeItem('loginMethod');
                localStorage.removeItem('unlockedScenario'); // Reset scenario progression

                // Clear all scenario timer and message count data
                Object.keys(localStorage).forEach(key => {
                    if (key.startsWith('sessionStartTime_scenario_') ||
                        key.startsWith('scenarioSession_') ||
                        key.startsWith('messageCount_scenario_')) {
                        localStorage.removeItem(key);
                    }
                });

                window.location.href = 'index.html';
            } catch (error) {
                console.error('Logout failed:', error);
                isExplicitLogoutInProgress = false;
                logoutBtn.disabled = false;
                setLogoutLoadingState(false);
                showTransientTopNotice('Logout failed. Please try again.', true);
            }
        });
    }
    
    function addSystemStatusMessage(text) {
        if (!chatMessages || !text) return;
        const messageDiv = document.createElement('div');
        messageDiv.className = 'message sent system-message center-system-note';
        messageDiv.innerHTML = `
            <div class="message-content">
                <p>${text}</p>
            </div>
        `;
        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
    }

    async function navigateScenarioList(direction) {
        let data = allScenariosData || {};
        let keys = [];

        const runtimeIndex = await loadRuntimeScenariosIndex();
        if (runtimeIndex && Array.isArray(runtimeIndex.order) && runtimeIndex.order.length) {
            keys = runtimeIndex.order.map(v => String(v || '').trim()).filter(Boolean);
        }

        if (!keys.length) {
            data = await loadScenariosData();
            if (!data) return;
            keys = Object.keys(data)
                .map(k => parseInt(k, 10))
                .filter(n => !isNaN(n))
                .sort((a, b) => a - b)
                .map(n => String(n));
        }

        if (!keys.length) return;
        const current = String(getCurrentScenarioNumber());
        const currentIndex = keys.indexOf(current);
        const fallbackIndex = direction > 0 ? 0 : keys.length - 1;
        const targetIndex = currentIndex >= 0
            ? (currentIndex + direction + keys.length) % keys.length
            : fallbackIndex;
        const targetScenario = keys[targetIndex];
        if (!isCsvScenarioMode() && !canAccessScenario(targetScenario)) return;

        if (!data[targetScenario]) {
            await ensureScenariosLoaded([targetScenario]).catch(() => {});
            data = allScenariosData || data;
        }

        setCurrentScenarioNumber(targetScenario);
        window.location.href = buildScenarioUrl(targetScenario, data);
    }

    async function navigateAssignmentQueue(direction) {
        if (isSnapshotMode) return false;
        if (!canUseAssignmentMode()) return false;
        if (assignmentNavigationInProgress) {
            pendingAssignmentNavigationDirection = direction > 0 ? 1 : -1;
            debugLog('navigateAssignmentQueue queued (already in progress)', { direction: pendingAssignmentNavigationDirection });
            return true;
        }
        assignmentNavigationInProgress = true;
        if (previousConversationBtn) previousConversationBtn.disabled = true;
        if (nextConversationBtn) nextConversationBtn.disabled = true;
        try {
        let queue = assignmentQueue;
        if (!Array.isArray(queue) || !queue.length) {
            queue = await refreshAssignmentQueue().catch(() => []);
        }
        if (!Array.isArray(queue) || !queue.length) return false;
        queue = queue
            .filter(item => !isAssignmentPendingDone(item && item.assignment_id))
            .filter(item => !isAssignmentLocallySkipped(item && item.assignment_id));
        if (!queue.length) {
            setAssignmentsStatus('Background sync is still finishing. Please wait.', true);
            return false;
        }

        if (queue.length === 1) {
            const onlyItemId = String((queue[0] && queue[0].assignment_id) || '').trim();
            const currentIdSingle = assignmentContext && assignmentContext.assignment_id
                ? String(assignmentContext.assignment_id).trim()
                : '';
            if (onlyItemId && currentIdSingle && onlyItemId === currentIdSingle) {
                goToAssignmentLoadingPage('No other assigned conversation is ready yet. Checking queue...');
                return false;
            }
        }

        const currentId = assignmentContext && assignmentContext.assignment_id
            ? String(assignmentContext.assignment_id)
            : '';
        const currentIndex = currentId
            ? queue.findIndex(item => String((item && item.assignment_id) || '') === currentId)
            : -1;
        const fallbackIndex = direction > 0 ? 0 : queue.length - 1;
        const targetIndex = currentIndex >= 0
            ? (currentIndex + direction + queue.length) % queue.length
            : fallbackIndex;
        const target = queue[targetIndex];
        debugLog('navigateAssignmentQueue', {
            direction,
            currentId,
            currentIndex,
            targetIndex,
            targetAssignmentId: target && target.assignment_id ? String(target.assignment_id) : '',
            queueSize: queue.length
        });
        const prefersViewUrl = !!(assignmentContext && (assignmentContext.role === 'viewer' || assignmentContext.mode === 'view'));
        const url = getAssignmentUrlFromQueueItem(target, { prefersView: prefersViewUrl });
        if (!url) return false;

        const opened = await openAssignmentInPageByUrl(url, {
            updateHistory: true,
            replaceHistory: false,
            refreshQueue: false
        });
        if (!opened) {
            debugLog('Primary queue navigation target failed; retrying after queue refresh.');
            const refreshedQueue = await refreshAssignmentQueue().catch(() => []);
            const retryTarget = getNextQueueItem(refreshedQueue, currentId, direction, []);
            if (!retryTarget) {
                setAssignmentsStatus('Unable to open the target assignment.', true);
                return false;
            }
            const retryUrl = getAssignmentUrlFromQueueItem(retryTarget, { prefersView: prefersViewUrl });
            if (!retryUrl) {
                setAssignmentsStatus('Unable to open the target assignment.', true);
                return false;
            }
            const openedRetry = await openAssignmentInPageByUrl(retryUrl, {
                updateHistory: true,
                replaceHistory: false,
                refreshQueue: false
            });
            if (!openedRetry) {
                setAssignmentsStatus('Unable to open the target assignment.', true);
                return false;
            }
        }
        return true;
        } finally {
            assignmentNavigationInProgress = false;
            const forceView = !!(assignmentContext && (assignmentContext.role === 'viewer' || assignmentContext.mode === 'view'));
            if (previousConversationBtn) previousConversationBtn.disabled = forceView;
            if (nextConversationBtn) nextConversationBtn.disabled = forceView;
            const queuedDirection = pendingAssignmentNavigationDirection;
            pendingAssignmentNavigationDirection = 0;
            if (queuedDirection === 1 || queuedDirection === -1) {
                setTimeout(() => {
                    navigateAssignmentQueue(queuedDirection).catch(() => {});
                }, 0);
            }
        }
    }

    async function navigateConversation(direction) {
        if (isSnapshotMode) return;
        const movedByAssignment = await navigateAssignmentQueue(direction);
        if (assignmentContext && assignmentContext.assignment_id) {
            return;
        }
        if (!movedByAssignment) {
            await navigateScenarioList(direction);
        }
    }

    function goToAssignmentLoadingPage(statusMessage = 'Loading assignments...') {
        if (!canUseAssignmentMode() || isSnapshotMode) return;
        setAssignmentsStatus(statusMessage, false);
        window.location.href = 'app.html';
    }

    function getNextQueueItem(queue, currentAssignmentId, direction, excludeAssignmentIds = []) {
        const list = (Array.isArray(queue) ? queue : [])
            .filter(item => !isAssignmentPendingDone(item && item.assignment_id))
            .filter(item => !isAssignmentLocallySkipped(item && item.assignment_id));
        if (!list.length) return null;

        const excludeSet = new Set((Array.isArray(excludeAssignmentIds) ? excludeAssignmentIds : [])
            .map(id => String(id || '').trim())
            .filter(Boolean));
        const currentId = String(currentAssignmentId || '').trim();
        const currentIndex = currentId
            ? list.findIndex(item => String((item && item.assignment_id) || '').trim() === currentId)
            : -1;

        if (currentIndex >= 0) {
            for (let step = 1; step <= list.length; step++) {
                const idx = (currentIndex + (step * direction) + list.length) % list.length;
                const item = list[idx] || {};
                const assignmentId = String(item.assignment_id || '').trim();
                if (!assignmentId || excludeSet.has(assignmentId) || isAssignmentLocallySkipped(assignmentId)) continue;
                return item;
            }
        }

        for (let i = 0; i < list.length; i++) {
            const item = list[i] || {};
            const assignmentId = String(item.assignment_id || '').trim();
            if (!assignmentId || excludeSet.has(assignmentId) || isAssignmentLocallySkipped(assignmentId)) continue;
            return item;
        }
        return null;
    }

    async function openNextAssignmentAfterOptimisticSubmit(submittedAssignmentId) {
        const prefersView = !!(assignmentContext && (assignmentContext.role === 'viewer' || assignmentContext.mode === 'view'));
        let nextItem = getNextQueueItem(assignmentQueue, submittedAssignmentId, 1, [submittedAssignmentId]);
        if (!nextItem) {
            const refreshedQueue = await refreshAssignmentQueue().catch(() => []);
            nextItem = getNextQueueItem(refreshedQueue, submittedAssignmentId, 1, [submittedAssignmentId]);
        }
        if (!nextItem) return false;

        const nextUrl = getAssignmentUrlFromQueueItem(nextItem, { prefersView });
        if (!nextUrl) return false;
        debugLog('optimistic_advance_target', {
            submittedAssignmentId: String(submittedAssignmentId || ''),
            nextAssignmentId: String(nextItem && nextItem.assignment_id || '')
        });
        let opened = await openAssignmentInPageByUrl(nextUrl, {
            updateHistory: true,
            replaceHistory: false,
            refreshQueue: false
        });
        if (!opened) {
            await new Promise((resolve) => setTimeout(resolve, 350));
            const refreshedQueue = await refreshAssignmentQueue().catch(() => []);
            const retryItem = getNextQueueItem(refreshedQueue, submittedAssignmentId, 1, [submittedAssignmentId]);
            if (retryItem) {
                const retryUrl = getAssignmentUrlFromQueueItem(retryItem, { prefersView });
                if (retryUrl) {
                    opened = await openAssignmentInPageByUrl(retryUrl, {
                        updateHistory: true,
                        replaceHistory: false,
                        refreshQueue: false
                    });
                }
            }
        }
        debugLog('Optimistic submit auto-advance', {
            submittedAssignmentId: String(submittedAssignmentId || ''),
            nextAssignmentId: String(nextItem && nextItem.assignment_id || ''),
            opened: !!opened
        });
        return !!opened;
    }

    function isTypingTarget(target) {
        if (!target) return false;
        const tag = String(target.tagName || '').toUpperCase();
        return target.isContentEditable || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
    }

    // Event listeners
    if (previousConversationBtn) {
        previousConversationBtn.addEventListener('click', async () => {
            await navigateConversation(-1);
        });
    }

    if (nextConversationBtn) {
        nextConversationBtn.addEventListener('click', async () => {
            await navigateConversation(1);
        });
    }

    document.addEventListener('keydown', async (event) => {
        if (!event) return;
        if (event.repeat) return;
        if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
        if (isTypingTarget(event.target)) return;

        if (event.key === 'ArrowLeft') {
            if (previousConversationBtn && previousConversationBtn.disabled) return;
            event.preventDefault();
            await navigateConversation(-1);
            return;
        }

        if (event.key === 'ArrowRight') {
            if (nextConversationBtn && nextConversationBtn.disabled) return;
            event.preventDefault();
            await navigateConversation(1);
        }
    });

    // Session Timer functionality - persists across page refreshes
    let timerStartTime = Date.now();
    let timerInterval = null;

    function initSessionTimer() {
        const timerElement = document.getElementById('sessionTimer');
        if (!timerElement) {
            console.error('Timer element not found!');
            return;
        }
        
        console.log('Initializing timer...');

        // Get or create session start time that persists across refreshes
        const currentScenario = getCurrentScenarioNumber();
        const sessionKey = `sessionStartTime_scenario_${currentScenario}`;
        
        let sessionStartTime = localStorage.getItem(sessionKey);
        if (!sessionStartTime) {
            // First time loading this scenario - start fresh timer
            sessionStartTime = Date.now();
            localStorage.setItem(sessionKey, sessionStartTime);
        } else {
            // Convert back to number
            sessionStartTime = parseInt(sessionStartTime);
        }
        
        timerStartTime = sessionStartTime;
        
        function updateTimer() {
            const currentTime = Date.now();
            const elapsedTime = Math.floor((currentTime - timerStartTime) / 1000); // in seconds
            
            const minutes = Math.floor(elapsedTime / 60);
            const seconds = elapsedTime % 60;
            
            const formattedTime = `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
            timerElement.textContent = formattedTime;
        }

        // Clear any existing timer interval
        if (timerInterval) {
            clearInterval(timerInterval);
        }

        // Update timer immediately and then every second
        updateTimer();
        timerInterval = setInterval(updateTimer, 1000);
        console.log('Timer started successfully');
    }

    // Function to reset the timer when agent sends a message
    function resetTimer() {
        console.log('Resetting timer...');
        const newStartTime = Date.now();
        timerStartTime = newStartTime;
        
        // Update localStorage with new start time
        const currentScenario = getCurrentScenarioNumber();
        const sessionKey = `sessionStartTime_scenario_${currentScenario}`;
        localStorage.setItem(sessionKey, newStartTime);
        
        // Don't update display immediately - let the timer interval handle it
        // This ensures the timer value is captured before the reset takes effect
    }

    // Utility to safely create highlighted content without using innerHTML
    function escapeRegExpForSearch(input) {
        return input.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    function createHighlightedFragment(text, searchTerm) {
        const fragment = document.createDocumentFragment();
        if (!searchTerm) {
            fragment.appendChild(document.createTextNode(text));
            return fragment;
        }
        const safePattern = new RegExp(`(${escapeRegExpForSearch(searchTerm)})`, 'gi');
        let lastIndex = 0;
        let match;
        while ((match = safePattern.exec(text)) !== null) {
            const start = match.index;
            const end = start + match[0].length;
            if (start > lastIndex) {
                fragment.appendChild(document.createTextNode(text.slice(lastIndex, start)));
            }
            const mark = document.createElement('mark');
            mark.textContent = text.slice(start, end);
            fragment.appendChild(mark);
            lastIndex = end;
        }
        if (lastIndex < text.length) {
            fragment.appendChild(document.createTextNode(text.slice(lastIndex)));
        }
        return fragment;
    }

    function renderTemplateItems(templates, searchTerm) {
        const templatesContainer = document.getElementById('templateItems');
        if (!templatesContainer) return;
        templatesContainer.innerHTML = '';

        const safeSearch = String(searchTerm || '').toLowerCase().trim();
        const sourceTemplates = Array.isArray(templates) ? templates : [];
        const filteredTemplates = sourceTemplates.filter(template => {
            const name = String((template && template.name) || '').toLowerCase();
            const shortcut = String((template && template.shortcut) || '').toLowerCase();
            const content = String((template && template.content) || '').toLowerCase();
            return name.includes(safeSearch) ||
                shortcut.includes(safeSearch) ||
                content.includes(safeSearch);
        });

        filteredTemplates.forEach(template => {
            const templateDiv = document.createElement('div');
            templateDiv.className = 'template-item';
            if (isGlobalTemplate(template)) {
                templateDiv.classList.add('template-item--global');
            }

            const headerDiv = document.createElement('div');
            headerDiv.className = 'template-header';

            const nameSpan = document.createElement('span');
            nameSpan.appendChild(createHighlightedFragment(String(template && template.name ? template.name : ''), safeSearch));

            headerDiv.appendChild(nameSpan);
            const shortcutText = String((template && template.shortcut) || '').trim();
            if (shortcutText) {
                const shortcutSpan = document.createElement('span');
                shortcutSpan.className = 'template-shortcut';
                shortcutSpan.appendChild(createHighlightedFragment(shortcutText, safeSearch));
                headerDiv.appendChild(shortcutSpan);
            }

            const contentP = document.createElement('p');
            contentP.appendChild(createHighlightedFragment(String(template && template.content ? template.content : ''), safeSearch));

            templateDiv.appendChild(headerDiv);
            templateDiv.appendChild(contentP);
            templatesContainer.appendChild(templateDiv);
        });

        if (filteredTemplates.length === 0 && safeSearch !== '') {
            const noResultsDiv = document.createElement('div');
            noResultsDiv.className = 'no-results';
            noResultsDiv.textContent = 'No templates found';
            templatesContainer.appendChild(noResultsDiv);
        }
    }

    // Function to initialize template search functionality
    function initializeTemplateSearch(templates) {
        const searchInput = document.querySelector('.search-templates input');
        const templatesContainer = document.getElementById('templateItems');
        if (!searchInput || !templatesContainer) return;

        templateSearchSourceTemplates = Array.isArray(templates) ? templates : [];

        if (!templateSearchBound) {
            searchInput.addEventListener('input', function(e) {
                const searchTerm = e && e.target ? e.target.value : '';
                renderTemplateItems(templateSearchSourceTemplates, searchTerm);
            });
            templateSearchBound = true;
        }

        renderTemplateItems(templateSearchSourceTemplates, searchInput.value || '');
    }

    function resetTemplateSearch() {
        const searchInput = document.querySelector('.search-templates input');
        if (searchInput) {
            searchInput.value = '';
        }
        renderTemplateItems(templateSearchSourceTemplates, '');
    }
    
    // Initialize everything
    const snapshotParams = getSnapshotParamsFromUrl();
    const hasSnapshot = !!(snapshotParams.snapshotId && snapshotParams.snapshotToken);
    const existingSessionId = getAssignmentSessionId();
    const existingSubmitOutboxJobs = filterOutboxJobsForSession(readSubmitOutboxJobs(), existingSessionId);
    writeSubmitOutboxJobs(existingSubmitOutboxJobs);
    refreshPendingDoneAssignmentsFromOutbox(existingSubmitOutboxJobs);
    if (existingSubmitOutboxJobs.length && !hasSnapshot) {
        scheduleSubmitOutboxProcessing(0);
    }
    updateSnapshotShareButtonVisibility();
    const assignmentParams = getAssignmentParamsFromUrl();
    const hasAid = !!assignmentParams.aid;

    if (hasSnapshot) {
        await loadSnapshotFromLink(snapshotParams.snapshotId, snapshotParams.snapshotToken);
    } else if (hasAid) {
        await loadRuntimeScenariosIndex();
        await loadRuntimeTemplatesIndex();
        try {
            if (!canUseAssignmentMode()) {
                throw new Error('Assignment mode requires email login.');
            }
            getAssignmentSessionId({ createIfMissing: true });
            await refreshAssignmentQueue().catch(() => []);
            const opened = await openAssignmentInPage(assignmentParams, {
                updateHistory: true,
                replaceHistory: true,
                refreshQueue: false
            });
            if (!opened) {
                throw new Error('Failed to open assignment in-page.');
            }
        } catch (assignmentError) {
            console.error('Assignment flow error:', assignmentError);
            setAssignmentsStatus(`Assignment error: ${assignmentError.message || assignmentError}`, true);
            debugLog('Assignment init failed; trying queue fallback without monolith load.', String((assignmentError && assignmentError.message) || assignmentError || ''));
            try {
                const queue = await refreshAssignmentQueue().catch(() => []);
                const fallbackParams = getNextAssignmentParamsFromQueue(queue, { prefersView: false });
                if (fallbackParams) {
                    const openedFallback = await openAssignmentInPage(fallbackParams, {
                        updateHistory: true,
                        replaceHistory: true,
                        refreshQueue: false,
                        maxAttempts: 3
                    });
                    if (openedFallback) {
                        setAssignmentsStatus('Opened next available assignment.', false);
                    }
                }
            } catch (fallbackError) {
                debugLog('Queue fallback after assignment init failure did not open a conversation.', String((fallbackError && fallbackError.message) || fallbackError || ''));
            }
        }
    } else {
        let openedAssignmentFromQueue = false;
        if (canUseAssignmentMode()) {
            await loadRuntimeScenariosIndex();
            getAssignmentSessionId({ createIfMissing: true });
            try {
                const queue = await refreshAssignmentQueue();
                if (queue.length) {
                    setAssignmentsStatus('Queue loaded. Opening first assignment...', false);
                    const firstParams = getNextAssignmentParamsFromQueue(queue, {
                        prefersView: false
                    });
                    if (firstParams) {
                        const opened = await openAssignmentInPage(firstParams, {
                            updateHistory: true,
                            replaceHistory: true,
                            refreshQueue: false,
                            maxAttempts: 3
                        });
                        if (opened) {
                            openedAssignmentFromQueue = true;
                        }
                    }
                    const firstEditUrl = (!openedAssignmentFromQueue && queue[0])
                        ? (queue[0].edit_url || queue[0].view_url || '')
                        : '';
                    if (!openedAssignmentFromQueue && firstEditUrl) {
                        const openedFallback = await openAssignmentInPageByUrl(firstEditUrl, {
                            updateHistory: true,
                            replaceHistory: true,
                            refreshQueue: false
                        });
                        if (openedFallback) {
                            openedAssignmentFromQueue = true;
                        } else {
                            setAssignmentsStatus('Queue loaded, but first assignment could not be opened.', true);
                        }
                    }
                } else {
                    setAssignmentsStatus('No assignments available.', false);
                }
            } catch (assignmentError) {
                console.error('Assignment flow error:', assignmentError);
                setAssignmentsStatus(`Assignment error: ${assignmentError.message || assignmentError}`, true);
            }
        }

        if (!openedAssignmentFromQueue) {
            templatesData = await loadTemplatesData();
            const scenarios = await loadScenariosData();
            allScenariosData = scenarios || {};
            if (!scenarios) {
                console.error('Could not load scenarios data');
            } else {
                if (!canUseAssignmentMode()) {
                    setAssignmentsStatus('Assignment mode requires email login.', true);
                }
                const scenarioKeys = Object.keys(scenarios)
                    .map(k => parseInt(k, 10))
                    .filter(n => !isNaN(n))
                    .sort((a, b) => a - b)
                    .map(n => String(n));
                const requestedScenario = resolveRequestedScenarioKey(scenarios) || getCurrentScenarioNumber();
                const activeScenario = scenarios[requestedScenario] ? requestedScenario : (scenarioKeys[0] || '1');
                setCurrentScenarioNumber(activeScenario);
                await ensureTemplatesLoadedForScenarioKeys([activeScenario]).catch(() => {});
                loadScenarioContent(activeScenario, scenarios);
            }
        }
    }

    // If this scenario was previously ended (via action buttons), keep input disabled IF it's NOT the current unlocked scenario.
    // This prevents stale ended flags from blocking a fresh session when logging back in or starting a new unlocked scenario.
    // Conversation end state and timer removed
    
    // Initialize new features
    initTemplateSearchKeyboardShortcut();

    if (assignmentSelect) {
        assignmentSelect.addEventListener('dblclick', () => {
            openSelectedAssignmentFromList();
        });
    }

    window.addEventListener('popstate', async () => {
        const snapshotParamsNow = getSnapshotParamsFromUrl();
        if (snapshotParamsNow.snapshotId && snapshotParamsNow.snapshotToken) {
            await loadSnapshotFromLink(snapshotParamsNow.snapshotId, snapshotParamsNow.snapshotToken);
            return;
        }
        if (isSnapshotMode) {
            window.location.reload();
            return;
        }
        const params = getAssignmentParamsFromUrl();
        if (!params.aid || !params.token) return;
        const currentAid = assignmentContext ? String(assignmentContext.assignment_id || '') : '';
        const currentToken = assignmentContext ? String(assignmentContext.token || '') : '';
        const currentMode = assignmentContext ? String(assignmentContext.mode || '') : '';
        if (currentAid === params.aid && currentToken === params.token && currentMode === params.mode) return;
        await openAssignmentInPage(params, {
            updateHistory: false,
            replaceHistory: false,
            refreshQueue: false
        });
    });
    
    // Persist internal notes and assignment drafts
    if (internalNotesEl) {
        internalNotesEl.addEventListener('input', () => {
            if (isSnapshotMode) return;
            if (assignmentContext && assignmentContext.assignment_id) {
                const key = assignmentNotesStorageKey();
                if (key) localStorage.setItem(key, internalNotesEl.value);
                const customForm = document.getElementById('customForm');
                scheduleAssignmentDraftSave(customForm);
            } else {
                const scenarioNumForNotes = getCurrentScenarioNumber();
                localStorage.setItem(`internalNotes_scenario_${scenarioNumForNotes}`, internalNotesEl.value);
            }
        });
        // Add drag-to-resize behavior via handle below the textarea
        const notesContainer = document.getElementById('internalNotesContainer');
        const resizeHandle = document.querySelector('.resize-handle-horizontal[data-resize="internal-notes"]');
        if (notesContainer && resizeHandle && internalNotesEl) {
            let isResizingNotes = false;
            let startY = 0;
            let startHeight = 0;
            resizeHandle.addEventListener('mousedown', (e) => {
                isResizingNotes = true;
                startY = e.clientY;
                startHeight = internalNotesEl.offsetHeight;
                document.body.style.cursor = 'row-resize';
                document.body.style.userSelect = 'none';
                e.preventDefault();
            });
            document.addEventListener('mousemove', (e) => {
                if (!isResizingNotes) return;
                const delta = e.clientY - startY;
                // Handle is on top: moving up (smaller clientY) should increase height
                const newHeight = Math.max(60, Math.min(300, startHeight - delta));
                internalNotesEl.style.height = newHeight + 'px';
                // Persist height
                try { localStorage.setItem('internalNotesHeight', String(newHeight)); } catch (_) {}
            });
            document.addEventListener('mouseup', () => {
                if (!isResizingNotes) return;
                isResizingNotes = false;
                document.body.style.cursor = '';
                document.body.style.userSelect = '';
            });
        }
        // Restore saved height
        const savedHeight = parseInt(localStorage.getItem('internalNotesHeight') || '0', 10);
        if (!isNaN(savedHeight) && savedHeight > 0) {
            internalNotesEl.style.height = savedHeight + 'px';
        }
    }

    // Start the session timer after content is loaded (not used in anonymous snapshot mode)
    if (!isSnapshotMode) {
        initSessionTimer();
    }

    // Custom form submission -> Google Sheets (Data tab)
    const customForm = document.getElementById('customForm');
    if (customForm) {
        const formStatus = document.getElementById('formStatus');
        const formSubmitBtn = document.getElementById('formSubmitBtn');
        const clearFormBtn = document.getElementById('clearFormBtn');
        const notesField = customForm.querySelector('#notes');
        const zeroToleranceSelect = customForm.querySelector('#zeroTolerance');
        const notesRequiredIndicator = customForm.querySelector('h4 .required');

        function shouldRequireNotes() {
            return true;
        }

        function updateNotesRequirementUI() {
            const required = shouldRequireNotes();
            if (notesField) {
                notesField.required = required;
            }
            if (notesRequiredIndicator) {
                notesRequiredIndicator.style.visibility = required ? 'visible' : 'hidden';
            }
            return required;
        }
        
        // Ensure all checkboxes are checked by default (and on reset)
        applyDefaultCustomFormState(customForm);
        updateNotesRequirementUI();
        
        // ---- Form autosave/restore ----
        function saveCustomFormState() {
            const state = collectCustomFormState(customForm);
            const key = assignmentContext && assignmentContext.assignment_id
                ? assignmentFormStateStorageKey()
                : 'customFormState';
            try { localStorage.setItem(key, JSON.stringify(state)); } catch (_) {}
            scheduleAssignmentDraftSave(customForm);
        }
        function restoreCustomFormState() {
            let parsed = null;
            const key = assignmentContext && assignmentContext.assignment_id
                ? assignmentFormStateStorageKey()
                : 'customFormState';
            try { parsed = JSON.parse(localStorage.getItem(key) || 'null'); } catch (_) { parsed = null; }
            if (!parsed) return;
            applyCustomFormState(customForm, parsed);
        }
        customForm.addEventListener('input', saveCustomFormState);
        customForm.addEventListener('change', () => {
            updateNotesRequirementUI();
            saveCustomFormState();
        });
        restoreCustomFormState();
        updateNotesRequirementUI();

        // Clear form functionality
        if (clearFormBtn) {
            clearFormBtn.addEventListener('click', () => {
                customForm.reset();
                // Re-apply default checked state
                applyDefaultCustomFormState(customForm);
                updateNotesRequirementUI();
                try {
                    const key = assignmentContext && assignmentContext.assignment_id
                        ? assignmentFormStateStorageKey()
                        : 'customFormState';
                    localStorage.removeItem(key);
                } catch (_) {}
                scheduleAssignmentDraftSave(customForm);
                if (formStatus) {
                    formStatus.textContent = '';
                }
            });
        }
        
        customForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (isSnapshotMode) {
                if (formStatus) {
                    formStatus.textContent = 'Snapshot view is read-only.';
                    formStatus.style.color = '#e74c3c';
                }
                return;
            }
            if (assignmentContext && (assignmentContext.role !== 'editor' || assignmentContext.mode === 'view')) {
                if (formStatus) {
                    formStatus.textContent = 'View-only link cannot submit.';
                    formStatus.style.color = '#e74c3c';
                }
                return;
            }
            if (assignmentContext && assignmentContext.assignment_id && isAssignmentPendingDone(assignmentContext.assignment_id)) {
                if (formStatus) {
                    formStatus.textContent = 'This conversation is already submitted and syncing in the background.';
                    formStatus.style.color = '#e74c3c';
                }
                return;
            }
            
            // Collect all form data
            const formData = new FormData(customForm);

            // Full ordered labels and slug->label maps for each category
            const CATEGORY_LABELS = {
                issue_identification: ['Intent Identified', 'Necessary Reply'],
                proper_resolution: ['Efficient Troubleshooting', 'Correct Escalation', 'Double Text', 'Partial Reply'],
                product_sales: ['General Recommendation', 'Discount Upsell', 'Restock Question', 'Upsell'],
                accuracy: [
                    'Credible Source', 'Promo - Active', 'Promo - Correct', 'Promo - Hallucinated',
                    'Link - Broken', 'Link - Correct Page', 'Link - Correct Region', 'Link - Correct Website',
                    'Link - Filtered', 'Link - Relevant Item'
                ],
                workflow: [
                    'Checkout Page', 'Company Profile', 'Conversation', 'Customer Profile', 'Notes',
                    'Product Information', 'Promo Notes', 'Templates', 'Website'
                ],
                clarity: ['Correct Grammar', 'No Typos', 'No Repetition', 'Understandable Message'],
                tone: ['Preferred tone followed', 'Personalized', 'Empathetic']
            };
            const VALUE_TO_LABEL = {
                issue_identification: { intent_identified: 'Intent Identified', necessary_reply: 'Necessary Reply' },
                proper_resolution: { efficient_troubleshooting: 'Efficient Troubleshooting', correct_escalation: 'Correct Escalation', double_text: 'Double Text', partial_reply: 'Partial Reply' },
                product_sales: { general_recommendation: 'General Recommendation', discount_upsell: 'Discount Upsell', restock_question: 'Restock Question', upsell: 'Upsell' },
                accuracy: {
                    credible_source: 'Credible Source', promo_active: 'Promo - Active', promo_correct: 'Promo - Correct', promo_hallucinated: 'Promo - Hallucinated',
                    link_broken: 'Link - Broken', link_correct_page: 'Link - Correct Page', link_correct_region: 'Link - Correct Region', link_correct_website: 'Link - Correct Website',
                    link_filtered: 'Link - Filtered', link_relevant_item: 'Link - Relevant Item'
                },
                workflow: {
                    checkout_page: 'Checkout Page', company_profile: 'Company Profile', conversation: 'Conversation', customer_profile: 'Customer Profile',
                    notes: 'Notes', product_information: 'Product Information', promo_notes: 'Promo Notes', templates: 'Templates', website: 'Website'
                },
                clarity: { correct_grammar: 'Correct Grammar', no_typos: 'No Typos', no_repetition: 'No Repetition', understandable_message: 'Understandable Message' },
                tone: { preferred_tone_followed: 'Preferred tone followed', personalized: 'Personalized', empathetic: 'Empathetic' }
            };

            // Process checkboxes (multiple values per category)
            const checkboxCategories = ['issue_identification', 'proper_resolution', 'product_sales', 'accuracy', 'workflow', 'clarity', 'tone'];
            const selectedByCategory = {};
            checkboxCategories.forEach(category => {
                selectedByCategory[category] = formData.getAll(category); // array of slugs
            });

            function buildCategoryCell(categoryKey) {
                const full = CATEGORY_LABELS[categoryKey] || [];
                const valueMap = VALUE_TO_LABEL[categoryKey] || {};
                const selected = new Set((selectedByCategory[categoryKey] || []).map(v => valueMap[v]).filter(Boolean));
                // Include selected items only, keeping original category order.
                const included = full.filter(label => selected.has(label));
                return included.join(',');
            }

            // Process dropdowns: capture human-readable labels
            const zeroTolSel = document.getElementById('zeroTolerance');
            const zeroToleranceLabel = (zeroTolSel && zeroTolSel.value) ? zeroTolSel.options[zeroTolSel.selectedIndex].text : '';
            const notesVal = formData.get('notes') || '';
            
            // Validate required fields
            const notesRequiredNow = updateNotesRequirementUI();
            if (notesRequiredNow && !notesVal.trim()) {
                if (formStatus) { 
                    formStatus.textContent = 'Notes field is required.'; 
                    formStatus.style.color = '#e74c3c'; 
                }
                return;
            }
            
            const agentUsername = localStorage.getItem('agentName') || 'Unknown Agent';
            const agentEmail = localStorage.getItem('agentEmail') || '';
            const emailAddress = agentEmail || agentUsername;
            const auditTime = getCurrentTimerTime();
            // Reset timer after capturing audit time
            resetTimer();
            
            try {
                if (formSubmitBtn) formSubmitBtn.disabled = true;
                if (formStatus) { 
                    formStatus.textContent = 'Submitting...'; 
                    formStatus.style.color = '#555'; 
                }
                
                // Build Data tab payload
                const payload = {
                    eventType: 'evaluationFormSubmission',
                    timestamp: toESTDateTimeNoComma(), // e.g., 1/30/2025 13:24:49
                    emailAddress,
                    messageId: assignmentContext ? (assignmentContext.send_id || '') : '',
                    auditTime,
                    issueIdentification: buildCategoryCell('issue_identification'),
                    properResolution: buildCategoryCell('proper_resolution'),
                    productSales: buildCategoryCell('product_sales'),
                    accuracy: buildCategoryCell('accuracy'),
                    workflow: buildCategoryCell('workflow'),
                    clarity: buildCategoryCell('clarity'),
                    tone: buildCategoryCell('tone'),
                    zeroTolerance: zeroToleranceLabel || '',
                    notes: notesVal
                };
                const submitContext = assignmentContext && assignmentContext.role === 'editor'
                    ? {
                        assignment_id: String(assignmentContext.assignment_id || ''),
                        token: String(assignmentContext.token || ''),
                        mode: String(assignmentContext.mode || 'edit')
                    }
                    : null;

                if (submitContext && submitContext.assignment_id && submitContext.token) {
                    const sessionId = getAssignmentSessionId();
                    if (!sessionId) {
                        throw new Error('Missing assignment session id.');
                    }

                    queueSubmitOutboxJob({
                        assignment_id: submitContext.assignment_id,
                        token: submitContext.token,
                        session_id: sessionId,
                        app_base: getCurrentAppBaseUrl(),
                        payload
                    });

                    customForm.reset();
                    applyDefaultCustomFormState(customForm);
                    resetTemplateSearch();
                    try {
                        localStorage.removeItem(`customFormState_assignment_${submitContext.assignment_id}`);
                        localStorage.removeItem(`internalNotes_assignment_${submitContext.assignment_id}`);
                    } catch (_) {}

                    if (formStatus) {
                        formStatus.textContent = 'Submitted. Moving to next conversation...';
                        formStatus.style.color = '#28a745';
                    }
                    setAssignmentsStatus('Submitted. Syncing in background and opening next conversation...', false);
                    await new Promise((resolve) => setTimeout(resolve, SUBMIT_ADVANCE_DELAY_MS));
                    const moved = await openNextAssignmentAfterOptimisticSubmit(submitContext.assignment_id);
                    if (!moved) {
                        goToAssignmentLoadingPage('Submitted. Syncing in background; checking queue...');
                    }
                    return;
                }

                await submitEvaluationFormPayload(payload);
                if (formStatus) {
                    formStatus.textContent = 'Submitted successfully.';
                    formStatus.style.color = '#28a745';
                }
                customForm.reset();
                applyDefaultCustomFormState(customForm);
                resetTemplateSearch();
                try {
                    const key = assignmentContext && assignmentContext.assignment_id
                        ? assignmentFormStateStorageKey()
                        : 'customFormState';
                    localStorage.removeItem(key);
                } catch (_) {}
            } catch (err) {
                console.error('Form submission error:', err);
                if (formStatus) { 
                    formStatus.textContent = 'Submission failed. Please try again.'; 
                    formStatus.style.color = '#e74c3c'; 
                }
            } finally {
                if (formSubmitBtn) formSubmitBtn.disabled = false;
            }
        });
        debugLog('submit_handler_bound');
    }

    // Panel resizing functionality
    initPanelResizing();

    // Attempt to record logout on tab close/navigation
    window.addEventListener('online', () => {
        const jobs = filterOutboxJobsForSession(readSubmitOutboxJobs(), getAssignmentSessionId());
        if (jobs.length) {
            scheduleSubmitOutboxProcessing(0);
        }
    });

    window.addEventListener('beforeunload', () => {
        stopAssignmentHeartbeat();
        if (isExplicitLogoutInProgress) {
            sendBeaconReleaseSession('logout', pendingLogoutReleasePayload);
            pendingLogoutReleasePayload = null;
        }
    });
});

// Panel resizing functionality
function initPanelResizing() {
    const resizeHandles = document.querySelectorAll('.resize-handle');
    let isResizing = false;
    let currentHandle = null;
    let startX = 0;
    let startWidths = {};

    resizeHandles.forEach(handle => {
        handle.addEventListener('mousedown', (e) => {
            isResizing = true;
            currentHandle = handle;
            startX = e.clientX;
            
            // Get current panel widths
            const panels = getPanelsForHandle(handle);
            startWidths = {
                left: panels.left.offsetWidth,
                right: panels.right.offsetWidth
            };
            
            handle.classList.add('resizing');
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
            
            e.preventDefault();
        });
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing || !currentHandle) return;
        
        const deltaX = e.clientX - startX;
        const panels = getPanelsForHandle(currentHandle);
        const container = document.querySelector('.main-content');
        const containerWidth = container.offsetWidth;
        
        // Calculate new widths
        const newLeftWidth = startWidths.left + deltaX;
        const newRightWidth = startWidths.right - deltaX;
        
        // Set minimum widths
        const minWidth = 200;
        if (newLeftWidth < minWidth || newRightWidth < minWidth) return;
        
        // Calculate percentages
        const leftPercent = (newLeftWidth / containerWidth) * 100;
        const rightPercent = (newRightWidth / containerWidth) * 100;
        
        // Apply new flex-basis values
        panels.left.style.flexBasis = `${leftPercent}%`;
        panels.right.style.flexBasis = `${rightPercent}%`;
    });

    document.addEventListener('mouseup', () => {
        if (!isResizing) return;
        
        isResizing = false;
        if (currentHandle) {
            currentHandle.classList.remove('resizing');
            currentHandle = null;
        }
        
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
    });

    function getPanelsForHandle(handle) {
        const resizeType = handle.getAttribute('data-resize');
        
        switch (resizeType) {
            case 'form-left':
                return {
                    left: document.querySelector('.form-panel'),
                    right: document.querySelector('.left-panel')
                };
            case 'left-chat':
                return {
                    left: document.querySelector('.left-panel'),
                    right: document.querySelector('.chat-panel')
                };
            case 'chat-right':
                return {
                    left: document.querySelector('.chat-panel'),
                    right: document.querySelector('.right-panel')
                };
            default:
                return { left: null, right: null };
        }
    }

}

// ==================
// NEW FEATURES CODE
// ==================

// Template search keyboard shortcut (Ctrl + /)
function initTemplateSearchKeyboardShortcut() {
    const templateSearch = document.getElementById('templateSearch');
    
    if (templateSearch) {
        document.addEventListener('keydown', (event) => {
            // Check for Ctrl + / or Cmd + / (Mac)
            if ((event.ctrlKey || event.metaKey) && event.key === '/') {
                event.preventDefault();
                templateSearch.focus();
            }
        });
        
        // Optional: Add visual feedback when focused via keyboard shortcut
        templateSearch.addEventListener('focus', () => {
            templateSearch.style.borderColor = '#007bff';
        });
        
        templateSearch.addEventListener('blur', () => {
            templateSearch.style.borderColor = '#ddd';
        });
    }
}
