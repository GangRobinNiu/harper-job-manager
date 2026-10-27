/**
 * Admin UI for the job manager (internal). Read-only: it polls the REST-exported tables every few seconds.
 * Polling (not the SSE stream) because a table-wide stream replays every row on connect.
 * All data is rendered with textContent, never as HTML, so job payloads cannot inject markup.
 */
const POLL_MS = 3_000;
const LIST_LIMIT = 200;
const COUNT_LIMIT = 10_000;
const LIST_FIELDS = 'id,definitionId,tenant,queue,trigger,status,attempts,maxAttempts,runAfter,lastError,createdAt,completedAt';

const state = {
	tab: 'jobs',
	status: '',
	tenant: '',
	queue: '',
	search: '',
	selectedJobId: null,
	paused: false,
	jobs: [],
	user: null,
};

const $ = (id) => document.getElementById(id);

class SignedOut extends Error {}

async function get(path) {
	const response = await fetch(path, { headers: { accept: 'application/json' } });
	if (response.status === 401) throw new SignedOut();
	if (response.status === 403) throw new Error(`your user is not allowed to read ${path.split('?')[0]}`);
	if (!response.ok) throw new Error(`${response.status} ${response.statusText} for ${path}`);
	return response.json();
}

async function send(method, path, body) {
	const response = await fetch(path, {
		method,
		headers: { accept: 'application/json', 'content-type': 'application/json' },
		body: body === undefined ? undefined : JSON.stringify(body),
	});
	return { status: response.status, data: await response.json().catch(() => ({})) };
}

/** Build an element: el('td', { className: 'mono' }, 'text', childElement, ...). */
function el(tag, props = {}, ...children) {
	const node = Object.assign(document.createElement(tag), props);
	for (const child of children) {
		if (child == null || child === false) continue;
		node.append(child instanceof Node ? child : document.createTextNode(String(child)));
	}
	return node;
}

// ---------- formatting ----------

const toMs = (value) => (value == null ? null : typeof value === 'number' ? value : Date.parse(value));

function ago(value) {
	const ms = toMs(value);
	if (!ms) return '–';
	const s = Math.round((Date.now() - ms) / 1000);
	if (s < 0) return `in ${formatDuration(-s)}`;
	if (s < 5) return 'just now';
	return `${formatDuration(s)} ago`;
}

function formatDuration(s) {
	if (s < 60) return `${s}s`;
	if (s < 3600) return `${Math.floor(s / 60)}m`;
	if (s < 86400) return `${Math.floor(s / 3600)}h`;
	return `${Math.floor(s / 86400)}d`;
}

const clock = (value) => {
	const ms = toMs(value);
	return ms ? new Date(ms).toLocaleTimeString() : '–';
};
const fullTime = (value) => {
	const ms = toMs(value);
	return ms ? new Date(ms).toLocaleString() : '–';
};

const STATUS_LABEL = { pending: 'Pending', claimed: 'Running', completed: 'Completed', dead: 'Dead' };

/** A status pill; a pending job that failed before shows when it will retry. */
function statusPill(job) {
	const pill = el('span', { className: `pill pill-${job.status}` }, STATUS_LABEL[job.status] ?? job.status);
	if (job.status === 'pending' && job.lastError && job.runAfter > Date.now()) {
		return el('span', {}, pill, el('span', { className: 'sub' }, `retry ${ago(job.runAfter)}`));
	}
	return pill;
}

// ---------- data ----------

function jobsQuery() {
	const conditions = [];
	if (state.status) conditions.push(`status=${encodeURIComponent(state.status)}`);
	if (state.tenant) conditions.push(`tenant=${encodeURIComponent(state.tenant)}`);
	if (state.queue) conditions.push(`queue=${encodeURIComponent(state.queue)}`);
	return `/Job/?${[...conditions, `select(${LIST_FIELDS})`, 'sort(-createdAt)', `limit(${LIST_LIMIT})`].join('&')}`;
}

async function countStatus(status) {
	const rows = await get(`/Job/?status=${status}&select(id)&limit(${COUNT_LIMIT + 1})`);
	return rows.length > COUNT_LIMIT ? `${COUNT_LIMIT.toLocaleString()}+` : rows.length.toLocaleString();
}

async function refresh() {
	if (!state.user || state.paused || document.hidden) return;
	try {
		// Check the session first: a data request without one gets a 401 Basic challenge, and the
		// browser would show its own password box over the page.
		const session = await send('GET', '/session');
		if (!session.data.username) return showSignIn('Your session ended. Sign in again.');
		const [pending, claimed, completed, dead, unrouted, jobs, definitions] = await Promise.all([
			countStatus('pending'),
			countStatus('claimed'),
			countStatus('completed'),
			countStatus('dead'),
			get('/Event/?status=unrouted&limit(500)'),
			state.tab === 'jobs' ? get(jobsQuery()) : null,
			get('/JobDefinition/'),
		]);
		renderCounts({ pending, claimed, completed, dead, unrouted: unrouted.length.toLocaleString() });
		renderFilterOptions(definitions);
		if (jobs) {
			state.jobs = jobs;
			renderJobs();
		}
		if (state.tab === 'events') renderEvents(unrouted);
		if (state.tab === 'definitions') renderDefinitions(definitions);
		if (state.selectedJobId) await renderDetail(state.selectedJobId).catch(() => closeDetail());
		setLive('ok', `Updated ${new Date().toLocaleTimeString()}`);
		$('error').hidden = true;
	} catch (error) {
		if (error instanceof SignedOut) return showSignIn('Your session ended. Sign in again.');
		setLive('down', 'Not reachable');
		$('error').textContent = `Can't load data from Harper: ${error.message}. Retrying every ${POLL_MS / 1000} seconds.`;
		$('error').hidden = false;
	}
}

// ---------- rendering ----------

function setLive(kind, text) {
	$('live-dot').className = `dot ${kind}`;
	$('live-text').textContent = text;
}

function renderCounts(counts) {
	for (const [key, value] of Object.entries(counts)) {
		const node = $(`count-${key}`);
		node.textContent = value;
		node.classList.toggle('nonzero', value !== '0');
	}
}

function renderFilterOptions(definitions) {
	const fill = (select, values) => {
		const current = select.value;
		const wanted = ['', ...[...new Set(values.filter(Boolean))].sort()];
		const existing = [...select.options].map((o) => o.value);
		if (wanted.join('\n') === existing.join('\n')) return;
		select.replaceChildren(...wanted.map((v) => el('option', { value: v }, v || 'All')));
		select.value = wanted.includes(current) ? current : '';
	};
	fill($('filter-tenant'), definitions.map((d) => d.tenant));
	fill($('filter-queue'), definitions.map((d) => d.queue));
}

function renderJobs() {
	const needle = state.search.trim().toLowerCase();
	const rows = state.jobs.filter((job) => !needle || job.id.toLowerCase().includes(needle) || (job.definitionId ?? '').toLowerCase().includes(needle));
	$('jobs-body').replaceChildren(
		...rows.map((job) => {
			const row = el(
				'tr',
				{ className: `clickable${job.id === state.selectedJobId ? ' selected' : ''}`, tabIndex: 0 },
				el('td', { className: 'mono' }, job.id, el('span', { className: 'sub' }, job.definitionId)),
				el('td', {}, statusPill(job)),
				el('td', {}, job.tenant ?? '–'),
				el('td', { className: 'mono' }, job.queue),
				el('td', {}, job.trigger ?? '–'),
				el('td', { className: 'num' }, `${job.attempts ?? 0}/${job.maxAttempts ?? '–'}`),
				el('td', { title: fullTime(job.createdAt) }, ago(job.createdAt)),
				el('td', { title: fullTime(job.completedAt) }, job.completedAt ? ago(job.completedAt) : '–')
			);
			const open = () => selectJob(job.id);
			row.addEventListener('click', open);
			row.addEventListener('keydown', (event) => event.key === 'Enter' && open());
			return row;
		})
	);
	$('jobs-empty').hidden = rows.length > 0;
}

function renderEvents(events) {
	const rows = [...events].sort((a, b) => (b.receivedAt ?? 0) - (a.receivedAt ?? 0));
	$('events-body').replaceChildren(
		...rows.map((event) =>
			el(
				'tr',
				{},
				el('td', { className: 'mono' }, event.id),
				el('td', { className: 'mono' }, event.type),
				el('td', { className: 'mono' }, event.subject ?? '–'),
				el('td', { title: fullTime(event.receivedAt) }, ago(event.receivedAt)),
				el('td', { className: 'error-text' }, event.error ?? 'Matched no subscription')
			)
		)
	);
	$('events-empty').hidden = rows.length > 0;
}

function renderDefinitions(definitions) {
	const rows = [...definitions].sort((a, b) => a.id.localeCompare(b.id));
	$('definitions-body').replaceChildren(
		...rows.map((d) =>
			el(
				'tr',
				{},
				el('td', { className: 'mono' }, d.id),
				el('td', {}, el('span', { className: d.enabled === false ? 'pill pill-off' : 'pill pill-completed' }, d.enabled === false ? 'Off' : 'On')),
				el('td', {}, d.tenant ?? '–'),
				el('td', { className: 'mono' }, d.queue),
				el('td', { className: 'mono' }, d.handler),
				el('td', { className: 'mono' }, d.cron ? `${d.cron}${d.timezone ? ` (${d.timezone})` : ''}` : '–'),
				el('td', { title: fullTime(d.nextRunAt) }, d.cron && d.enabled !== false ? ago(d.nextRunAt) : '–')
			)
		)
	);
	$('definitions-empty').hidden = rows.length > 0;
}

function selectJob(id) {
	state.selectedJobId = id;
	$('detail').hidden = false;
	renderJobs();
	renderDetail(id);
}

async function renderDetail(id) {
	const [job, events] = await Promise.all([get(`/Job/${encodeURIComponent(id)}`), get(`/JobEvent/?jobId=${encodeURIComponent(id)}`)]);
	if (state.selectedJobId !== id) return;
	$('detail-title').textContent = id;

	const field = (label, value) => [el('dt', {}, label), el('dd', {}, value ?? '–')];
	const timeline = [...events].sort((a, b) => a.at - b.at);
	const json = (value) => el('pre', {}, JSON.stringify(value, null, 2));

	$('detail-body').replaceChildren(
		el(
			'dl',
			{ className: 'fields' },
			...field('Status', statusPill(job)),
			...field('Definition', el('span', { className: 'mono' }, job.definitionId)),
			...field('Tenant', job.tenant),
			...field('Queue', el('span', { className: 'mono' }, job.queue)),
			...field('Trigger', job.trigger),
			...field('Attempts', `${job.attempts ?? 0} of ${job.maxAttempts ?? '–'}`),
			...field('Claimed by', job.claimedBy),
			...field('Lease expires', job.status === 'claimed' ? ago(job.leaseExpiresAt) : null),
			...field('Created', fullTime(job.createdAt)),
			...field('Finished', job.completedAt ? fullTime(job.completedAt) : null)
		),
		job.lastError && el('h3', {}, 'Last error'),
		job.lastError && el('pre', { className: 'error-text' }, job.lastError),
		el('h3', {}, 'Timeline'),
		timeline.length
			? el(
					'ol',
					{ className: 'timeline' },
					...timeline.map((e) =>
						el('li', {}, el('time', { title: fullTime(e.at) }, clock(e.at)), el('div', {}, el('span', { className: `kind kind-${e.kind}` }, e.kind), ` ${e.message ?? ''}`))
					)
				)
			: el('p', { className: 'note' }, 'No events yet.'),
		el('h3', {}, 'Payload'),
		json(job.payload),
		job.result != null && el('h3', {}, 'Result'),
		job.result != null && json(job.result)
	);
}

// ---------- controls ----------

function setTab(tab) {
	state.tab = tab;
	for (const button of document.querySelectorAll('.tabs [data-tab]')) button.setAttribute('aria-selected', String(button.dataset.tab === tab));
	for (const name of ['jobs', 'events', 'definitions']) $(`tab-${name}`).hidden = name !== tab;
	refresh();
}

function setStatusFilter(status) {
	state.status = status;
	for (const chip of document.querySelectorAll('[data-filter-status]')) chip.setAttribute('aria-pressed', String(chip.dataset.filterStatus === status));
	if (state.tab !== 'jobs') setTab('jobs');
	else refresh();
}

for (const button of document.querySelectorAll('.tabs [data-tab]')) button.addEventListener('click', () => setTab(button.dataset.tab));
for (const chip of document.querySelectorAll('[data-filter-status]')) chip.addEventListener('click', () => setStatusFilter(chip.dataset.filterStatus));
for (const tile of document.querySelectorAll('.tile')) {
	tile.addEventListener('click', () => (tile.dataset.tab ? setTab(tile.dataset.tab) : setStatusFilter(tile.dataset.status)));
}
$('filter-tenant').addEventListener('change', (event) => {
	state.tenant = event.target.value;
	refresh();
});
$('filter-queue').addEventListener('change', (event) => {
	state.queue = event.target.value;
	refresh();
});
$('filter-search').addEventListener('input', (event) => {
	state.search = event.target.value;
	renderJobs();
});
$('pause').addEventListener('click', () => {
	state.paused = !state.paused;
	$('pause').textContent = state.paused ? 'Resume' : 'Pause';
	if (state.paused) setLive('paused', 'Paused');
	else refresh();
});
const closeDetail = () => {
	state.selectedJobId = null;
	$('detail').hidden = true;
	renderJobs();
};
$('detail-close').addEventListener('click', closeDetail);
document.addEventListener('keydown', (event) => event.key === 'Escape' && closeDetail());
document.addEventListener('visibilitychange', () => !document.hidden && refresh());

// ---------- sign-in ----------

function showSignIn(message) {
	state.user = null;
	closeDetail();
	$('app').hidden = true;
	$('whoami').hidden = $('sign-out').hidden = $('pause').hidden = true;
	$('error').hidden = true;
	setLive('', 'Signed out');
	$('sign-in').hidden = false;
	$('sign-in-error').textContent = message ?? '';
	$('sign-in-error').hidden = !message;
	$('sign-in-password').value = '';
	$('sign-in-username').focus();
}

function showApp(user) {
	state.user = user;
	$('sign-in').hidden = true;
	$('app').hidden = false;
	$('whoami').textContent = `${user.username} (${user.superUser ? 'administrator' : user.role ?? 'no role'})`;
	$('whoami').hidden = $('sign-out').hidden = $('pause').hidden = false;
	refresh();
}

/** On localhost, Harper's authorizeLocal signs every request in as a super user, so there is no form. */
async function checkSession(message) {
	const { status, data } = await send('GET', '/session');
	if (status === 200 && data.username) showApp(data);
	else showSignIn(message);
}

$('sign-in-form').addEventListener('submit', async (event) => {
	event.preventDefault();
	const username = $('sign-in-username').value.trim();
	const password = $('sign-in-password').value;
	if (!username || !password) {
		$('sign-in-error').textContent = 'Enter your username and password.';
		$('sign-in-error').hidden = false;
		return;
	}
	$('sign-in-submit').disabled = true;
	try {
		const { status, data } = await send('POST', '/session', { username, password });
		if (status === 200 && data.username) return showApp(data);
		$('sign-in-error').textContent = data.error ?? `Sign-in failed (${status}).`;
		$('sign-in-error').hidden = false;
	} catch (error) {
		$('sign-in-error').textContent = `Can't reach Harper: ${error.message}`;
		$('sign-in-error').hidden = false;
	} finally {
		$('sign-in-submit').disabled = false;
	}
});

$('sign-out').addEventListener('click', async () => {
	await send('DELETE', '/session').catch(() => {});
	await checkSession();
	if (state.user) {
		$('error').textContent = 'Still signed in: on localhost, Harper signs every request in as an administrator (authorizeLocal).';
		$('error').hidden = false;
	}
});

setStatusFilter('');
setTab('jobs');
checkSession();
setInterval(refresh, POLL_MS);
