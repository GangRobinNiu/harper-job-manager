/**
 * End-to-end test against a running Harper instance (`npm run dev` in another terminal).
 *
 *   node test/e2e.mjs
 *
 * Covers: API and webhook triggers, idempotency, predefined jobs, retries with backoff,
 * dead-letter, cron, concurrent claims by competing workers, lease expiry, and rejection
 * of a late completion from a worker that lost its lease.
 */
import assert from 'node:assert/strict';

const BASE = process.env.HARPER_URL ?? 'http://localhost:9926';
const headers = { 'content-type': 'application/json' };
if (process.env.HARPER_USER) {
	headers.authorization = 'Basic ' + Buffer.from(`${process.env.HARPER_USER}:${process.env.HARPER_PASS}`).toString('base64');
}
const RUN = Date.now().toString(36);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function http(method, path, body, extraHeaders = {}) {
	const res = await fetch(`${BASE}${path}`, { method, headers: { ...headers, ...extraHeaders }, body: body === undefined ? undefined : JSON.stringify(body) });
	const text = await res.text();
	let data;
	try { data = text ? JSON.parse(text) : null; } catch { data = text; }
	return { status: res.status, data };
}

const getJob = async (id) => (await http('GET', `/Job/${encodeURIComponent(id)}`)).data;
const jobsFor = async (definitionId) => (await http('GET', `/Job/?definitionId=${encodeURIComponent(definitionId)}`)).data ?? [];
const eventsFor = async (jobId) => (await http('GET', `/JobEvent/?jobId=${encodeURIComponent(jobId)}`)).data ?? [];

async function waitFor(label, fn, timeoutMs = 30_000) {
	const end = Date.now() + timeoutMs;
	for (;;) {
		const value = await fn();
		if (value) return value;
		if (Date.now() > end) throw new Error(`Timed out waiting for: ${label}`);
		await sleep(500);
	}
}

async function define(id, definition) {
	const { status } = await http('PUT', `/JobDefinition/${id}`, { id, enabled: true, ...definition });
	assert.ok(status < 300, `define ${id}: ${status}`);
	return id;
}

const claim = (workerId, queues, limit = 1, leaseMs) => http('POST', '/claim', { workerId, queues, limit, leaseMs });
const complete = (workerId, jobId, ok, extra = {}) => http('POST', '/complete', { workerId, jobId, ok, ...extra });

let passed = 0;
async function test(name, fn) {
	const started = Date.now();
	await fn();
	passed++;
	console.log(`ok  ${name} (${((Date.now() - started) / 1000).toFixed(1)}s)`);
}

// ---------- definitions (unique per run) ----------
const echo = await define(`acme-echo-${RUN}`, { tenant: 'acme', queue: 'local', handler: 'echo', settings: { region: 'us-west' } });
const flaky = await define(`acme-flaky-${RUN}`, { tenant: 'acme', queue: 'local', handler: 'flaky', settings: { succeedOnAttempt: 2 }, maxAttempts: 3 });
const broken = await define(`acme-broken-${RUN}`, { tenant: 'acme', queue: 'local', handler: 'alwaysFails', maxAttempts: 2 });
const loader = await define(`acme-loader-${RUN}`, { tenant: 'acme', queue: `data-loader-${RUN}`, handler: 'jfrog/acme/data-loader:1.4', settings: { rows: 5000 } });
const generic = await define(`globex-sync-${RUN}`, { tenant: 'globex', queue: `generic-${RUN}`, handler: 'jfrog/globex/crm-sync:2.0' });
const hang = await define(`initech-hang-${RUN}`, { tenant: 'initech', queue: `slow-${RUN}`, handler: 'jfrog/initech/report:1.0', leaseMs: 2_000, maxAttempts: 2 });
const scheduled = await define(`acme-nightly-${RUN}`, { tenant: 'acme', queue: 'local', handler: 'echo', cron: '* * * * *', timezone: 'America/Los_Angeles' });

await test('API trigger runs a predefined job inside Harper', async () => {
	const { status, data } = await http('POST', `/trigger/${echo}`, { hello: 'world' });
	assert.equal(status, 202);
	const job = await waitFor('echo completed', async () => {
		const j = await getJob(data.jobId);
		return j?.status === 'completed' && j;
	});
	assert.deepEqual(job.result.echoed, { hello: 'world' });
	assert.equal(job.result.settings.region, 'us-west');
	assert.equal(job.trigger, 'api');
});

await test('Webhook with an Idempotency-Key is deduplicated', async () => {
	const first = await http('POST', `/webhook/${loader}`, { file: 'positions.csv' }, { 'idempotency-key': 'evt-123' });
	const second = await http('POST', `/webhook/${loader}`, { file: 'positions.csv' }, { 'idempotency-key': 'evt-123' });
	assert.equal(first.status, 202);
	assert.equal(second.status, 200);
	assert.equal(second.data.deduplicated, true);
	assert.equal(first.data.jobId, second.data.jobId);
	assert.equal((await jobsFor(loader)).length, 1);
});

await test('Unknown definition returns 404', async () => {
	const { status } = await http('POST', `/trigger/nope-${RUN}`, {});
	assert.equal(status, 404);
});

await test('External worker claims from its dedicated queue, records milestones, completes', async () => {
	const [job] = (await claim('w-loader', [`data-loader-${RUN}`])).data;
	assert.equal(job.definitionId, loader);
	assert.equal(job.handler, 'jfrog/acme/data-loader:1.4');
	assert.equal(job.settings.rows, 5000);
	assert.equal((await http('POST', '/heartbeat', { workerId: 'w-loader', jobId: job.id, milestone: 'Rows loaded', data: { rows: 5000 } })).status, 200);
	assert.equal((await complete('w-loader', job.id, true, { result: { rows: 5000 } })).status, 200);
	assert.equal((await getJob(job.id)).status, 'completed');
	const kinds = (await eventsFor(job.id)).map((e) => e.kind);
	for (const kind of ['enqueued', 'claimed', 'milestone', 'completed']) assert.ok(kinds.includes(kind), `missing event ${kind}`);
});

await test('Competing workers never claim the same job (40 jobs, 4 workers)', async () => {
	await Promise.all(Array.from({ length: 40 }, (_, i) => http('POST', `/trigger/${generic}`, { n: i })));
	const seen = new Map();
	const worker = async (workerId) => {
		for (;;) {
			const { data } = await claim(workerId, [`generic-${RUN}`], 3);
			if (!data.length) return;
			for (const job of data) {
				seen.set(job.id, (seen.get(job.id) ?? 0) + 1);
				assert.equal((await complete(workerId, job.id, true)).status, 200);
			}
		}
	};
	await Promise.all(['w1', 'w2', 'w3', 'w4'].map(worker));
	assert.equal(seen.size, 40, `claimed ${seen.size} distinct jobs`);
	assert.ok([...seen.values()].every((n) => n === 1), 'a job was claimed twice');
	assert.ok((await jobsFor(generic)).every((j) => j.status === 'completed' && j.attempts === 1));
});

await test('Flaky predefined job retries with backoff, then succeeds', async () => {
	const { data } = await http('POST', `/trigger/${flaky}`, {});
	const job = await waitFor('flaky completed', async () => {
		const j = await getJob(data.jobId);
		return j?.status === 'completed' && j;
	}, 30_000);
	assert.equal(job.attempts, 2);
	assert.match(job.lastError, /attempt 1/);
});

await test('Job that always fails ends in the dead-letter state after maxAttempts', async () => {
	const { data } = await http('POST', `/trigger/${broken}`, {});
	const job = await waitFor('broken dead', async () => {
		const j = await getJob(data.jobId);
		return j?.status === 'dead' && j;
	}, 30_000);
	assert.equal(job.attempts, 2);
	const dead = (await http('GET', `/Job/?status=dead&definitionId=${broken}`)).data;
	assert.equal(dead.length, 1);
});

await test('Expired lease: job is retried, the late worker is rejected, then it goes dead', async () => {
	const { data } = await http('POST', `/trigger/${hang}`, {});
	const [first] = (await claim('w-stuck', [`slow-${RUN}`])).data;
	assert.equal(first.id, data.jobId);
	// The worker hangs past its 2s lease; the scheduler tick (every 5s) reaps it.
	await waitFor('lease reaped', async () => (await getJob(first.id)).status === 'pending', 20_000);
	const late = await complete('w-stuck', first.id, true);
	assert.equal(late.status, 409, 'late completion must be rejected');
	// Retry after backoff goes to another worker, which also stalls: attempt 2 of 2 -> dead.
	const second = await waitFor('claimable again', async () => (await claim('w-other', [`slow-${RUN}`])).data[0], 20_000);
	assert.equal(second.attempts, 2);
	const job = await waitFor('dead after second lease', async () => {
		const j = await getJob(first.id);
		return j.status === 'dead' && j;
	}, 20_000);
	assert.match(job.lastError, /Lease expired/);
});

// ---------- event router ----------
const container = `/containers/input-${RUN}/blobs/`;
const tenantLoader = await define(`acme-data-loader-${RUN}`, { tenant: 'acme', queue: `dl-${RUN}`, handler: 'jfrog/acme/data-loader:1.4' });
const auditLog = await define(`audit-log-${RUN}`, { queue: 'local', handler: 'echo' });
await http('PUT', `/EventSubscription/blob-jobs-${RUN}`, {
	id: `blob-jobs-${RUN}`, enabled: true, eventTypes: ['storage.blob.created'],
	subjectBeginsWith: container,
	subjectPattern: `^/containers/input-${RUN}/blobs/(?<tenant>[^/]+)/(?<jobType>[^/]+)/`,
	definitionTemplate: `{tenant}-{jobType}-${RUN}`,
});
await http('PUT', `/EventSubscription/blob-audit-${RUN}`, {
	id: `blob-audit-${RUN}`, enabled: true, eventTypes: ['storage.blob.created'], subjectBeginsWith: container, definitionId: auditLog,
});
const blobEvent = (path, data = {}) => ({ id: `blob-${RUN}-${path}`, type: 'storage.blob.created', source: '/storage/input', subject: `${container}${path}`, data: { size: 1024, ...data } });

await test('Blob event is routed by its path: tenant and job type come from the folders, fan-out to audit', async () => {
	const { status, data } = await http('POST', '/events', blobEvent('acme/data-loader/positions.csv'));
	assert.equal(status, 202);
	assert.equal(data.status, 'routed');
	const byDefinition = Object.fromEntries(data.routedTo.map((r) => [r.definitionId, r]));
	assert.ok(byDefinition[tenantLoader], 'routed to the tenant data loader');
	assert.ok(byDefinition[auditLog], 'fanned out to the audit job');
	const job = await getJob(byDefinition[tenantLoader].jobId);
	assert.equal(job.trigger, 'event');
	assert.equal(job.queue, `dl-${RUN}`);
	assert.deepEqual(job.context.matched, { tenant: 'acme', jobType: 'data-loader' });
	assert.equal(job.payload.size, 1024);
});

await test('Publishing the same event twice does not create duplicate jobs', async () => {
	const { data } = await http('POST', '/events', blobEvent('acme/data-loader/positions.csv'));
	assert.ok(data.routedTo.every((r) => r.deduplicated));
	assert.equal((await jobsFor(tenantLoader)).length, 1);
});

await test('Event can pin an image version; the worker gets that image', async () => {
	await http('POST', '/events', blobEvent('acme/data-loader/canary.csv', { imageVersion: '1.5-rc' }));
	const jobs = [];
	for (;;) {
		const { data } = await claim('w-dl', [`dl-${RUN}`], 5);
		if (!data.length) break;
		jobs.push(...data);
	}
	const canary = jobs.find((j) => j.payload.imageVersion === '1.5-rc');
	const normal = jobs.find((j) => !j.payload.imageVersion);
	assert.equal(canary.handler, 'jfrog/acme/data-loader:1.5-rc');
	assert.equal(normal.handler, 'jfrog/acme/data-loader:1.4');
});

await test('Event for a folder with no job definition: audit still runs, the gap is recorded', async () => {
	const { data } = await http('POST', '/events', blobEvent('globex/unknown-type/x.csv'));
	assert.deepEqual(data.routedTo.map((r) => r.definitionId), [auditLog]);
	assert.match(data.errors[0], /Unknown job definition: globex-unknown-type/);
});

await test('Event that matches no subscription is kept as unrouted (the router dead letter)', async () => {
	const { status, data } = await http('POST', '/events', { id: `del-${RUN}`, type: 'storage.blob.deleted', subject: `${container}acme/data-loader/old.csv` });
	assert.equal(status, 200);
	assert.equal(data.status, 'unrouted');
	const stored = (await http('GET', `/Event/del-${RUN}`)).data;
	assert.equal(stored.status, 'unrouted');
});

await test('Cron definition enqueues a run on schedule (waits for the next minute)', async () => {
	const job = await waitFor('cron run', async () => {
		const jobs = await jobsFor(scheduled);
		return jobs.find((j) => j.trigger === 'cron' && j.status === 'completed');
	}, 140_000);
	assert.match(job.id, /:cron-\d+$/);
	assert.ok(job.payload.scheduledAt);
});

console.log(`\n${passed} tests passed`);
