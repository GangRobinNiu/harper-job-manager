/**
 * A sample external worker: the part that would run outside Harper.
 *
 * It claims jobs from its queues over HTTP, runs them, sends milestones, and reports the
 * result. In production, `runJob` would start a container from `job.handler` (an image
 * reference) with the tenant's settings, for example as a Kubernetes Job, the way the
 * Service Platform dispatcher did. Here it simulates the work so the flow can be tested.
 *
 * Usage:
 *   HARPER_URL=http://localhost:9926 QUEUES=data-loader,generic node worker/external-worker.mjs
 *   Optional: HARPER_USER / HARPER_PASS (outside dev mode), WORKER_ID, CONCURRENCY, ONCE=1
 */

const HARPER_URL = process.env.HARPER_URL ?? 'http://localhost:9926';
const QUEUES = (process.env.QUEUES ?? 'generic').split(',').map((q) => q.trim());
const WORKER_ID = process.env.WORKER_ID ?? `external-${process.pid}`;
const CONCURRENCY = Number(process.env.CONCURRENCY ?? 2);
const POLL_MS = 1_000;

const headers = { 'content-type': 'application/json' };
if (process.env.HARPER_USER) {
	headers.authorization = 'Basic ' + Buffer.from(`${process.env.HARPER_USER}:${process.env.HARPER_PASS}`).toString('base64');
}

async function call(path, body) {
	const response = await fetch(`${HARPER_URL}/${path}`, { method: 'POST', headers, body: JSON.stringify(body) });
	const data = await response.json().catch(() => ({}));
	return { status: response.status, data };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** Simulated work. Settings can ask for a slow run or a failure, to exercise leases and retries. */
async function runJob(job, milestone) {
	const settings = job.settings ?? {};
	await milestone(`Starting ${job.handler} for tenant ${job.tenant ?? 'n/a'}`);
	if (settings.hangForMs) await sleep(settings.hangForMs); // no heartbeats: the lease will expire
	if (settings.failOnAttemptsBelow && job.attempts < settings.failOnAttemptsBelow) {
		throw new Error(`Simulated failure on attempt ${job.attempts}`);
	}
	await sleep(settings.workMs ?? 300);
	await milestone('Rows loaded', { rows: settings.rows ?? 1000 });
	return { image: job.handler, rows: settings.rows ?? 1000 };
}

let active = 0;

async function handle(job) {
	const milestone = (message, data) => call('heartbeat', { workerId: WORKER_ID, jobId: job.id, milestone: message, data });
	try {
		const result = await runJob(job, milestone);
		const { status, data } = await call('complete', { workerId: WORKER_ID, jobId: job.id, ok: true, result });
		console.log(`${job.id}: completed (${status})`, status === 409 ? data.error : '');
	} catch (error) {
		const { status, data } = await call('complete', { workerId: WORKER_ID, jobId: job.id, ok: false, error: error.message });
		console.log(`${job.id}: failed (${status})`, data.status ?? data.error ?? '');
	}
}

async function loop() {
	console.log(`Worker ${WORKER_ID} polling ${QUEUES.join(', ')} at ${HARPER_URL}`);
	for (;;) {
		const free = CONCURRENCY - active;
		if (free > 0) {
			const { status, data } = await call('claim', { workerId: WORKER_ID, queues: QUEUES, limit: free }).catch((error) => ({ status: 0, data: error.message }));
			if (status === 200 && Array.isArray(data)) {
				for (const job of data) {
					console.log(`${job.id}: claimed, attempt ${job.attempts}, image ${job.handler}`);
					active++;
					handle(job).finally(() => active--);
				}
			} else if (status !== 200) {
				console.error('Claim failed:', status, data);
			}
		}
		if (process.env.ONCE && active === 0) break;
		await sleep(POLL_MS);
	}
}

loop();
