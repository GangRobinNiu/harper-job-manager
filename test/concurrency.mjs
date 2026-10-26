/**
 * Concurrency check: many competing workers, no job claimed twice.
 *
 *   node test/concurrency.mjs
 *
 * To make the race easy to hit, start Harper with CLAIM_DELAY_MS=20 and several threads
 * (THREADS_COUNT=4). With Table.lock() in tryClaim, every job is claimed exactly once.
 * Replace the lock with a plain get() and the same run claims some jobs many times.
 */
const BASE = process.env.HARPER_URL ?? 'http://localhost:9926';
const JOBS = Number(process.env.JOBS ?? 300);
const WORKERS = Number(process.env.WORKERS ?? 12);
const headers = { 'content-type': 'application/json' };
const run = Date.now().toString(36);
const post = (path, body, extra = {}) => fetch(BASE + path, { method: 'POST', headers: { ...headers, ...extra }, body: JSON.stringify(body) }).then((r) => r.json());

const id = `stress-${run}`;
await fetch(`${BASE}/JobDefinition/${id}`, { method: 'PUT', headers, body: JSON.stringify({ id, enabled: true, queue: id, handler: 'jfrog/demo/noop:1.0' }) });
await Promise.all(Array.from({ length: JOBS }, (_, i) => post(`/trigger/${id}`, { i }, { 'idempotency-key': `job-${i}` })));

const claims = new Map();
const started = Date.now();
await Promise.all(
	Array.from({ length: WORKERS }, async (_, w) => {
		for (;;) {
			const jobs = await post('/claim', { workerId: `w${w}`, queues: [id], limit: 5 });
			if (!jobs.length) return;
			for (const job of jobs) {
				claims.set(job.id, (claims.get(job.id) ?? 0) + 1);
				await post('/complete', { workerId: `w${w}`, jobId: job.id, ok: true });
			}
		}
	})
);

const maxClaims = Math.max(...claims.values());
console.log(`${JOBS} jobs, ${WORKERS} workers: ${claims.size} distinct jobs claimed, max claims per job = ${maxClaims} (${Date.now() - started} ms)`);
process.exit(claims.size === JOBS && maxClaims === 1 ? 0 : 1);
