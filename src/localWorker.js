/**
 * Runs Harper jobs: the handlers in handlers.js, executed inside Harper. It uses the same
 * claim, heartbeat, and complete calls as an external worker.
 *
 * Two modes, chosen by environment variables:
 *   - Queue node (default): claims straight from the Job table, skipping the HTTP hop.
 *   - Harper job node: with JOB_MANAGER_URL set, it claims from that queue node over HTTP,
 *     like an external worker. Heavy jobs then run on their own nodes, away from the API and
 *     the queue. Claims always go to the queue node because Table.lock is per node.
 *
 *   JOB_QUEUES        queues to serve, comma-separated (default "local")
 *   JOB_CONCURRENCY   jobs run at once (default 4)
 *   JOB_MANAGER_URL   the queue node; unset means this node is the queue node
 *   JOB_MANAGER_USER / JOB_MANAGER_PASS   credentials for the queue node
 */
import { logger, server } from 'harper';
import { claim, heartbeat, complete } from './queue.js';
import { handlers } from './handlers.js';

const POLL_MS = 1_000;
const HEARTBEAT_MS = 20_000;
// How long a finished job keeps trying to report its result while the queue node is unreachable.
const REPORT_RETRY_FOR_MS = 30 * 60_000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Keep sending until the queue node answers. A network error or a 5xx means "not delivered";
 * any other answer, including 409 (the lease was lost), is final.
 */
async function untilDelivered(label, send) {
	const giveUpAt = Date.now() + REPORT_RETRY_FOR_MS;
	for (let delay = 1_000; ; delay = Math.min(delay * 2, 30_000)) {
		try {
			return await send();
		} catch (error) {
			if (Date.now() + delay > giveUpAt) throw error;
			logger.warn(`job-manager worker: ${label} not delivered (${error.message}), retrying in ${delay / 1000}s`);
			await sleep(delay);
		}
	}
}

/** Claim, heartbeat, and complete over HTTP, for a job node that is not the queue node. */
function remoteQueue(baseUrl) {
	const headers = { 'content-type': 'application/json' };
	if (process.env.JOB_MANAGER_USER) {
		headers.authorization = 'Basic ' + Buffer.from(`${process.env.JOB_MANAGER_USER}:${process.env.JOB_MANAGER_PASS}`).toString('base64');
	}
	const call = async (path, body) => {
		const response = await fetch(`${baseUrl.replace(/\/$/, '')}/${path}`, { method: 'POST', headers, body: JSON.stringify(body) });
		const data = await response.json().catch(() => ({}));
		if (response.status >= 500) throw new Error(`${path} returned ${response.status}: ${data?.error ?? ''}`);
		return { status: response.status, data };
	};
	return {
		async claim(body) {
			const { status, data } = await call('claim', body);
			if (status !== 200) throw new Error(`claim from ${baseUrl} failed with ${status}: ${data?.error ?? ''}`);
			return data;
		},
		heartbeat: async (body) => (await call('heartbeat', body)).data,
		complete: async (body) => (await call('complete', body)).data,
	};
}

export function startLocalWorker() {
	const queues = (process.env.JOB_QUEUES ?? 'local').split(',').map((q) => q.trim()).filter(Boolean);
	const concurrency = Number(process.env.JOB_CONCURRENCY ?? 4);
	const remoteUrl = process.env.JOB_MANAGER_URL;
	if (remoteUrl && queues.includes('local')) {
		logger.error('job-manager: a job node cannot serve the "local" queue; it belongs to the queue node. Set JOB_QUEUES to other queues.');
		return;
	}

	const queue = remoteUrl ? remoteQueue(remoteUrl) : { claim, heartbeat, complete };
	const workerId = `${server.hostname ?? 'harper'}:${remoteUrl ? 'job-node' : 'local'}`;
	let active = 0;

	const poll = async () => {
		try {
			const free = concurrency - active;
			if (free > 0) {
				const jobs = await queue.claim({ workerId, queues, limit: free });
				for (const job of jobs) {
					active++;
					run(queue, workerId, job)
						.catch((error) => logger.error(`job-manager worker: ${job.id} could not report its result: ${error.message}`))
						.finally(() => active--);
				}
			}
		} catch (error) {
			logger.error(`job-manager worker: ${error.message}`);
		}
		setTimeout(poll, POLL_MS).unref?.();
	};

	logger.info(`job-manager: worker ${workerId} serving [${queues.join(', ')}]${remoteUrl ? ` from ${remoteUrl}` : ''}`);
	poll();
}

async function run(queue, workerId, job) {
	const report = (outcome) => untilDelivered(`complete ${job.id}`, () => queue.complete({ workerId, jobId: job.id, ...outcome }));
	const handler = handlers[job.handler];
	if (!handler) return report({ ok: false, error: `No Harper handler named "${job.handler}"` });

	// Milestones and heartbeats are best effort: if the queue node is briefly down, the next
	// heartbeat extends the lease, and the job itself must not fail because of it.
	const beat = (body) => queue.heartbeat({ workerId, jobId: job.id, ...body }).catch((error) => logger.warn(`job-manager worker: heartbeat for ${job.id} failed: ${error.message}`));
	const milestone = (message, data) => beat({ milestone: message, data });
	const keepAlive = setInterval(beat, HEARTBEAT_MS);

	// Run the job first, then report. A failed report is retried; it is never mistaken for a failed job.
	let outcome;
	try {
		outcome = { ok: true, result: await handler(job, job.settings ?? {}, { milestone }) };
	} catch (error) {
		outcome = { ok: false, error: error.message };
	} finally {
		clearInterval(keepAlive);
	}
	await report(outcome);
}
