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

/** Claim, heartbeat, and complete over HTTP, for a job node that is not the queue node. */
function remoteQueue(baseUrl) {
	const headers = { 'content-type': 'application/json' };
	if (process.env.JOB_MANAGER_USER) {
		headers.authorization = 'Basic ' + Buffer.from(`${process.env.JOB_MANAGER_USER}:${process.env.JOB_MANAGER_PASS}`).toString('base64');
	}
	const call = async (path, body) => {
		const response = await fetch(`${baseUrl.replace(/\/$/, '')}/${path}`, { method: 'POST', headers, body: JSON.stringify(body) });
		const data = await response.json().catch(() => ({}));
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
	const handler = handlers[job.handler];
	if (!handler) {
		await queue.complete({ workerId, jobId: job.id, ok: false, error: `No Harper handler named "${job.handler}"` });
		return;
	}

	const milestone = (message, data) => queue.heartbeat({ workerId, jobId: job.id, milestone: message, data });
	const keepAlive = setInterval(() => {
		queue.heartbeat({ workerId, jobId: job.id }).catch((error) => logger.warn(`job-manager worker: heartbeat for ${job.id} failed: ${error.message}`));
	}, 20_000);
	try {
		const result = await handler(job, job.settings ?? {}, { milestone });
		await queue.complete({ workerId, jobId: job.id, ok: true, result });
	} catch (error) {
		await queue.complete({ workerId, jobId: job.id, ok: false, error: error.message });
	} finally {
		clearInterval(keepAlive);
	}
}
