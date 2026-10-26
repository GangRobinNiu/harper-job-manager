/**
 * Runs predefined jobs (queue "local") inside Harper. It uses the same claim, heartbeat,
 * and complete calls as an external worker; it just skips the HTTP hop.
 */
import { logger, server } from 'harper';
import { claim, heartbeat, complete } from './queue.js';
import { handlers } from './handlers.js';

const POLL_MS = 1_000;
const CONCURRENCY = 4;

export function startLocalWorker() {
	const workerId = `${server.hostname ?? 'harper'}:local`;
	let active = 0;

	const poll = async () => {
		try {
			const free = CONCURRENCY - active;
			if (free > 0) {
				const jobs = await claim({ workerId, queues: ['local'], limit: free });
				for (const job of jobs) {
					active++;
					run(workerId, job).finally(() => active--);
				}
			}
		} catch (error) {
			logger.error(`job-manager local worker: ${error.message}`);
		}
		setTimeout(poll, POLL_MS).unref?.();
	};

	logger.info(`job-manager: local worker ${workerId} started`);
	poll();
}

async function run(workerId, job) {
	const handler = handlers[job.handler];
	if (!handler) {
		await complete({ workerId, jobId: job.id, ok: false, error: `No local handler named "${job.handler}"` });
		return;
	}

	const milestone = (message, data) => heartbeat({ workerId, jobId: job.id, milestone: message, data });
	const keepAlive = setInterval(() => heartbeat({ workerId, jobId: job.id }), 20_000);
	try {
		const result = await handler(job, job.settings ?? {}, { milestone });
		await complete({ workerId, jobId: job.id, ok: true, result });
	} catch (error) {
		await complete({ workerId, jobId: job.id, ok: false, error: error.message });
	} finally {
		clearInterval(keepAlive);
	}
}
