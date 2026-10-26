/**
 * HTTP endpoints for the job manager.
 *
 *   POST /events                   publish an event; subscriptions route it to jobs
 *   POST /trigger/{definitionId}   start a run from an API call         (trigger: "api")
 *   POST /webhook/{definitionId}   start a run from a webhook           (trigger: "webhook")
 *   POST /claim                    external worker claims jobs from its queues
 *   POST /heartbeat                extend a lease, optionally record a milestone
 *   POST /complete                 report success or failure
 *
 * The tables (JobDefinition, Job, JobEvent) are also exported over REST, so
 * GET /Job/?status=dead lists the dead-letter jobs, and clients can subscribe to
 * Job changes over WebSocket, SSE, or MQTT for live status.
 */
import { Resource, server } from 'harper';
import { claim, heartbeat, complete } from './src/queue.js';
import { publish } from './src/router.js';
import { startLocalWorker } from './src/localWorker.js';

const json = (status, data) => ({ status, headers: { 'content-type': 'application/json' }, data });

/**
 * API calls and webhooks know which job they want, so they publish a direct event that names
 * its definition. They still go through the router, so every start is recorded as an Event.
 */
function triggerResource(trigger) {
	return class extends Resource {
		static async post(target, data, context) {
			const definitionId = target?.id;
			if (!definitionId) return json(400, { error: 'Missing job definition id in the path' });
			const key = context?.headers?.get?.('idempotency-key') ?? undefined;
			try {
				const { routedTo } = await publish(
					{
						id: key ? `${definitionId}:${key}` : undefined,
						type: `job.${trigger}`,
						source: `/${trigger}`,
						subject: `/definitions/${definitionId}`,
						definitionId,
						data: await data,
					},
					{ trigger, idempotencyKey: key }
				);
				const [{ jobId, deduplicated }] = routedTo;
				return json(deduplicated ? 200 : 202, { jobId, status: 'pending', deduplicated });
			} catch (error) {
				return json(error.statusCode ?? 500, { error: error.message });
			}
		}
	};
}

export const trigger = triggerResource('api');
export const webhook = triggerResource('webhook');

/**
 * POST /events: publish any event, CloudEvents style. Used by sources that don't know which
 * job they start, such as a storage "blob created" notification. Subscriptions decide.
 */
class PublishEvent extends Resource {
	static async post(target, data) {
		const event = (await data) ?? {};
		if (!event.type) return json(400, { error: 'An event needs a type' });
		try {
			const result = await publish(event);
			return json(result.status === 'routed' ? 202 : 200, result);
		} catch (error) {
			return json(error.statusCode ?? 500, { error: error.message });
		}
	}
}

class ClaimJobs extends Resource {
	static async post(target, data) {
		const { workerId, queues, limit, leaseMs } = (await data) ?? {};
		if (!workerId || !Array.isArray(queues) || queues.length === 0) {
			return json(400, { error: 'Body needs workerId and a non-empty queues array' });
		}
		if (queues.includes('local')) return json(400, { error: 'The "local" queue is reserved for in-process jobs' });
		return json(200, await claim({ workerId, queues, limit: Math.min(limit ?? 1, 50), leaseMs }));
	}
}

class HeartbeatJob extends Resource {
	static async post(target, data) {
		const body = (await data) ?? {};
		const result = await heartbeat(body);
		return json(result.ok ? 200 : 409, result.ok ? result : { ok: false, error: 'Job is not claimed by this worker' });
	}
}

class CompleteJob extends Resource {
	static async post(target, data) {
		const body = (await data) ?? {};
		const outcome = await complete(body);
		return json(outcome.accepted ? 200 : 409, outcome.accepted ? outcome : { error: 'Job is not claimed by this worker (lease expired or already finished)' });
	}
}

// Path names for the classes above.
export { PublishEvent as events, ClaimJobs as claim, HeartbeatJob as heartbeat, CompleteJob as complete };

// One Harper worker per node, on the primary worker thread (see src/localWorker.js for its settings).
if (server.workerIndex === 0) startLocalWorker();
