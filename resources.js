/**
 * HTTP endpoints for the job manager.
 *
 *   POST /events                   publish an event; subscriptions route it to jobs
 *   POST /trigger/{definitionId}   start a run from an API call         (trigger: "api")
 *   POST /webhook/{definitionId}   start a run from a webhook           (trigger: "webhook")
 *   POST /claim                    external worker claims jobs from its queues
 *   POST /heartbeat                extend a lease, optionally record a milestone
 *   POST /complete                 report success or failure
 *   GET|POST|DELETE /session       who is signed in, sign in, sign out (for the admin UI)
 *
 * The tables (JobDefinition, Job, JobEvent) are also exported over REST, so
 * GET /Job/?status=dead lists the dead-letter jobs, and clients can subscribe to
 * Job changes over WebSocket, SSE, or MQTT for live status.
 */
import { Resource, databases, server } from 'harper';
import { claim, heartbeat, complete } from './src/queue.js';
import { publish } from './src/router.js';
import { startLocalWorker } from './src/localWorker.js';

const json = (status, data) => ({ status, headers: { 'content-type': 'application/json' }, data });

const isSuperUser = (user) => user?.role?.permission?.super_user === true;

/**
 * Static endpoints get no permission check from Harper, so every endpoint that changes jobs
 * checks here. Only super users may; read-only roles such as job_viewer only read the tables.
 * Returns a response to send back, or null when the caller may go ahead.
 */
function requireSuperUser(context) {
	if (isSuperUser(context?.user)) return null;
	return context?.user ? json(403, { error: 'This needs a job manager administrator (a super user)' }) : json(401, { error: 'Sign in first' });
}

/**
 * API calls and webhooks know which job they want, so they publish a direct event that names
 * its definition. They still go through the router, so every start is recorded as an Event.
 */
function triggerResource(trigger) {
	return class extends Resource {
		static async post(target, data, context) {
			const denied = requireSuperUser(context);
			if (denied) return denied;
			const definitionId = target?.id;
			if (!definitionId) return json(400, { error: 'Missing job definition id in the path' });
			const key = context?.headers?.get?.('idempotency-key');
			// Required so a caller can safely retry: a retry with the same key lands on the same job.
			if (!key) return json(400, { error: 'An Idempotency-Key header is required, so a retried request cannot create a second job' });
			try {
				const { routedTo } = await publish(
					{
						id: `${definitionId}:${key}`,
						type: `job.${trigger}`,
						source: `/${trigger}`,
						subject: `/definitions/${definitionId}`,
						definitionId,
						data: await data,
					},
					{ trigger, idempotencyKey: key }
				);
				const [{ jobId, deduplicated }] = routedTo;
				// A duplicate points at the existing job, which may already be running or finished.
				const status = deduplicated ? (await databases.jobs.Job.get(jobId))?.status : 'pending';
				return json(deduplicated ? 200 : 202, { jobId, status, deduplicated });
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
	static async post(target, data, context) {
		const denied = requireSuperUser(context);
		if (denied) return denied;
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
	static async post(target, data, context) {
		const denied = requireSuperUser(context);
		if (denied) return denied;
		const { workerId, queues, limit, leaseMs } = (await data) ?? {};
		if (!workerId || !Array.isArray(queues) || queues.length === 0) {
			return json(400, { error: 'Body needs workerId and a non-empty queues array' });
		}
		if (queues.includes('local')) return json(400, { error: 'The "local" queue is reserved for in-process jobs' });
		return json(200, await claim({ workerId, queues, limit: Math.min(limit ?? 1, 50), leaseMs }));
	}
}

class HeartbeatJob extends Resource {
	static async post(target, data, context) {
		const denied = requireSuperUser(context);
		if (denied) return denied;
		const body = (await data) ?? {};
		const result = await heartbeat(body);
		return json(result.ok ? 200 : 409, result.ok ? result : { ok: false, error: 'Job is not claimed by this worker' });
	}
}

class CompleteJob extends Resource {
	static async post(target, data, context) {
		const denied = requireSuperUser(context);
		if (denied) return denied;
		const body = (await data) ?? {};
		const outcome = await complete(body);
		return json(outcome.accepted ? 200 : 409, outcome.accepted ? outcome : { error: 'Job is not claimed by this worker (lease expired or already finished)' });
	}
}

/**
 * Sign-in for the admin UI, on Harper's session cookies.
 *
 *   GET    /session   who is signed in: { username, role, superUser }, username null if nobody
 *   POST   /session   { username, password } signs in and sets the session cookie
 *   DELETE /session   signs out
 *
 * A wrong password answers 403, not 401: a 401 makes Harper add a Basic challenge, and the
 * browser would pop up its own password box over the page.
 */
class AdminSession extends Resource {
	static async get(target, context) {
		const user = context?.user;
		return json(200, { username: user?.username ?? null, role: user?.role?.role ?? null, superUser: isSuperUser(user) });
	}

	static async post(target, data, context) {
		const { username, password } = (await data) ?? {};
		if (!username || !password) return json(400, { error: 'Enter a username and password' });
		if (typeof context?.login !== 'function') return json(500, { error: 'Sessions are off in this Harper (authentication.enableSessions)' });
		try {
			await context.login(username, password);
		} catch {
			return json(403, { error: 'Wrong username or password' });
		}
		return AdminSession.get(target, context);
	}

	static async delete(target, context) {
		await context?.session?.update?.({ user: null });
		return json(200, { username: null });
	}
}

// Path names for the classes above.
export { PublishEvent as events, ClaimJobs as claim, HeartbeatJob as heartbeat, CompleteJob as complete, AdminSession as session };

// One Harper worker per node, on the primary worker thread (see src/localWorker.js for its settings).
if (server.workerIndex === 0) startLocalWorker();
