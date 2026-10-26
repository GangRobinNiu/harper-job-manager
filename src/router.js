/**
 * The event router: this project's equivalent of Azure Event Grid.
 *
 * Every way a job can start is an event published here: a file landing, a cron schedule,
 * a webhook, an API call. The router records the event, decides which job definitions it
 * should start, and enqueues one job per match. It does not hold work: the Job table is
 * the queue (Service Bus in the old design); the router only decides where work goes.
 *
 * Two ways to route:
 *   1. Direct: the event names its job definition (`definitionId`). Cron, webhooks, and API
 *      calls know who they are, so they use this.
 *   2. Subscriptions: rules match on event type and subject, like Event Grid subscriptions.
 *      A blob-created event only carries a file path, so a rule reads the tenant and job type
 *      from the path ("{tenant}/{jobType}/file.csv") and builds the definition id.
 *
 * One event can match several subscriptions (fan-out). An event that matches nothing is kept
 * with status "unrouted": the router's dead letter.
 *
 * Event shape follows CloudEvents: { id, type, source, subject, data }, plus an optional
 * `definitionId` extension for direct routing.
 */
import { databases, logger } from 'harper';
import { enqueue, safeKey } from './queue.js';

const { Event, EventSubscription } = databases.jobs;

// Compiled subject patterns, cached by source string.
const patterns = new Map();
function compile(pattern) {
	if (!patterns.has(pattern)) patterns.set(pattern, new RegExp(pattern));
	return patterns.get(pattern);
}

/** Fill "{tenant}-{jobType}" from named groups. Returns null if a group is missing. */
function fillTemplate(template, groups) {
	let missing = false;
	const value = template.replace(/\{(\w+)\}/g, (_, name) => {
		if (groups[name] === undefined) missing = true;
		return groups[name];
	});
	return missing ? null : value;
}

/** Does this subscription accept the event? Returns the named groups from the subject, or null. */
export function matchSubscription(subscription, event) {
	if (subscription.enabled === false) return null;
	const types = subscription.eventTypes;
	if (Array.isArray(types) && types.length && !types.includes(event.type)) return null;

	const subject = event.subject ?? '';
	if (subscription.subjectBeginsWith && !subject.startsWith(subscription.subjectBeginsWith)) return null;
	if (subscription.subjectPattern) {
		const match = compile(subscription.subjectPattern).exec(subject);
		if (!match) return null;
		return { ...(match.groups ?? {}) };
	}
	return {};
}

async function routesFor(event) {
	if (event.definitionId) {
		return [{ subscriptionId: null, definitionId: event.definitionId, groups: {}, priority: undefined }];
	}

	const routes = [];
	for await (const subscription of EventSubscription.search({ conditions: [{ attribute: 'enabled', value: true }] })) {
		const groups = matchSubscription(subscription, event);
		if (!groups) continue;
		const definitionId = subscription.definitionId ?? (subscription.definitionTemplate ? fillTemplate(subscription.definitionTemplate, groups) : null);
		if (definitionId) routes.push({ subscriptionId: subscription.id, definitionId, groups, priority: subscription.priority ?? undefined });
	}
	return routes;
}

/**
 * Publish an event. Returns where it went.
 *
 * Idempotent by event id: publishing the same event twice routes to the same jobs, because
 * each job's idempotency key is built from the event id.
 */
export async function publish(event, { trigger = 'event', idempotencyKey } = {}) {
	const sourceId = event.id ?? crypto.randomUUID();
	const id = safeKey(sourceId);
	const record = {
		id,
		sourceId: String(sourceId),
		type: event.type,
		source: event.source ?? null,
		subject: event.subject ?? null,
		data: event.data ?? null,
		receivedAt: Date.now(),
	};

	const routes = await routesFor(event);
	const routedTo = [];
	const errors = [];

	for (const route of routes) {
		try {
			const { job, deduplicated } = await enqueue({
				definitionId: route.definitionId,
				trigger,
				eventId: id,
				idempotencyKey: idempotencyKey ?? `evt-${id}`,
				priority: route.priority,
				payload: event.data ?? null,
				context: {
					event: { id, type: event.type, subject: event.subject ?? null, source: event.source ?? null },
					subscriptionId: route.subscriptionId,
					matched: route.groups,
				},
			});
			routedTo.push({ subscriptionId: route.subscriptionId, definitionId: route.definitionId, jobId: job.id, deduplicated });
		} catch (error) {
			// A rule pointed at a definition that doesn't exist (e.g. a new tenant folder): record it, keep going.
			errors.push(`${route.definitionId}: ${error.message}`);
			if (event.definitionId) {
				error.eventId = id;
				await Event.put(id, { ...record, status: 'unrouted', routedTo, error: errors.join('; ') });
				throw error;
			}
		}
	}

	const status = routedTo.length ? 'routed' : 'unrouted';
	await Event.put(id, { ...record, status, routedTo, error: errors.length ? errors.join('; ') : null });
	if (status === 'unrouted') logger.warn(`job-manager router: event ${id} (${event.type} ${event.subject ?? ''}) matched no job`);
	return { eventId: id, status, routedTo, errors };
}
