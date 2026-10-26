/**
 * The scheduler tick. Harper's built-in scheduler calls this every few seconds, once per
 * cluster (see `scheduler:` in config.yaml).
 *
 * The tick never runs a job. It publishes a "schedule.fired" event for each due cron run,
 * and reaps expired leases.
 * Every job, whatever started it, then goes through the same router, queue, retries,
 * and dead-letter handling.
 */
import { databases, logger } from 'harper';
import { CronExpressionParser } from 'cron-parser';
import { reapExpiredLeases, DEFAULTS } from './queue.js';
import { publish } from './router.js';

const { JobDefinition } = databases.jobs;

export function nextRunAfter(cron, timezone, after) {
	return CronExpressionParser.parse(cron, { currentDate: new Date(after), tz: timezone || undefined })
		.next()
		.getTime();
}

export async function enqueueDueCronJobs(now = Date.now()) {
	let count = 0;
	const definitions = JobDefinition.search({
		conditions: [{ attribute: 'enabled', value: true }],
	});

	for await (const definition of definitions) {
		if (!definition.cron) continue;
		try {
			// First time we see a schedule: set its next run, don't fire immediately.
			if (!definition.nextRunAt) {
				await JobDefinition.patch(definition.id, { nextRunAt: nextRunAfter(definition.cron, definition.timezone, now) });
				continue;
			}
			if (definition.nextRunAt > now) continue;

			// The scheduled time is the idempotency key, so a double fire during leader
			// failover lands on the same job instead of running twice.
			const key = `cron-${definition.nextRunAt}`;
			await publish(
				{
					id: `${definition.id}:${key}`,
					type: 'schedule.fired',
					source: '/scheduler',
					subject: `/definitions/${definition.id}`,
					definitionId: definition.id,
					data: { scheduledAt: new Date(definition.nextRunAt).toISOString() },
				},
				{ trigger: 'cron', idempotencyKey: key }
			);
			// Catch up at most once: skip any occurrences missed while we were down.
			await JobDefinition.patch(definition.id, { nextRunAt: nextRunAfter(definition.cron, definition.timezone, now) });
			count++;
		} catch (error) {
			logger.error(`job-manager: cron enqueue failed for ${definition.id}: ${error.message}`);
		}
	}
	return count;
}

// After a restart every lease looks expired, because no heartbeats could arrive while this node
// was down. Wait one default lease (workers heartbeat every 20s) so they can reconnect first.
const REAP_GRACE_MS = Number(process.env.REAP_GRACE_MS ?? DEFAULTS.leaseMs);
const startedAt = Date.now();
let warnedJobNode = false;

export async function tick() {
	// Cron and lease reaping belong to the queue node. A job node only gets here if it joined
	// the queue node's cluster and won the scheduler election, which leaves the queue unticked.
	if (process.env.JOB_MANAGER_URL) {
		if (!warnedJobNode) logger.error('job-manager: this job node is the scheduler leader, so cron runs and lease reaping are not running. Job nodes must not join the queue node\'s cluster.');
		warnedJobNode = true;
		return;
	}
	const enqueued = await enqueueDueCronJobs();
	const reaped = Date.now() - startedAt >= REAP_GRACE_MS ? await reapExpiredLeases() : 0;
	if (enqueued || reaped) logger.info(`job-manager tick: ${enqueued} cron runs enqueued, ${reaped} leases reaped`);
}
