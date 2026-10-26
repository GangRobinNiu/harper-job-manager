/**
 * The job queue: enqueue, claim, heartbeat, complete, and retry.
 *
 * Every state change on a job happens under Harper's record lock (`Table.lock`), so two
 * workers can never claim the same job, and a worker whose lease expired cannot complete
 * a job that was already handed to someone else.
 */
import { createHash } from 'node:crypto';
import { databases, transaction, logger } from 'harper';

const { Job, JobDefinition, JobEvent } = databases.jobs;

export const STATUS = { pending: 'pending', claimed: 'claimed', completed: 'completed', dead: 'dead' };

export const DEFAULTS = { maxAttempts: 3, leaseMs: 60_000, priority: 2 };

const BACKOFF_BASE_MS = 5_000;

/**
 * Keys from outside (webhook ids, file paths) can contain "/" or other characters that break
 * REST paths like /Job/{id}. Keep simple keys readable; hash anything else.
 */
export function safeKey(key) {
	const value = String(key);
	if (/^[A-Za-z0-9._:-]{1,120}$/.test(value)) return value;
	return 'h-' + createHash('sha256').update(value).digest('base64url').slice(0, 24);
}


const BACKOFF_MAX_MS = 5 * 60_000;

/** Retry delay: 5s, 10s, 20s, ... capped at 5 minutes. */
export function backoffMs(attempts) {
	return Math.min(BACKOFF_BASE_MS * 2 ** Math.max(attempts - 1, 0), BACKOFF_MAX_MS);
}

export async function recordEvent(jobId, kind, message, data) {
	await JobEvent.put({ id: `${jobId}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`, jobId, at: Date.now(), kind, message, data });
	logger.info(`job-manager: ${jobId} ${kind}: ${message}`);
}

/**
 * Create a run of a job definition.
 *
 * With an idempotency key, the job id is derived from it, so a retried webhook, a double
 * cron fire after leader failover, or a client resubmitting the same request all land on
 * the same job instead of creating duplicates.
 */
export async function enqueue({ definitionId, trigger, payload, idempotencyKey, priority, runAfter, eventId, context }) {
	const definition = await JobDefinition.get(definitionId);
	if (!definition) {
		const error = new Error(`Unknown job definition: ${definitionId}`);
		error.statusCode = 404;
		throw error;
	}
	if (definition.enabled === false) {
		const error = new Error(`Job definition is disabled: ${definitionId}`);
		error.statusCode = 409;
		throw error;
	}

	const id = idempotencyKey ? `${definitionId}:${safeKey(idempotencyKey)}` : `${definitionId}:${crypto.randomUUID()}`;

	return transaction(async (txn) => {
		const existing = await Job.lock(id, undefined, txn);
		if (existing?.status) {
			// A locked record exposes fields as getters; read a plain copy after the lock.
			return { job: null, deduplicated: true };
		}

		const job = {
			id,
			definitionId,
			tenant: definition.tenant ?? null,
			queue: definition.queue,
			trigger,
			eventId: eventId ?? null,
			idempotencyKey: idempotencyKey ?? null,
			payload: payload ?? null,
			context: context ?? null,
			status: STATUS.pending,
			priority: priority ?? definition.priority ?? DEFAULTS.priority,
			attempts: 0,
			maxAttempts: definition.maxAttempts ?? DEFAULTS.maxAttempts,
			runAfter: runAfter ?? 0,
			leaseExpiresAt: 0,
			claimedBy: null,
			lastError: null,
		};
		await Job.put(id, job, txn);
		return { job, deduplicated: false };
	}).then(async (result) => {
		if (result.deduplicated) return { job: await Job.get(id), deduplicated: true };
		await recordEvent(id, 'enqueued', `Triggered by ${trigger}`);
		return result;
	});
}

/**
 * The event says who and what (tenant, job type); the definition says how (image).
 * An event may pin an image version, e.g. to test a new image on one tenant or roll back.
 */
export function pinImageVersion(handler, version) {
	if (!handler || !version) return handler;
	const lastSlash = handler.lastIndexOf('/');
	const colon = handler.indexOf(':', lastSlash + 1);
	return `${colon === -1 ? handler : handler.slice(0, colon)}:${version}`;
}

/**
 * Claim up to `limit` runnable jobs from the given queues, highest priority first.
 * Each claim gets a lease; a worker must heartbeat or complete before it expires.
 */
export async function claim({ workerId, queues, limit = 1, leaseMs }) {
	const now = Date.now();
	const claimed = [];

	for (const queue of queues) {
		if (claimed.length >= limit) break;

		const candidates = Job.search({
			conditions: [
				{ attribute: 'queue', value: queue },
				{ attribute: 'status', value: STATUS.pending },
				{ attribute: 'runAfter', comparator: 'less_than_equal', value: now },
			],
			sort: { attribute: 'priority' },
			// Read a few extra: another worker may claim some of them first.
			limit: (limit - claimed.length) * 3,
		});

		for await (const candidate of candidates) {
			if (claimed.length >= limit) break;
			const job = await tryClaim(candidate.id, workerId, leaseMs);
			if (job) claimed.push(job);
		}
	}

	// Hand the worker everything it needs: the definition's handler and tenant settings.
	return Promise.all(
		claimed.map(async (job) => {
			const definition = await JobDefinition.get(job.definitionId);
			await recordEvent(job.id, 'claimed', `Attempt ${job.attempts} claimed by ${workerId}`);
			return { ...job, handler: pinImageVersion(definition?.handler, job.payload?.imageVersion ?? job.context?.matched?.imageVersion), settings: definition?.settings ?? {} };
		})
	);
}

async function tryClaim(id, workerId, leaseMs) {
	return transaction(async (txn) => {
		const record = await Job.lock(id, undefined, txn);
		// Someone else got it between the search and the lock.
		if (!record || record.status !== STATUS.pending) return null;
		// Test hook: widen the window between check and write to prove the lock prevents double claims.
		if (process.env.CLAIM_DELAY_MS) await new Promise((resolve) => setTimeout(resolve, Number(process.env.CLAIM_DELAY_MS)));

		const definition = await JobDefinition.get(record.definitionId);
		const lease = leaseMs ?? definition?.leaseMs ?? DEFAULTS.leaseMs;
		const update = {
			status: STATUS.claimed,
			claimedBy: workerId,
			attempts: (record.attempts ?? 0) + 1,
			leaseExpiresAt: Date.now() + lease,
		};
		await Job.patch(id, update, txn);
		return true;
	}).then(async (won) => (won ? { ...(await Job.get(id)) } : null));
}

/**
 * Extend the lease and optionally record a milestone.
 * Fails if the job is no longer this worker's, so a stalled worker learns it lost the job.
 */
export async function heartbeat({ workerId, jobId, extendMs, milestone, data }) {
	const result = await transaction(async (txn) => {
		const record = await Job.lock(jobId, undefined, txn);
		if (!record || record.status !== STATUS.claimed || record.claimedBy !== workerId) return { ok: false };
		const definition = await JobDefinition.get(record.definitionId);
		const lease = extendMs ?? definition?.leaseMs ?? DEFAULTS.leaseMs;
		await Job.patch(jobId, { leaseExpiresAt: Date.now() + lease }, txn);
		return { ok: true };
	});
	if (result.ok && milestone) await recordEvent(jobId, 'milestone', milestone, data);
	return result;
}

/**
 * Finish a run. On failure, retry with backoff until maxAttempts, then mark it dead.
 * Only the worker that holds the current lease may complete it.
 */
export async function complete({ workerId, jobId, ok, result, error }) {
	const outcome = await transaction(async (txn) => {
		const record = await Job.lock(jobId, undefined, txn);
		if (!record || record.status !== STATUS.claimed || record.claimedBy !== workerId) {
			return { accepted: false };
		}
		if (ok) {
			await Job.patch(jobId, { status: STATUS.completed, result: result ?? null, completedAt: Date.now(), leaseExpiresAt: 0 }, txn);
			return { accepted: true, status: STATUS.completed };
		}
		return { accepted: true, ...(await failAttempt(record, error ?? 'Unknown error', txn)) };
	});

	if (outcome.accepted) {
		const message = ok ? 'Completed' : outcome.status === STATUS.dead ? `Dead after ${outcome.attempts} attempts: ${error}` : `Failed, retry in ${outcome.retryInMs} ms: ${error}`;
		await recordEvent(jobId, ok ? 'completed' : outcome.status === STATUS.dead ? 'dead' : 'failed', message);
	}
	return outcome;
}

/** Shared by worker-reported failures and expired leases. Runs inside the caller's lock. */
async function failAttempt(record, error, txn) {
	const attempts = record.attempts ?? 0;
	const maxAttempts = record.maxAttempts ?? DEFAULTS.maxAttempts;

	if (attempts >= maxAttempts) {
		await Job.patch(record.id, { status: STATUS.dead, lastError: error, claimedBy: null, leaseExpiresAt: 0, completedAt: Date.now() }, txn);
		return { status: STATUS.dead, attempts };
	}

	const retryInMs = backoffMs(attempts);
	await Job.patch(record.id, { status: STATUS.pending, lastError: error, claimedBy: null, leaseExpiresAt: 0, runAfter: Date.now() + retryInMs }, txn);
	return { status: STATUS.pending, attempts, retryInMs };
}

/** Find claimed jobs whose lease ran out (a crashed or stuck worker) and retry or kill them. */
export async function reapExpiredLeases() {
	const now = Date.now();
	const expired = Job.search({
		conditions: [
			{ attribute: 'status', value: STATUS.claimed },
			{ attribute: 'leaseExpiresAt', comparator: 'less_than', value: now },
		],
	});

	let count = 0;
	for await (const candidate of expired) {
		const outcome = await transaction(async (txn) => {
			const record = await Job.lock(candidate.id, undefined, txn);
			// Re-check under the lock: the worker may have just completed or renewed it.
			if (!record || record.status !== STATUS.claimed || record.leaseExpiresAt >= Date.now()) return null;
			return failAttempt(record, `Lease expired (worker ${record.claimedBy})`, txn);
		});
		if (outcome) {
			count++;
			await recordEvent(candidate.id, outcome.status === STATUS.dead ? 'dead' : 'lease-expired', `Lease expired; ${outcome.status === STATUS.dead ? 'dead' : 'will retry'}`);
		}
	}
	if (count) logger.info(`job-manager: reaped ${count} expired leases`);
	return count;
}
