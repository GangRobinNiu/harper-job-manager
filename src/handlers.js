/**
 * Predefined jobs that run inside Harper (queue "local").
 * Each handler gets the job (payload, tenant, attempts) and the definition's settings,
 * plus `milestone()` to record progress. Return a value to store it as the job's result.
 */
import { databases } from 'harper';

const { Job, JobEvent } = databases.jobs;

export const handlers = {
	/** Returns its input. Useful for testing triggers end to end. */
	async echo(job, settings) {
		return { echoed: job.payload, tenant: job.tenant, settings };
	},

	/** Fails until `settings.succeedOnAttempt`, to show retries with backoff. */
	async flaky(job, settings, { milestone }) {
		await milestone(`Attempt ${job.attempts}`);
		if (job.attempts < (settings.succeedOnAttempt ?? 2)) throw new Error(`Simulated failure on attempt ${job.attempts}`);
		return { succeededOnAttempt: job.attempts };
	},

	/** Waits `settings.ms` (default 5s), for testing long-running jobs and outages. */
	async sleep(job, settings, { milestone }) {
		await milestone('Sleeping');
		await new Promise((resolve) => setTimeout(resolve, settings.ms ?? 5_000));
		return { sleptMs: settings.ms ?? 5_000 };
	},

	/** Always fails, to show the dead-letter state. */
	async alwaysFails() {
		throw new Error('This job always fails');
	},

	/** Maintenance: delete finished jobs and their events older than `settings.olderThanHours`. */
	async cleanupFinishedJobs(job, settings, { milestone }) {
		const cutoff = Date.now() - (settings.olderThanHours ?? 24) * 3_600_000;
		let deleted = 0;
		for (const status of ['completed', 'dead']) {
			const old = Job.search({
				conditions: [
					{ attribute: 'status', value: status },
					{ attribute: 'completedAt', comparator: 'less_than', value: cutoff },
				],
				select: ['id'],
			});
			for await (const { id } of old) {
				for await (const event of JobEvent.search({ conditions: [{ attribute: 'jobId', value: id }], select: ['id'] })) {
					await JobEvent.delete(event.id);
				}
				await Job.delete(id);
				deleted++;
			}
		}
		await milestone(`Deleted ${deleted} finished jobs`);
		return { deleted };
	},
};
