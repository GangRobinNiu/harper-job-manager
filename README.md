# Harper Job Manager (MVP)

A small multi-tenant job manager built as a Harper component. One queue model handles every way a job can start:

- **Cron jobs**: a definition with a cron schedule
- **API calls and webhooks**: `POST /trigger/{id}` and `POST /webhook/{id}`, with idempotency keys
- **Harper jobs**: handlers that run inside Harper, on the queue node or on dedicated Harper job nodes
- **Custom integration jobs**: run by external workers that claim them over HTTP, for example to start a container image

Every start is an event. An event router decides which jobs each event starts. Every job, whatever started it, then gets the same priorities, leases, retries with backoff, a dead-letter state, and a milestone log.

## How it works

![Architecture: event sources publish to the router on the Harper queue node, which enqueues jobs into queues in the Job table; the queue node's Harper worker runs the internal local queue, and Harper job nodes claim the other queues over HTTP](docs/architecture.svg)

- **Everything is an event.** The cron tick, API calls, webhooks, and storage notifications all publish events to the router. Retries, dead-lettering, and monitoring are built once and apply to every job.
- **Routing by rules.** Sources that know their job (cron, API, webhooks) name it in the event. Sources that don't, like a "blob created" notification, are matched by `EventSubscription` rules on event type and subject. A rule can read the tenant and job type from the file path, e.g. `^/blobs/(?<tenant>[^/]+)/(?<jobType>[^/]+)/` with the template `{tenant}-{jobType}`. One event can match several rules (fan-out), and an event that matches nothing is kept as `unrouted`.
- **Events say who and what; definitions say how.** The image comes from the job definition, so an upgrade is one settings change. An event can pin `imageVersion` to test a new image on one tenant or roll back.
- **Atomic claims.** `tryClaim` takes Harper's record lock (`Table.lock`) before checking and updating a job, so competing workers on any thread never claim the same job.
- **Leases, not acknowledgments.** A claim comes with a lease. Workers extend it with `/heartbeat`. If a worker dies, the scheduler tick reaps the expired lease and retries the job.
- **Fencing.** Only the worker holding the current lease can complete a job. A worker that stalled past its lease gets `409` and its late result is discarded.
- **Retries with a limit.** Failures retry with exponential backoff (5s, 10s, 20s, up to 5 minutes). After `maxAttempts`, the job moves to `dead`: `GET /Job/?status=dead` is the dead-letter queue.
- **Idempotency.** With an `Idempotency-Key` header, the job id is derived from the key, so a retried webhook lands on the same job. Cron runs use the scheduled time as the key, so a double fire during leader failover cannot run twice.
- **Cluster-once scheduling.** The tick runs on Harper's built-in `scheduler` plugin, which elects one leader per cluster.
- **Dedicated and shared queues.** A definition names its queue: a dedicated one for a high-volume solution (`data-loader`) or a shared `generic` one for custom integrations. Workers say which queues they serve.

## Run it

Requires Node 22+ and Harper 5.3+ (`npm i -g harper`).

```bash
npm install
npm run dev                 # Harper on http://localhost:9926
```

Define jobs (the tables are exposed over REST):

```bash
# A predefined job that runs inside Harper
curl -X PUT localhost:9926/JobDefinition/acme-echo -H 'content-type: application/json' \
  -d '{"id":"acme-echo","enabled":true,"tenant":"acme","queue":"local","handler":"echo","settings":{"region":"us-west"}}'

# A custom integration on a dedicated queue, run by an external worker
curl -X PUT localhost:9926/JobDefinition/acme-loader -H 'content-type: application/json' \
  -d '{"id":"acme-loader","enabled":true,"tenant":"acme","queue":"data-loader","handler":"jfrog/acme/data-loader:1.4","settings":{"rows":5000},"maxAttempts":3,"leaseMs":60000}'

# A nightly cron job
curl -X PUT localhost:9926/JobDefinition/nightly-cleanup -H 'content-type: application/json' \
  -d '{"id":"nightly-cleanup","enabled":true,"queue":"local","handler":"cleanupFinishedJobs","cron":"0 2 * * *","timezone":"America/Los_Angeles","settings":{"olderThanHours":72}}'
```

Route storage events by path:

```bash
# Rule: any new file under {tenant}/{jobType}/ starts the definition "{tenant}-{jobType}"
curl -X PUT localhost:9926/EventSubscription/blob-jobs -H 'content-type: application/json' \
  -d '{"id":"blob-jobs","enabled":true,"eventTypes":["storage.blob.created"],"subjectPattern":"^/containers/input/blobs/(?<tenant>[^/]+)/(?<jobType>[^/]+)/","definitionTemplate":"{tenant}-{jobType}"}'

# A definition that rule can reach
curl -X PUT localhost:9926/JobDefinition/acme-data-loader -H 'content-type: application/json' \
  -d '{"id":"acme-data-loader","enabled":true,"tenant":"acme","queue":"data-loader","handler":"jfrog/acme/data-loader:1.4"}'

# The storage notification (CloudEvents shape)
curl -X POST localhost:9926/events -H 'content-type: application/json' \
  -d '{"id":"blob-001","type":"storage.blob.created","subject":"/containers/input/blobs/acme/data-loader/positions.csv","data":{"size":1024}}'

curl "localhost:9926/Event/?status=unrouted"     # events that matched nothing
```

Trigger and watch:

```bash
curl -X POST localhost:9926/trigger/acme-echo -H 'content-type: application/json' -d '{"hello":"world"}'
curl -X POST localhost:9926/webhook/acme-loader -H 'content-type: application/json' \
  -H 'idempotency-key: positions-2026-10-26' -d '{"file":"positions.csv"}'

QUEUES=data-loader npm run worker          # sample external worker

curl "localhost:9926/Job/?status=dead"     # dead-letter jobs
curl "localhost:9926/JobEvent/?jobId=acme-loader:positions-2026-10-26"
curl -N -H "Accept: text/event-stream" "localhost:9926/Job/acme-loader:positions-2026-10-26"   # live status
```

## Harper jobs

A Harper job is a handler in `src/handlers.js`, run inside Harper by the worker in `src/localWorker.js`. Put generic jobs, repeatable solution jobs, and tenant-specific jobs there as named handlers, and point a definition at one with `handler`.

The worker reads its settings from environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `JOB_QUEUES` | `local` | Queues to serve, comma-separated |
| `JOB_CONCURRENCY` | `4` | Jobs run at once |
| `JOB_MANAGER_URL` | unset | The queue node to claim from. Unset means this node is the queue node |
| `JOB_MANAGER_USER`, `JOB_MANAGER_PASS` | unset | Credentials for the queue node |

On the queue node, the worker claims straight from the `Job` table. Light jobs can stay here:

```bash
JOB_QUEUES=local,harper npm run dev
```

Heavy jobs belong on **Harper job nodes**: separate Harper instances that run the same component but only work jobs, so a long data load never slows the API or the queue. A job node claims from the queue node over HTTP, the same way an external worker does, because `Table.lock` only protects claims within one node:

```bash
JOB_MANAGER_URL=http://queue-node:9926 JOB_QUEUES=harper npm run dev
```

The `local` queue is for internal use: job system maintenance such as `cleanupFinishedJobs`, which works on the queue node's own tables, and quick tests like `echo`. It only runs on the queue node: `/claim` refuses it, so no job node or external worker can take it. Tenant and solution jobs go on other queues. Harper jobs run in Node.js, without per-job CPU or memory limits, so a runaway job can affect other jobs on the same job node, but not the queue node.

## API

| Endpoint | Body | Result |
| --- | --- | --- |
| `POST /events` | `{ id, type, source?, subject?, data?, definitionId? }` (CloudEvents shape) | `202` with where it was routed, or `200` with `status: "unrouted"` |
| `POST /trigger/{definitionId}` | Job payload; optional `Idempotency-Key` header | `202` new job, `200` duplicate, `404` unknown definition |
| `POST /webhook/{definitionId}` | Same, recorded as trigger `webhook` | Same |
| `POST /claim` | `{ workerId, queues: [...], limit, leaseMs? }` | Claimed jobs with `handler` and tenant `settings` |
| `POST /heartbeat` | `{ workerId, jobId, milestone?, data?, extendMs? }` | `200`, or `409` if the worker lost the job |
| `POST /complete` | `{ workerId, jobId, ok, result?, error? }` | `200` with the new status, or `409` if the lease was lost |

Tables: `JobDefinition`, `Job`, `JobEvent`, `Event`, `EventSubscription` (see `schema.graphql`).

## Tests

```bash
node test/e2e.mjs           # 14 scenarios: triggers, routing, idempotency, retries, dead-letter, cron, leases, fencing
node test/concurrency.mjs   # 300 jobs, 12 competing workers: each job claimed exactly once
```

Results on Harper 5.3.4 with 4 worker threads:

- All 14 end-to-end scenarios pass.
- Concurrency, with `CLAIM_DELAY_MS=20` to widen the race window: with `Table.lock`, 300 of 300 jobs were claimed exactly once. With the lock replaced by a plain `get`, the same run claimed some jobs up to 12 times.

## Limits of this MVP

- **One node.** In core Harper, `Table.lock` is exclusive across every thread of one node, not across a cluster. For several nodes, pin each queue's claims to one node (the static prerender template pins workers to nodes for the same reason) or use cluster-wide locks where available.
- **Scheduling granularity.** The tick runs every 5 seconds, so cron runs and lease expiry are accurate to about 5 seconds.
- **No per-worker authorization.** Any authenticated worker can claim from any queue it names. Production would give each worker a role limited to its queues.
- **Simulated external work.** The sample worker simulates running `job.handler`. A real one would start that container image, for example as a Kubernetes Job.
- **Synchronous routing.** The router enqueues jobs during the publish call, so a publisher gets a result right away, but a publish is not retried if enqueueing fails partway. If a job definition is missing, the event is still recorded with the error.
- **Cleanup is a job.** Old jobs and events are removed by the `cleanupFinishedJobs` handler; schedule it with a cron definition.

## Backlog

- **Kubernetes jobs.** Run container jobs as Kubernetes Jobs, for work that is not JavaScript or that needs per-job CPU and memory limits. Planned shape:
  - Add `runtime: "harper" | "kubernetes"` to `JobDefinition` (default `harper`). For `kubernetes`, `handler` is an image reference.
  - A dispatcher claims `kubernetes` jobs, creates a Kubernetes Job from the image with the tenant settings, heartbeats while it runs, and completes the job when the pod finishes. It replaces the sample external worker.
  - Routing, queues, leases, retries, dead-lettering, and milestones stay shared, so moving a job between Harper and Kubernetes is a definition change.
- **MQTT wake-up for job nodes.** Today each job node polls `/claim` every second, so an idle cluster still sends a steady stream of empty claims, and a new job waits up to a second. Build this when there are many job nodes or queues, or a job type where pickup speed matters. Planned shape:
  - Add a `QueueSignal` table with one record per queue. Enqueue and retry touch the queue's record, and Harper publishes the change over MQTT. Workers do not subscribe to `Job` itself, because every heartbeat updates it.
  - Job nodes subscribe to `QueueSignal/{queue}` for each queue they serve. On a message, they claim until a claim comes back empty or they are full.
  - Claiming stays on `/claim` with `Table.lock`. MQTT sends every message to every subscriber, so several nodes may wake at once, and the lock still gives each job to one node.
  - Keep a slow fallback poll (about 30 seconds) for messages missed during a disconnect and for retries whose `runAfter` comes due with no new signal. The scheduler tick could also signal queues whose delayed jobs just came due.
  - Prior art: the static prerender template publishes `queue_status/producer` (`queued` or `empty`) and its workers claim only when it says `queued`.
- **Web UI.** A page for operators to manage job definitions, watch jobs and milestones live, and inspect or rerun dead-letter jobs. Most of it needs no new backend: it reads the REST-exported tables and subscribes to `Job` and `JobEvent` changes over SSE or WebSocket. Rerunning a dead job needs one new endpoint that resets it to `pending`.

## Files

```
config.yaml            REST, schema, resources, and the scheduler tick
schema.graphql         JobDefinition, Job, JobEvent, Event, EventSubscription
resources.js           /events, /trigger, /webhook, /claim, /heartbeat, /complete; starts the local worker
src/router.js          the event router: direct routes, subscription rules, fan-out, unrouted events
src/queue.js           enqueue, claim, heartbeat, complete, retries, dead-letter, lease reaping
src/tick.js            publishes due cron runs as events and reaps expired leases, once per cluster
src/localWorker.js     Harper job worker: on the queue node, or on a job node claiming over HTTP
src/handlers.js        predefined handlers (echo, flaky, alwaysFails, cleanupFinishedJobs)
worker/external-worker.mjs   sample external worker
docs/architecture.svg  the architecture chart above
test/                  end-to-end and concurrency tests
```
