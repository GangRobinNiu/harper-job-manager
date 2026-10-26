# Harper Job Manager (MVP)

A small multi-tenant job manager built as a Harper component. One queue model handles every way a job can start:

- **Cron jobs**: a definition with a cron schedule
- **API calls and webhooks**: `POST /trigger/{id}` and `POST /webhook/{id}`, with idempotency keys
- **Predefined jobs**: handlers that run inside Harper
- **Custom integration jobs**: run by external workers that claim them over HTTP, for example to start a container image

Every start is an event. An event router (the equivalent of Azure Event Grid) decides which jobs each event starts. Every job, whatever started it, then gets the same priorities, leases, retries with backoff, a dead-letter state, and a milestone log.

## How it works

```
 cron tick ──┐                                                    ┌─> in-process worker (queue "local")
 /trigger   ─┤                                                    │
 /webhook   ─┼─> router ──> Job table, one queue per job type ────┤
 /events    ─┘  (Event      (dedupe by idempotency key)            └─> external workers: /claim ─> run image
 (blob created)  Grid)                    │                                             ─> /heartbeat ─> /complete
                   │                      └─> JobEvent table (milestones), live over SSE / WebSocket / MQTT
                   └─> Event table (every event and where it went; "unrouted" = router dead letter)
```

| Old Service Platform (Azure) | Here (Harper) |
| --- | --- |
| Event Grid: routes events by type and subject | `src/router.js` with `EventSubscription` rules |
| Service Bus queues, one per job type | `Job` table with a `queue` field |
| Dispatcher plus KEDA, starting a Kubernetes Job per message | External workers calling `/claim` (a real one would start the image) |
| Kubernetes CronJob publishing an event | Harper's built-in scheduler running the tick, which publishes `schedule.fired` |
| Event Hub plus milestone logger plus Cosmos DB | `JobEvent` and `Job` tables, streamed live by Harper |

- **Everything is an event.** The cron tick, API calls, webhooks, and storage notifications all publish events to the router. Retries, dead-lettering, and monitoring are built once and apply to every job.
- **Routing like Event Grid.** Sources that know their job (cron, API, webhooks) name it in the event. Sources that don't, like a "blob created" notification, are matched by `EventSubscription` rules on event type and subject. A rule can read the tenant and job type from the file path, e.g. `^/blobs/(?<tenant>[^/]+)/(?<jobType>[^/]+)/` with the template `{tenant}-{jobType}`. One event can match several rules (fan-out), and an event that matches nothing is kept as `unrouted`.
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

Route storage events by path (the Event Grid part):

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
- **Synchronous routing.** The router enqueues jobs during the publish call, so a publisher gets a result right away; Event Grid instead accepts the event and delivers it with retries. If a job definition is missing, the event is still recorded with the error.
- **Cleanup is a job.** Old jobs and events are removed by the `cleanupFinishedJobs` handler; schedule it with a cron definition.

## Files

```
config.yaml            REST, schema, resources, and the scheduler tick
schema.graphql         JobDefinition, Job, JobEvent, Event, EventSubscription
resources.js           /events, /trigger, /webhook, /claim, /heartbeat, /complete; starts the local worker
src/router.js          the event router: direct routes, subscription rules, fan-out, unrouted events
src/queue.js           enqueue, claim, heartbeat, complete, retries, dead-letter, lease reaping
src/tick.js            publishes due cron runs as events and reaps expired leases, once per cluster
src/localWorker.js     in-process worker for predefined jobs
src/handlers.js        predefined handlers (echo, flaky, alwaysFails, cleanupFinishedJobs)
worker/external-worker.mjs   sample external worker
test/                  end-to-end and concurrency tests
```
