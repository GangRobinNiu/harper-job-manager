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
- **Idempotency.** `/trigger` and `/webhook` require an `Idempotency-Key` header. The job id is derived from the key, so a caller can safely retry, for example after a timeout or an outage, and the retry lands on the same job. Cron runs use the scheduled time as the key, so a double fire during leader failover cannot run twice.
- **Cluster-once scheduling.** The tick runs on Harper's built-in `scheduler` plugin, which elects one leader per cluster.
- **Dedicated and shared queues.** A definition names its queue: a dedicated one for a high-volume solution (`data-loader`) or a shared `generic` one for custom integrations. Workers say which queues they serve.
- **Outages pause work, they don't lose it.** If the queue node is unreachable when a job finishes, the Harper worker keeps the result and retries `/complete` with backoff (up to 30 minutes) until it is delivered, so the job is not run again. After a restart, the queue node waits one lease length before reaping expired leases, so workers can reconnect and heartbeat first.

## Run it

Requires Node 22+ and Harper 5.3+ (`npm i -g harper`).

```bash
npm install
npm run dev                 # Harper on http://localhost:9926
```

For a guided tour with copy-paste commands and the expected results, see [docs/walkthrough.md](docs/walkthrough.md).

**Admin UI:** open http://localhost:9926/admin_ui/ to watch jobs live: counts by status, the newest jobs with filters by status, tenant, and queue, each job's timeline and result, unrouted events, and job definitions. It is an internal, read-only page that polls the REST tables every 3 seconds.

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
curl -X POST localhost:9926/trigger/acme-echo -H 'content-type: application/json' \
  -H 'idempotency-key: hello-1' -d '{"hello":"world"}'
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
| `REAP_GRACE_MS` | `60000` | On the queue node: how long after startup to wait before reaping expired leases |

On the queue node, the worker claims straight from the `Job` table. Light jobs can stay here:

```bash
JOB_QUEUES=local,harper npm run dev
```

Heavy jobs belong on **Harper job nodes**: separate Harper instances that run the same component but only work jobs, so a long data load never slows the API or the queue. A job node claims from the queue node over HTTP, the same way an external worker does, because `Table.lock` only protects claims within one node:

```bash
JOB_MANAGER_URL=http://queue-node:9926 JOB_QUEUES=harper npm run dev
```

Keep job nodes out of the queue node's Harper cluster; they only need HTTP access to it. Inside one cluster, the scheduler could elect a job node as its leader, and cron runs and lease reaping would stop. A job node that finds itself the leader logs an error saying so.

The `local` queue is for internal use: job system maintenance such as `cleanupFinishedJobs`, which works on the queue node's own tables, and quick tests like `echo`. It only runs on the queue node: `/claim` refuses it, so no job node or external worker can take it. Tenant and solution jobs go on other queues. Harper jobs run in Node.js, without per-job CPU or memory limits, so a runaway job can affect other jobs on the same job node, but not the queue node.

## API

| Endpoint | Body | Result |
| --- | --- | --- |
| `POST /events` | `{ id, type, source?, subject?, data?, definitionId? }` (CloudEvents shape) | `202` with where it was routed, or `200` with `status: "unrouted"` |
| `POST /trigger/{definitionId}` | Job payload; required `Idempotency-Key` header | `202` new job, `200` duplicate, `400` missing key, `404` unknown definition |
| `POST /webhook/{definitionId}` | Same, recorded as trigger `webhook` | Same |
| `POST /claim` | `{ workerId, queues: [...], limit, leaseMs? }` | Claimed jobs with `handler` and tenant `settings` |
| `POST /heartbeat` | `{ workerId, jobId, milestone?, data?, extendMs? }` | `200`, or `409` if the worker lost the job |
| `POST /complete` | `{ workerId, jobId, ok, result?, error? }` | `200` with the new status, or `409` if the lease was lost |
| `GET /session` | | `{ username, role, superUser }`; `username` is `null` when nobody is signed in |
| `POST /session` | `{ username, password }` | Signs in and sets the session cookie; `403` for a wrong password |
| `DELETE /session` | | Signs out |

Every endpoint above except `/session` requires a **super user**: `401` when not signed in, `403` for any other role. Workers and job nodes must therefore use super user credentials (`HARPER_USER`, `JOB_MANAGER_USER`). On localhost, `authorizeLocal` signs every request in as a super user.

## Admin UI sign-in

On localhost the admin UI opens straight away, because `authorizeLocal` signs every local request in as a super user. From any other address it shows a sign-in form that uses Harper's session cookies (`/session`).

**A read-only user.** The component declares a `job_viewer` role in `roles.yaml`. It can read the job tables but not `JobDefinition.settings`, which can hold secrets, and the job endpoints refuse it. Create a user with it through the operations API, from the Harper machine:

```bash
curl -X POST localhost:9925 -H 'content-type: application/json' \
  -d '{"operation":"add_user","role":"job_viewer","username":"viewer","password":"CHOOSE-A-PASSWORD","active":true}'
```

Harper creates roles from `roles.yaml` when it starts, so restart Harper after adding or changing a role there.

**HTTPS.** Without it, passwords and the session cookie travel unencrypted. Harper's settings are in `harper-config.yaml` in the Harper root (not this repo):

1. Under `http:`, set `securePort: 9927` (any free port). Restart Harper. It serves the same app on `https://<host>:9927` with a self-signed certificate, so browsers warn.
2. For a certificate the browser trusts on a development machine, use [mkcert](https://github.com/FiloSottile/mkcert):

   ```bash
   winget install FiloSottile.mkcert      # then open a new terminal
   mkcert -install                         # trusts mkcert's local CA (a Java keytool error here only affects Java)
   cd <harper root>/keys
   mkcert -cert-file harper-cert.pem -key-file harper-key.pem <your-ip> localhost 127.0.0.1
   ```

   Then point `tls:` at the files (`certificate: <harper root>/keys/harper-cert.pem`, `privateKey: <harper root>/keys/harper-key.pem`) and restart Harper. The certificate is trusted only on machines that trust mkcert's CA, and must be reissued if the IP address changes. If one browser profile still says "Not secure", clear that profile's saved data for the site; it may remember an earlier "proceed anyway".
3. In production, use a certificate from a real certificate authority.

Over HTTPS, Harper marks the session cookie `Secure`. It names the cookie after the request's origin, so another website cannot send requests with a user's session.

**Ports.** `9926` is the app over HTTP and `9927` the same app over HTTPS: REST tables, job endpoints, `/session`, and `/admin_ui/`. `9925` is Harper's operations API (users, roles, Studio): the most powerful port, and HTTP only here.

**Before running anywhere but a development machine:**

- Set `authentication.authorizeLocal: false`, so local requests also need credentials.
- Serve HTTPS and, once nothing uses it, turn plain HTTP off (`http.port: null`).
- Keep the operations API (`9925`) off the network, or give it HTTPS (`operationsApi.network.securePort`).
- Give admin UI users the `job_viewer` role, not super user credentials.

Tables: `JobDefinition`, `Job`, `JobEvent`, `Event`, `EventSubscription` (see `schema.graphql`).

## Tests

```bash
node test/e2e.mjs           # 15 scenarios: triggers, routing, idempotency, retries, dead-letter, cron, leases, fencing
node test/concurrency.mjs   # 300 jobs, 12 competing workers: each job claimed exactly once
```

Results on Harper 5.3.4 with 4 worker threads:

- All end-to-end scenarios pass (15 as of the last run, on Harper 5.3.2 with 1 worker thread).
- Concurrency, with `CLAIM_DELAY_MS=20` to widen the race window: with `Table.lock`, 300 of 300 jobs were claimed exactly once. With the lock replaced by a plain `get`, the same run claimed some jobs up to 12 times.

## Deployment

The job manager is a Harper instance running this component: the **queue node**. The same component also runs on Harper job nodes in a second mode, and external workers are plain HTTP clients that need no Harper at all.

| Node | Runs | Configured by |
| --- | --- | --- |
| **Queue node** (one) | The queue tables, the API (`/trigger`, `/webhook`, `/events`, `/claim`, `/heartbeat`, `/complete`), the scheduler tick (cron and lease reaping), the admin UI, and the `local` queue | This component, `JOB_MANAGER_URL` unset |
| **Harper job nodes** (any number) | Harper job handlers from `src/handlers.js`, claimed from the queue node over HTTP | This component, `JOB_MANAGER_URL` set to the queue node, `JOB_QUEUES` set to their queues |
| **External workers** (any number) | Your own code or containers, using `/claim`, `/heartbeat`, and `/complete` | Any HTTP client; the sample is `worker/external-worker.mjs` |

Keep heavy work off the queue node. It should only keep the books, so its load stays light and predictable (see "Scalability and availability"). The `local` queue is for maintenance and quick tests.

### Setting up

1. **Queue node.** Install Harper, check out this repo, run `npm install`, and start it with `harper run .` (or deploy it with Harper's `deploy_component` operation).
2. **One stable address.** Callers, job nodes, and workers reach the queue node through a DNS name or load balancer, never the machine's own address, so the standby queue node in the backlog can take over later without reconfiguring anyone.
3. **Lock it down.** Apply the checklist under "Admin UI sign-in": `authorizeLocal: false`, HTTPS with a certificate from a real certificate authority, plain HTTP off, and the operations API (`9925`) kept off the network.
4. **Credentials.** Create a Harper user for each worker and job node. Today these must be super users (see "Limits"), so treat every worker credential as an admin credential and keep each one separate, so one can be revoked alone.
5. **Cleanup.** Add a cron definition on the `local` queue that runs `cleanupFinishedJobs`, or `Job`, `Event`, and `JobEvent` grow without limit.
6. **Harper job nodes.** Separate Harper instances with this component, **outside** the queue node's Harper cluster (see "Harper jobs"):

   ```bash
   JOB_MANAGER_URL=https://jobs.example.com:9927 JOB_MANAGER_USER=... JOB_MANAGER_PASS=... \
   JOB_QUEUES=data-loader JOB_CONCURRENCY=8 harper run .
   ```

   They hold no job state, so an autoscaler can add and remove them on each queue's pending count.
7. **External workers.** Point them at the same address:

   ```bash
   HARPER_URL=https://jobs.example.com:9927 HARPER_USER=... HARPER_PASS=... QUEUES=generic node worker/external-worker.mjs
   ```

### Network

| From | To | Port |
| --- | --- | --- |
| Callers, webhooks, storage notifications | Queue node: `/trigger`, `/webhook`, `/events` | `9927` (HTTPS) |
| Harper job nodes, external workers | Queue node: `/claim`, `/heartbeat`, `/complete` | `9927` (HTTPS) |
| Operators | Queue node: `/admin_ui/` | `9927` (HTTPS) |
| Administrators only, from inside the network | Queue node: operations API (users, roles, Studio) | `9925` |
| Queue node | Standby queue node, once built: Harper replication | Harper's replication port |

Workers only ever connect out to the queue node. Nothing connects in to them, so they can run behind NAT or in another network.

## Scalability and availability

### Where the work happens

Jobs run on Harper job nodes, not on the queue node. The queue node only keeps the books: about a dozen small indexed reads and writes per job (enqueue, claim, complete, milestones), plus one write per heartbeat while a job runs. For scale, 1 million jobs a day averages about 12 jobs a second. Adding customers mostly means adding job nodes, which scale horizontally: give heavy tenants or solutions their own queue and their own job nodes.

Job nodes hold no job state, so they suit an autoscaler. Scale each queue's job nodes on its pending count (`GET /Job/?queue=data-loader&status=pending`).

### Where queue node load grows

The queue node's load does not track the job count alone. Watch these:

- **Bursts.** Many tenants with the same cron schedule are all published in the same 5-second tick. Spread schedules out or add a random offset.
- **Heartbeats** grow with the number of jobs running at once, not the number started: 10,000 running jobs heartbeating every 20 seconds is 500 writes a second. Long jobs can heartbeat less often.
- **Idle polling.** Every job node polls `/claim` once a second whether or not there is work. The MQTT wake-up in the backlog removes this.
- **Table size.** `Job`, `Event`, and `JobEvent` grow until `cleanupFinishedJobs` removes old rows. Schedule it.

Queue node throughput has not been load-tested yet. Measure it before relying on a ceiling.

### Scaling steps, in order

1. **One bigger queue node.** Add worker threads (`Table.lock` is safe across all threads of a node) and a larger machine. Claim in batches, and reduce idle polling.
2. **Split queues across queue nodes.** Each queue has one owner node, so claims for a queue always happen in one place, and different queues live on different nodes.
3. **Cells.** Run independent copies of the whole system and assign each tenant group to one. Cells scale without any cross-node locking and keep tenants isolated from each other's failures. See the backlog.

### Availability

The queue node is a single point of failure: `Table.lock` is exclusive within one node, so only one node can hand out jobs. While it is down:

| | What happens |
| --- | --- |
| Running jobs | Keep running. Harper workers hold their results and retry `/complete` until it is delivered |
| Heartbeats | Fail and are retried on the next interval. After a restart, reaping waits one lease length (`REAP_GRACE_MS`) so workers can heartbeat first |
| New triggers and webhooks | Rejected. Callers retry with the same `Idempotency-Key`, so a retry never creates a second job |
| Cron runs | Delayed until the node returns, then fire once. Missed occurrences are skipped |
| Claims | None, so all queues pause |

The chosen direction is a **primary queue node with a standby** (see the backlog): replicate to a standby and fail over behind one address, with exactly one node active at a time. Two active nodes would hand the same job to two workers. Because replication is asynchronous, a failover can run a job twice, so handlers must be safe to run twice. Cells limit how many tenants one failure affects, but they do not replace the standby: each cell still needs one.

## Limits of this MVP

- **One queue node.** In core Harper, `Table.lock` is exclusive across every thread of one node, not across a cluster, so all claims go through one queue node. That node is a single point of failure: while it is down, running jobs keep going and hold their results, but nothing new is accepted or claimed. See the standby queue node in the backlog.
- **Scheduling granularity.** The tick runs every 5 seconds, so cron runs and lease expiry are accurate to about 5 seconds.
- **Workers need super user credentials.** The job endpoints accept only super users, so every worker and job node can claim from any queue and complete any job. Production would give each worker a role limited to its queues.
- **The admin UI is internal.** Its files are public, like all static files; its data needs a signed-in user (see "Admin UI sign-in"). Any signed-in user, `job_viewer` included, sees every tenant's jobs and payloads.
- **Simulated external work.** The sample worker simulates running `job.handler`. A real one would start that container image, for example as a Kubernetes Job.
- **Synchronous routing.** The router enqueues jobs during the publish call, so a publisher gets a result right away, but a publish is not retried if enqueueing fails partway. If a job definition is missing, the event is still recorded with the error.
- **Cleanup is a job.** Old jobs and events are removed by the `cleanupFinishedJobs` handler; schedule it with a cron definition.

## Backlog

- **Standby queue node.** Removes the single point of failure. Planned shape:
  - Replicate the `jobs` database from the active queue node to a standby. Job nodes and callers reach the queue through one load balancer or DNS name, never a node address.
  - Exactly one node is active at a time. Health checks move traffic to the standby when the active node fails. Two active nodes would hand the same job to two workers, because `Table.lock` does not span nodes, so failover must guarantee a single active node. The scheduler tick must also run only on the active node.
  - Replication is asynchronous (confirm for the Harper version in use), so the standby may be a few seconds behind. A claim that did not replicate makes its job look `pending` again, so it can run twice: the original worker's late result gets `409`. Handlers must be safe to run twice. An enqueue that did not replicate is recovered when the caller retries with the same `Idempotency-Key`. Cron runs are deduplicated by their scheduled-time key.
  - The outage handling already in place (result retries, the restart grace before reaping) is what makes a failover safe for running jobs.
- **Cells.** Scale out by running several independent copies of the system, each with its own queue node, standby, and job nodes, and assigning each tenant group to a cell. A management layer on top places tenants and routes their events to the right cell. One cell's failure or overload never affects another.
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
- **Put the `Job` table in its own database.** Harper serializes writes within a database and commits each database separately. Today `Job`, `JobEvent`, `Event`, `JobDefinition`, and `EventSubscription` all live in `jobs`, so milestone and event writes wait behind claims, heartbeats, and completions on the hot `Job` table. Cheap to do; worth it once write load on the queue node grows. Planned shape:
  - Declare `Job` with `@table(database: "job_queue")` in `schema.graphql` and read it from `databases.job_queue` in `src/queue.js` and `resources.js`. The REST path `/Job/` stays the same.
  - No change in consistency: `JobEvent` rows are already written after the job's transaction commits, as a log that can trail the status change.
  - The standby queue node must replicate both databases.
  - Prior art: `prerender-plugin` keeps its render queue in a database of its own for the same reason.
- **Archive finished jobs, and a real dead-letter table.** Needed at around 1 million jobs a day. Today finished jobs stay in `Job` until `cleanupFinishedJobs` deletes them, so the hot table carries a day of history, and the cleanup is one large burst of row-by-row deletes. Instead, `Job` holds only live work. Planned shape:
  - **Only finished jobs move.** A completed job moves to `JobArchive`. A job that runs out of attempts moves to `DeadJob`, the dead-letter queue. A failure with retries left goes back to `pending` and stays in `Job`.
  - **Move inline, in `/complete` and in lease reaping**, not in a separate listener. Write the archive row first, then delete the job from `Job`. Both use the job id as the key, so a retried move overwrites the same row and loses nothing. A sweeper in the scheduler tick moves any finished job left in `Job`, for example after a crash between the two steps.
  - **Idempotency must look beyond `Job`.** Deduplication works today because the finished job is still in `Job`. Once jobs move out immediately, a retry with the same `Idempotency-Key` would create a second job. `enqueue` checks `Job`, `JobArchive`, and `DeadJob`, or a small `IdempotencyKey` table (key to job id). State the window in the docs, for example "retries within 7 days return the original job".
  - **Fencing is unchanged.** A late `/complete` from a worker that lost its lease finds no job and gets `409`.
  - **Dead-letter work.** Operators inspect `DeadJob`, rerun a job (move it back to `Job` as `pending`), or delete it. It keeps dead jobs longer than the archive keeps completed ones, and alerting can watch it for new rows.
  - **Databases, split by traffic** (extends the previous item):

    | Database | Tables | Traffic |
    | --- | --- | --- |
    | `job_queue` | `Job`, live only | every claim, heartbeat, and completion |
    | `jobs` | `JobDefinition`, `EventSubscription`, `JobStats` | mostly reads |
    | `job_history` | `JobArchive`, `DeadJob`, `JobEvent`, `Event` | appends, with their own retention |

    `JobEvent` is an append-only log, so it lives with history from the start and never moves.
  - **Retention.** `cleanupFinishedJobs` prunes only `job_history`: `JobArchive`, `JobEvent`, and `Event` after N days (about 30 million jobs a month at 1 million a day), `DeadJob` later. It never touches the hot table.
  - **Lookups.** Add `GET /jobs/{id}`, which checks `Job`, then `JobArchive`, then `DeadJob`, so callers and the future Customer UI have one place to ask what happened to a job.
  - **Admin UI counts.** Pending and running are counted from `Job`, and dead from `DeadJob`. These are small sets, so counting them directly stays exact and cheap. Completed becomes a rate, "Completed, last 24 h", read from a `JobStats` table with one row per hour per queue (`completed`, `dead`, `enqueued`). Increment it in the step that deletes the job from `Job`, which happens once per job, so retries and the sweeper never double-count. To keep one hot row from serializing every completion, use an atomic increment if Harper offers one (check `addTo` in the Harper version in use), or count in memory per thread and flush every few seconds. A `GET /stats` endpoint computes all tiles once, caches them for a few seconds, and serves every open tab, so the admin UI's cost does not grow with the number of viewers. Hourly rows also give a throughput chart.
  - **Changes elsewhere.** The dead-letter queue moves from `GET /Job/?status=dead` to `GET /DeadJob/`. The admin UI's Completed and Dead tabs read the new tables, and the end-to-end tests that check `completed` or `dead` status follow them.
- **Admin UI, next steps.** The internal, read-only admin UI (`/admin_ui/`) is done. Still to build:
  - **Operator actions:** rerun a dead job (needs one new endpoint that resets it to `pending`, or moves it back from `DeadJob` once that exists), trigger a job, and enable or disable a definition.
  - **Customer UI.** A page where each tenant sees only its own jobs. Not safe on today's API: the REST tables are exported to any signed-in caller with no tenant filtering, so one tenant could read another's jobs, payloads, and definition `settings`. It needs a status endpoint scoped to the caller's tenant, returning only safe fields (status, times, milestone messages), with customer authentication.
  - **Live push instead of polling.** A table-wide SSE subscription replays every row on connect, so the page polls instead. Push would need a narrower feed, such as the `QueueSignal` table from the MQTT item.
  - **Cheaper counts.** The admin UI runs on the queue node, and every open tab polls every 3 seconds. Each round counts jobs by reading every job id for each status (up to 10,000 each), which is most of the load it adds, and the completed count is capped and wrong at high volume. The planned fix, `JobStats` counters behind a cached `GET /stats`, is part of "Archive finished jobs" above.
  - **Turn the admin UI off on job nodes.** Job nodes run the same component, so they serve `/admin_ui/` too, but it reads the job node's own tables, which are empty or unrelated. Someone opening it there sees an idle system. Disable it when `JOB_MANAGER_URL` is set, the same way the worker detects job-node mode, or redirect to the queue node. Until then, open the admin UI only on the queue node.
  - **Serve reads from the standby.** With the standby queue node, the admin UI can read the replicated tables there, taking its polling off the active node and staying up during a failover, at the cost of data a few seconds behind.

## Files

```
config.yaml            REST, schema, resources, and the scheduler tick
schema.graphql         JobDefinition, Job, JobEvent, Event, EventSubscription
resources.js           /events, /trigger, /webhook, /claim, /heartbeat, /complete; starts the local worker
src/router.js          the event router: direct routes, subscription rules, fan-out, unrouted events
src/queue.js           enqueue, claim, heartbeat, complete, retries, dead-letter, lease reaping
src/tick.js            publishes due cron runs as events and reaps expired leases, on the queue node only
src/localWorker.js     Harper job worker: on the queue node, or on a job node claiming over HTTP
src/handlers.js        predefined handlers (echo, flaky, sleep, alwaysFails, cleanupFinishedJobs)
worker/external-worker.mjs   sample external worker
docs/architecture.svg  the architecture chart above
docs/walkthrough.md    five copy-paste demos: first job, a running job, retries, routing, cron
admin_ui/              internal admin UI, served at /admin_ui/ by Harper's static plugin
roles.yaml             job_viewer, a read-only role for admin UI users
test/                  end-to-end and concurrency tests
```

## License

MIT. See [LICENSE](LICENSE).
