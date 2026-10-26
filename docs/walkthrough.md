# Walkthrough: see the job manager work

Five short demos. Each code block can be pasted into a terminal as a whole.

## Before you start

- **Terminal 1:** start Harper from the project folder and leave it running. Each job prints `job-manager: <jobId> enqueued / claimed / completed` lines here.

  ```bash
  cd /c/learning/harper-job-manager
  npm run dev
  ```

- **Terminal 2:** run the demo commands. Use Git Bash on Windows. In Windows PowerShell, `curl` is a different command.
- **Optional:** open Harper Studio at http://localhost:9925, database `jobs`, and refresh the `Job` and `JobEvent` tables after each step.

Every demo uses the `local` queue, which the Harper worker always serves. Idempotency keys (`try-1`, `s1`, ...) must be new for each run: if a key was used before, the trigger answers `"deduplicated": true` and starts nothing. Change the key to run a demo again.

---

## 1. Your first job

Define a job that echoes its input, then trigger it.

```bash
curl -X PUT localhost:9926/JobDefinition/demo-echo -H 'content-type: application/json' \
  -d '{"id":"demo-echo","enabled":true,"tenant":"acme","queue":"local","handler":"echo"}'

curl -X POST localhost:9926/trigger/demo-echo -H 'content-type: application/json' \
  -H 'idempotency-key: try-1' -d '{"hello":"world"}'
```

Expect `{"jobId":"demo-echo:try-1","status":"pending","deduplicated":false}`. Then look at the job and its log:

```bash
curl localhost:9926/Job/demo-echo:try-1
curl "localhost:9926/JobEvent/?jobId=demo-echo:try-1"
```

The job is `completed` with the result `{"echoed":{"hello":"world"},...}`, and the log shows `enqueued`, `claimed`, `completed`.

**Deduplication:** send the same trigger again. It answers `"deduplicated": true` with the job's current status, and no new job is created.

```bash
curl -X POST localhost:9926/trigger/demo-echo -H 'content-type: application/json' \
  -H 'idempotency-key: try-1' -d '{"hello":"world"}'
```

**Missing key:** without an `Idempotency-Key` header the trigger is rejected with `400`.

```bash
curl -i -X POST localhost:9926/trigger/demo-echo -H 'content-type: application/json' -d '{}'
```

---

## 2. A job you can watch running

A job that sleeps for 15 seconds.

```bash
curl -X PUT localhost:9926/JobDefinition/demo-sleep -H 'content-type: application/json' \
  -d '{"id":"demo-sleep","enabled":true,"queue":"local","handler":"sleep","settings":{"ms":15000}}'

curl -X POST localhost:9926/trigger/demo-sleep -H 'content-type: application/json' \
  -H 'idempotency-key: s1' -d '{}'
```

Check it a few times during the 15 seconds:

```bash
curl localhost:9926/Job/demo-sleep:s1
```

It stays `claimed`, with `claimedBy` set to the worker and a `leaseExpiresAt` time, then becomes `completed`.

**Live stream (optional):** in a third terminal, this prints every change to the job as it happens. Start it, then trigger with a new key such as `s2`, and use that id in the URL. Stop it with Ctrl+C.

```bash
curl -N -H "Accept: text/event-stream" localhost:9926/Job/demo-sleep:s2
```

---

## 3. Retries and the dead letter

A job that fails once and then succeeds, and a job that always fails.

```bash
curl -X PUT localhost:9926/JobDefinition/demo-flaky -H 'content-type: application/json' \
  -d '{"id":"demo-flaky","enabled":true,"queue":"local","handler":"flaky","settings":{"succeedOnAttempt":2}}'

curl -X POST localhost:9926/trigger/demo-flaky -H 'content-type: application/json' \
  -H 'idempotency-key: f1' -d '{}'

curl -X PUT localhost:9926/JobDefinition/demo-broken -H 'content-type: application/json' \
  -d '{"id":"demo-broken","enabled":true,"queue":"local","handler":"alwaysFails","maxAttempts":2}'

curl -X POST localhost:9926/trigger/demo-broken -H 'content-type: application/json' \
  -H 'idempotency-key: b1' -d '{}'
```

Wait about 10 seconds, then:

```bash
curl "localhost:9926/JobEvent/?jobId=demo-flaky:f1"
curl localhost:9926/Job/demo-broken:b1
curl "localhost:9926/Job/?status=dead"
```

- `demo-flaky:f1` fails on attempt 1, is retried after 5 seconds, and completes on attempt 2.
- `demo-broken:b1` fails twice and ends as `dead`, with the reason in `lastError`.
- `?status=dead` lists every dead job: the dead-letter queue.

---

## 4. Routing an event to a job

A rule that turns "file created" events into jobs, reading the tenant and job type from the file path.

```bash
curl -X PUT localhost:9926/EventSubscription/files -H 'content-type: application/json' \
  -d '{"id":"files","enabled":true,"eventTypes":["storage.blob.created"],"subjectPattern":"^/files/(?<tenant>[^/]+)/(?<jobType>[^/]+)/","definitionTemplate":"{tenant}-{jobType}"}'

curl -X PUT localhost:9926/JobDefinition/acme-import -H 'content-type: application/json' \
  -d '{"id":"acme-import","enabled":true,"tenant":"acme","queue":"local","handler":"echo"}'

curl -X POST localhost:9926/events -H 'content-type: application/json' \
  -d '{"id":"file-1","type":"storage.blob.created","subject":"/files/acme/import/orders.csv"}'
```

The answer shows the event `routed` to `acme-import`: the rule read `acme` and `import` from the path.

Now a file in a folder with no matching job definition:

```bash
curl -X POST localhost:9926/events -H 'content-type: application/json' \
  -d '{"id":"file-2","type":"storage.blob.created","subject":"/files/acme/unknown/x.csv"}'

curl "localhost:9926/Event/?status=unrouted"
```

It is recorded as `unrouted`, the router's dead letter, with the reason in `error`.

---

## 5. A cron job

A job that runs at the start of every minute.

```bash
curl -X PUT localhost:9926/JobDefinition/demo-cron -H 'content-type: application/json' \
  -d '{"id":"demo-cron","enabled":true,"queue":"local","handler":"echo","cron":"* * * * *"}'
```

The first run happens at the next full minute (the schedule is set up on the first tick). Terminal 1 then logs `1 cron runs enqueued` each minute. List the runs:

```bash
curl "localhost:9926/Job/?definitionId=demo-cron"
```

Each run's id ends in `:cron-<scheduled time>`, which is also its idempotency key.

**Stop it when you're done**, or it keeps running. Pause it:

```bash
curl -X PUT localhost:9926/JobDefinition/demo-cron -H 'content-type: application/json' \
  -d '{"id":"demo-cron","enabled":false,"queue":"local","handler":"echo","cron":"* * * * *"}'
```

Or delete it:

```bash
curl -X DELETE localhost:9926/JobDefinition/demo-cron
```

---

## Clean up

Delete the demo definitions and the routing rule. Their jobs stay in the `Job` table; delete those in Studio if you want a clean slate.

```bash
for id in demo-echo demo-sleep demo-flaky demo-broken demo-cron acme-import; do
  curl -s -o /dev/null -w "JobDefinition/$id -> %{http_code}\n" -X DELETE localhost:9926/JobDefinition/$id
done
curl -s -o /dev/null -w "EventSubscription/files -> %{http_code}\n" -X DELETE localhost:9926/EventSubscription/files
```
