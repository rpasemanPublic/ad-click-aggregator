# load-test

A small stress test for the click-recording pipeline: `record-click-service` → Kafka →
`kafka-streams-app`'s windowed aggregation → Postgres.

Not owned by any one service — same reasoning as `db/migrations` living at the repo
root — since it exercises the whole pipeline, not just `record-click-service`.

## Stack

- `autocannon` — HTTP load generator
- `pg` — looks up real `ad_id`s from the `ads` table rather than hardcoding them, so
  the test automatically spreads load across whatever's actually seeded

## What it does

Sends concurrent `POST /recordClick` requests (20 connections, 15 seconds) spread
across every ad in the database, then prints an autocannon summary (requests/sec,
latency percentiles for the HTTP calls themselves).

That summary only covers `record-click-service`'s HTTP response time — it doesn't tell
you how long it took for a click to actually be reflected in Postgres. For that, check
`kafka-streams-app`'s logs (`docker logs kafka-streams-app`) for `staleness=` lines,
which `PostgresClickCountWriter` logs on every write — see
[`kafka-streams-app/README.md`](../kafka-streams-app/README.md#latency).

## Running it

The whole stack needs to be up first:

```
docker compose up --build kafka postgres flyway seed kafka-streams-app record-click-service
```

Then, from this directory:

```
npm install
npm run stress-test
```

## Result (last run)

21,851 total clicks across 5 ads in 15 seconds, zero errors, zero key/value mismatches.
Staleness (click timestamp → written to Postgres) stayed in single/low-double-digit
milliseconds for most flushes, with one outlier around 1.6s — comfortably under the 5s
target from the hellointerview follow-up question this is based on.

## Windows note

If you hit `password authentication failed for user "postgres"` when running this (but
`docker exec postgres psql ...` works fine), check for a native Postgres Windows
service also bound to port 5432 — `Get-Service *postgres*` in PowerShell. Container-to-
container connections (e.g. `record-click-service` → `postgres:5432` via Docker's
internal network) are unaffected; only host-side connections like this script go
through the host's port mapping, where the conflict can happen.
