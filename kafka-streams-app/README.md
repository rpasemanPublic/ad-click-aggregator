# kafka-streams-app

Consumes raw click events from the `ad-clicks` Kafka topic and aggregates them into
per-minute click counts per ad.

## Stack

- Scala 2.13
- Kafka Streams — using the plain Java API directly, not `kafka-streams-scala`. That
  module is deprecated as of Kafka 4.3 (KIP-1244) and will be removed in Kafka 5.0, so
  serdes are wired up explicitly via `Consumed.with(...)` rather than relying on
  `kafka-streams-scala`'s implicit resolution.
- circe for JSON encoding/decoding
- Logback for logging (required — without a logging backend, Kafka Streams' internal
  errors, including background stream-thread failures, are silently discarded)
- PostgreSQL JDBC driver + HikariCP for the connection pool. Plain JDBC, no query
  builder/ORM (ruled out Doobie — needs Cats Effect — and Quill, due to past bad
  experience with its macro-derivation compile times)

## Layout

- `Main.scala` — app entry point: loads config (including `commit.interval.ms=2000`,
  lowered from the 30s default so aggregated counts reach Postgres quickly — see
  "Latency" below), auto-creates the `ad-clicks` topic if it doesn't already exist,
  builds and starts the `KafkaStreams` instance, and registers a shutdown hook for a
  clean `.close()`.
- `Models.scala` — `ClickEvent` (the raw click), `ClickEventWithOffset` (a click paired
  with the Kafka record offset it came from), and `ClickCountAggregate` (the running
  `count`, `maxTimestamp`, and `lastOffset` per window), plus circe `Encoder`/`Decoder`s
  and Kafka `Serde`s for the types that actually cross the wire (`ClickEventWithOffset`
  doesn't — it only exists transiently between processor steps).
- `JsonSerde.scala` — a generic `Serde[T]` builder on top of circe, reusable for any
  circe-encodable type.
- `TopologyBuilder.scala` — builds the Kafka Streams `Topology`: reads `ad-clicks`,
  validates that the Kafka record key matches `ClickEvent.adId` (logs and drops
  mismatches — no dead-letter topic yet), tags each record with its Kafka offset via
  `processValues` + a `FixedKeyProcessor` (needed because the plain DSL's `Aggregator`
  lambda has no access to record metadata — see "Multi-instance correctness" below),
  aggregates into 1-minute tumbling windows via `groupByKey`/`windowedBy`/`aggregate`,
  and writes each result out via an injected `ClickCountWriter`. Structured as an
  immutable builder (`case class` + `copy`, private constructor, companion `apply()`)
  rather than a plain object, since more configuration is expected here later.
- `ClickCountWriter.scala` — the `ClickCountWriter` trait (dependency-injected into
  `TopologyBuilder` so the topology can be unit tested later with a fake writer instead
  of a real database) and `PostgresClickCountWriter`, which conditionally upserts into
  `ad_click_counts_minute` (see "Multi-instance correctness") using
  `scala.util.Using.Manager` for resource cleanup, and logs observed staleness
  (`now - maxTimestamp`) on every successful write.
- `Database.scala` — a HikariCP connection pool singleton (`object` — Scala's built-in
  singleton, no manual pattern needed).

## Latency

A hellointerview follow-up question for this problem asks for clicks to be reflected in
aggregated results within 5 seconds. Kafka Streams batches `KTable` updates internally
(the "record cache") and only flushes to `.foreach()` — and therefore to Postgres —
whichever comes first: the cache fills up, or `commit.interval.ms` elapses (30s by
default, which would blow well past 5s). `commit.interval.ms` is set to 2000 here to
bound the worst case comfortably under the target.

This isn't just a config-level assumption — `PostgresClickCountWriter` logs real
observed staleness on every write (see `load-test/README.md` for how this was verified
under load: staleness stayed in single/low-double-digit milliseconds, with one outlier
around 1.6s, still well under target).

## Multi-instance correctness

Running more than one instance of this service is expected — `container_name` was
deliberately removed from `docker-compose.yml` so it can be scaled
(`docker compose up --scale kafka-streams-app=N`). That raises a real correctness
question: could two instances briefly race on the same Postgres row during a rebalance?

Specifically: a "zombie" instance — one that's stalled (a GC pause, a network blip) long
enough to be evicted from the consumer group, but is still alive and can still finish
processing whatever it already had buffered — could write stale data to Postgres *after*
a healthier instance has already taken over and written fresher data. Neither of Kafka's
own protections fully cover this: consumer-group generation fencing only gates offset
commits (not arbitrary writes), and exactly-once semantics only fence writes that go
through Kafka's own transactional producer — Postgres is outside that boundary either
way.

The fix: `ad_click_counts_minute.last_offset` and a conditional upsert —
`ON CONFLICT ... DO UPDATE ... WHERE EXCLUDED.last_offset > ad_click_counts_minute.last_offset`.
Guarded by the Kafka record's own offset (canonical, broker-assigned, strictly
increasing per partition) rather than event timestamp, since a zombie's un-flushed local
state could technically have a *higher* timestamp than what a fresh instance rebuilds
from the changelog.

This was verified against a real reproduced zombie, not just reasoned about: `docker
pause` (freezes every process in a container via the OS's freezer cgroup — a faithful GC
pause simulation, unlike killing the container outright) on one instance while under
load, confirming its partitions built real lag, then `docker unpause` — which resumed it
mid-stream, still believing it owned what it owned before. It *did* try to keep working
and *did* write stale data (`staleness=91573ms` in the log) before Kafka fenced it
(`TaskMigratedException`, from `max.poll.interval.ms` being exceeded) and it rejoined the
group cleanly. Checking the row afterward: the legitimate instance's much higher, correct
count was what stuck — the stale write was silently rejected by the `last_offset` guard.

## Status

Fully working, verified end-to-end against the whole stack (Kafka, Postgres via Flyway
migrations, and `record-click-service` as the producer), including under load and under
real multi-instance rebalancing/zombie scenarios. Not yet built: dead-letter handling for
key/value mismatches, hour/day rollups, hot-key salting, and client-side `clickId`
generation (a *different*, retry-stable ID from `requestId`, which
`record-click-service` already generates server-side per attempt) — all deliberately
deferred until actually needed.

## Running locally

From the repo root:

```
docker compose up --build kafka postgres flyway seed kafka-streams-app
```

Or run directly from IntelliJ against locally running dependencies:

```
docker compose up -d kafka postgres flyway seed
```

then run `Main` — it falls back to `localhost:9092`/`localhost:5432` if the
`KAFKA_BROKER`/`DB_URL`/`DB_USER`/`DB_PASSWORD` environment variables aren't set (which
they will be automatically when run via `docker compose`).

`build.sbt` sets `Compile / run / fork := true` — without this, running via `sbt run`
exits immediately after `main()` returns, since the Streams background threads don't
keep sbt's own (unforked) JVM alive.

To run multiple instances (e.g. to watch partition rebalancing yourself):

```
docker compose up --build -d --scale kafka-streams-app=2 kafka postgres flyway seed kafka-streams-app
```
