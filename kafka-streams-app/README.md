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
  "Latency" below), auto-creates the `ad-clicks` topic if it doesn't already exist and
  looks up its actual partition count via `Admin.describeTopics` (used to size hot-key
  salting — see "Hot-key salting"; computed once at startup, not polled — see that
  section for why), builds and starts the `KafkaStreams` instance, and registers a
  shutdown hook for a clean `.close()`.
- `Models.scala` — `ClickEvent` (the raw click), `ClickEventWithOffset` (a click paired
  with the Kafka record offset it came from), `ClickCountAggregate` (the running
  `count`, `maxTimestamp`, and `lastOffset` per shard per window), `PartialWithShard`
  (a stage-1 partial paired with the shard key it came from, used when re-keying into
  stage 2), and `MergedClickCountAggregate` (a map of shard key → that shard's latest
  `ClickCountAggregate`, stage 2's accumulator — see "Hot-key salting" for why this is a
  map of *latest values*, not a running sum), plus circe `Encoder`/`Decoder`s and Kafka
  `Serde`s for every type that actually crosses a topic boundary (stage 1's `.selectKey`
  and stage 2's `.map` both trigger repartition topics, which need real serdes even
  though `kafka-streams-scala`'s implicit resolution isn't available here).
- `JsonSerde.scala` — a generic `Serde[T]` builder on top of circe, reusable for any
  circe-encodable type.
- `TopologyBuilder.scala` — builds the Kafka Streams `Topology`: reads `ad-clicks`,
  validates that the Kafka record key matches `ClickEvent.adId` (logs and drops
  mismatches — no dead-letter topic yet), tags each record with its Kafka offset via
  `processValues` + a `FixedKeyProcessor` (needed because the plain DSL's `Aggregator`
  lambda has no access to record metadata — see "Multi-instance correctness" below),
  looks up each click's ad against a `GlobalKTable` sourced from `hot_ads` (see
  "Hot-key salting") and conditionally salts the key, aggregates the (possibly salted)
  stream into 1-minute tumbling windows (stage 1), then re-keys back to the plain
  `adId` and merges shard partials into the true total (stage 2), writing the final
  result out via an injected `ClickCountWriter`. Structured as an immutable builder
  (`case class` + `copy`, private constructor, companion `apply()`) rather than a plain
  object, since more configuration is expected here later.
- `ClickCountWriter.scala` — the `ClickCountWriter` trait (dependency-injected into
  `TopologyBuilder` so the topology can be unit tested later with a fake writer instead
  of a real database) and `PostgresClickCountWriter`, which conditionally upserts into
  `ad_click_counts_minute` (see "Multi-instance correctness" and "Hot-key salting") using
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

## Hot-key salting

A single `adId` always hashes to the same `ad-clicks` partition, no matter how many
partitions or `kafka-streams-app` instances exist — a genuinely hot ad concentrates all
its load on one instance with no way to add parallelism. This wasn't built speculatively;
it was deferred until a real hot key could be reproduced and measured (see
`load-test/README.md`'s hot-key section), the same way the zombie-consumer fix above was
validated against a real reproduction rather than reasoned about in the abstract.
Reproduced: a synthetic 20:1-skewed load test put **83% of all traffic on a single
partition**.

**Which ads get salted is data-driven, not blanket.** Real ad platforms usually know
likely-hot ads in advance (large campaign budget, a broad targeted search term) rather
than needing to detect hotness live, so this models that: a Postgres `hot_ads` table is
an explicit allow-list (presence = hot), decoupled from *how* hotness gets decided so
that could change later (a scoring rule, a cron job) without touching the schema or the
streams topology. Getting that table into the streams app uses CDC (Debezium, via Kafka
Connect) rather than a dual write, so the Kafka topic can never drift from Postgres — see
`debezium/README.md`. `kafka-streams-app` consumes that topic into a `GlobalKTable` and
`leftJoin`s it against every click (`leftJoin`, not `join`, since most ads aren't hot and
an inner join would silently drop their clicks entirely).

**The topology shape doesn't change per-ad** — every click flows through the same two
stages; only the shard-selection function is conditional. Non-hot ads always resolve to
a single fixed shard (a no-op, equivalent to no salting). Hot ads get a random shard
suffix (`adId#0` .. `adId#(N-1)`), spreading their traffic across up to `N` different
keys before Kafka's own hashing decides which partition each shard lands on:

- **Stage 1**: aggregate per salted key per window (`groupByKey`/`windowedBy`/
  `aggregate`, same as before, just keyed by the salted key instead of the plain
  `adId`).
- **Stage 2**: re-key back to a composite `adId|windowStart` string (carrying the shard
  key forward in the value, since it's discarded from the key) and merge all shards for
  that `(adId, window)` into the true total.

`N` isn't arbitrary — it needs to be large enough that random shard-to-partition hashing
has good odds of actually covering every partition (a "balls into bins" coverage
problem; back-of-envelope, roughly `partitionCount × ln(partitionCount / ε)` for high
coverage confidence), but not so large that it fragments state pointlessly (`N` distinct
salted keys means `N` separate state-store entries, changelog writes, and shards to
merge in stage 2, regardless of how well they hash-distribute). Sized here as
`4 × partitionCount`, computed once at startup from a real `Admin.describeTopics` call
rather than hardcoded — see the `Main.scala` bullet above. Deliberately *not* polled
live: growing partitions on a live keyed topic changes `hash(key) mod partitionCount`
for every existing key anyway, which is a much bigger event than "N needs to grow," so a
restart (which re-derives `N` for free) is the right response, not a background thread.

**Stage 2's merge has a subtlety that's easy to get wrong**: stage 1 re-emits each
shard's *full running total* on every update (standard `KTable` aggregate semantics),
not a delta. Naively summing incoming values in stage 2 would double-count every
historical emission. `MergedClickCountAggregate` instead tracks a map of shard → that
shard's *latest* `ClickCountAggregate`, always replacing (never adding to) a shard's
entry; the true count is `shardAggregates.values.map(_.count).sum`, recomputed from the
map rather than accumulated incrementally.

**Salting also breaks the single-`lastOffset` correctness guard** from "Multi-instance
correctness" above — a merged aggregate now combines offsets from `N` independent
partitions with no shared ordering, so a single scalar can't detect staleness anymore.
Generalized to a per-shard offset map (`ad_click_counts_minute.shard_offsets`, `jsonb`):
the write guard rejects only if the incoming write would *regress* any individual
shard's offset relative to what's stored, rather than requiring every shard to have
improved — which correctly lets harmless duplicate/no-op writes through while still
catching a zombie trying to overwrite newer state with a stale snapshot.

Verified against the same reproduction: with salting live, that 83%-on-one-partition
skew became a roughly even 21-29% spread across all four partitions.

## Status

Fully working, verified end-to-end against the whole stack (Kafka, Postgres via Flyway
migrations, and `record-click-service` as the producer), including under load, under real
multi-instance rebalancing/zombie scenarios, and under a real reproduced hot-key skew (see
"Hot-key salting"). Not yet built: dead-letter handling for key/value mismatches, hour/day
rollups, and client-side `clickId` generation (a *different*, retry-stable ID from
`requestId`, which
`record-click-service` already generates server-side per attempt) — all deliberately
deferred until actually needed.

## Running locally

From the repo root:

```
docker compose up --build kafka postgres flyway seed kafka-connect register-hot-ads-connector kafka-streams-app
```

(`kafka-connect` and `register-hot-ads-connector` are only needed for hot-key salting to
actually salt anything — see "Hot-key salting" and `debezium/README.md`. Without them,
`kafka-streams-app` still runs fine; the `hot_ads` `GlobalKTable` just never has any
entries, so every ad is treated as not-hot.)

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
