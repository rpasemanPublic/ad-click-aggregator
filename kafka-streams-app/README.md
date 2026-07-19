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

- `Main.scala` — app entry point: loads config, auto-creates the `ad-clicks` topic if it
  doesn't already exist, builds and starts the `KafkaStreams` instance, and registers a
  shutdown hook for a clean `.close()`.
- `Models.scala` — the `ClickEvent` case class, its circe `Encoder`/`Decoder`, and its
  Kafka `Serde`.
- `JsonSerde.scala` — a generic `Serde[T]` builder on top of circe, reusable for any
  circe-encodable type.
- `TopologyBuilder.scala` — builds the Kafka Streams `Topology`: reads `ad-clicks`,
  validates that the Kafka record key matches `ClickEvent.adId` (logs and drops
  mismatches — no dead-letter topic yet), aggregates into 1-minute tumbling windows via
  `groupByKey`/`windowedBy`/`count`, and writes each result out via an injected
  `ClickCountWriter`. Structured as an immutable builder (`case class` + `copy`, private
  constructor, companion `apply()`) rather than a plain object, since more configuration
  is expected here later.
- `ClickCountWriter.scala` — the `ClickCountWriter` trait (dependency-injected into
  `TopologyBuilder` so the topology can be unit tested later with a fake writer instead
  of a real database) and `PostgresClickCountWriter`, which upserts into
  `ad_click_counts_minute` using `scala.util.Using.Manager` for resource cleanup.
- `Database.scala` — a HikariCP connection pool singleton (`object` — Scala's built-in
  singleton, no manual pattern needed).

## Status

Fully working, verified end-to-end against the whole stack (Kafka, Postgres via Flyway
migrations, and `record-click-service` as the producer). Not yet built: dead-letter
handling for key/value mismatches, hour/day rollups, and hot-key salting — all
deliberately deferred until they're actually needed.

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
