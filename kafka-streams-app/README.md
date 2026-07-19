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

## Layout

- `Main.scala` — app entry point: loads config, auto-creates the `ad-clicks` topic if it
  doesn't already exist, builds and starts the `KafkaStreams` instance, and registers a
  shutdown hook for a clean `.close()`.
- `Models.scala` — the `ClickEvent` case class, its circe `Encoder`/`Decoder`, and its
  Kafka `Serde`.
- `JsonSerde.scala` — a generic `Serde[T]` builder on top of circe, reusable for any
  circe-encodable type.
- `TopologyBuilder.scala` — builds the Kafka Streams `Topology`. Currently reads
  `ad-clicks` into a `KStream[String, ClickEvent]`.

## Status

Working: config, topic auto-creation, and the source stream (with correct Serdes) all
run cleanly end-to-end. Not yet built: the actual windowed aggregation
(`groupByKey` → `windowedBy` → `count`) and writing results to Postgres.

## Running locally

From the repo root:

```
docker compose up --build kafka kafka-streams-app
```

Or run directly from IntelliJ against a locally running broker:

```
docker compose up -d kafka
```

then run `Main` — it falls back to `localhost:9092` if the `KAFKA_BROKER` environment
variable isn't set (which it will be automatically when run via `docker compose`).

`build.sbt` sets `Compile / run / fork := true` — without this, running via `sbt run`
exits immediately after `main()` returns, since the Streams background threads don't
keep sbt's own (unforked) JVM alive.
