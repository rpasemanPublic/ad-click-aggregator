# Ad Click Aggregator

A system that records ad clicks, redirects users to the advertiser's destination URL, and
lets advertisers query aggregated click metrics over time (minimum granularity: 1 minute).

Built to practice an event-driven architecture using Kafka and Kafka Streams.

## Scale target

- 10M active ads
- ~100M clicks/day (~1,150 clicks/sec average)

## Architecture

```
ad-click-simulator (React)
        │ POST /recordClick { adId }
        ▼
record-click-service (Node/TS)
        │ produces ClickEvent
        ▼
   Kafka topic: ad-clicks
        │
        ▼
kafka-streams-app (Scala)
  - windowed aggregation (per-minute click counts, per ad)
        │
        ▼
   Postgres (aggregated rollups)
        ▲
        │ GET /analytics
analytics-service (Node/TS)
        ▲
        │
analytics-dashboard (React)
```

## Services

| Service                 | Language        | Purpose                                         |
|--------------------------|-----------------|--------------------------------------------------|
| `record-click-service`   | Node / TS       | Records clicks, produces to Kafka, returns destination URL |
| `kafka-streams-app`      | Scala           | Consumes click events, aggregates per-minute counts per ad |
| `analytics-service`      | Node / TS       | Serves aggregated click metrics to advertisers  |
| `ad-click-simulator`     | React / Vite    | Simulates a page with clickable ads             |
| `analytics-dashboard`    | React / Vite    | Displays click metrics over time                |

## Running locally

```
docker compose up --build
```

| Service               | URL                     |
|------------------------|--------------------------|
| record-click-service   | http://localhost:3001   |
| analytics-service      | http://localhost:3002   |
| ad-click-simulator     | http://localhost:5173   |
| analytics-dashboard    | http://localhost:5174   |
| Kafka                  | localhost:9092          |
| Postgres               | localhost:5432          |

## Status

- `kafka-streams-app` — bootstrap shell working end-to-end: config, auto-creates the
  `ad-clicks` topic on startup, and reads it into a `KStream[String, ClickEvent]` with
  working JSON Serdes. The windowed aggregation itself (count clicks per ad per minute)
  and the Postgres sink are not yet built. See
  [`kafka-streams-app/README.md`](kafka-streams-app/README.md) for details.
- `record-click-service`, `analytics-service`, `ad-click-simulator`, `analytics-dashboard`
  — scaffold only, no business logic yet.
