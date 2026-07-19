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

## Database

Schema migrations live in `db/migrations` (Flyway), applied automatically by the
`flyway` service before any dependent service starts. Local-dev sample data
(`db/seed.sql` — a handful of sample ads) is kept separate from the versioned
migrations and applied by the `seed` service, since seed data isn't schema and
shouldn't run against a real deployment the same way migrations would.

## Status

- **`record-click-service` and `kafka-streams-app` are fully working, verified
  end-to-end.** A real `POST /recordClick` looks up the ad, produces a click event to
  Kafka, gets aggregated into 1-minute windows, and lands in Postgres. See
  [`record-click-service/README.md`](record-click-service/README.md) and
  [`kafka-streams-app/README.md`](kafka-streams-app/README.md) for details.
- `analytics-service`, `ad-click-simulator`, `analytics-dashboard` — scaffold only, no
  business logic yet.
