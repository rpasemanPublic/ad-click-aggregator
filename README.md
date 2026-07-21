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
  - conditional hot-key salting (see below) + windowed aggregation
  - (per-minute click counts, per ad)
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

Hot-key salting (see [`kafka-streams-app/README.md`](kafka-streams-app/README.md#hot-key-salting))
needs to know which ads are expected to be hot, fed in via CDC rather than a dual write:

```
Postgres: hot_ads table (allow-list)
        │ WAL
        ▼
Kafka Connect + Debezium
        │ produces to a Kafka topic
        ▼
kafka-streams-app: GlobalKTable, joined against every click
```

## Services

| Service                | Language     | Purpose                                                    |
| ---------------------- | ------------ | ---------------------------------------------------------- |
| `record-click-service` | Node / TS    | Records clicks, produces to Kafka, returns destination URL |
| `kafka-streams-app`    | Scala        | Consumes click events, aggregates per-minute counts per ad |
| `analytics-service`    | Node / TS    | Serves aggregated click metrics to advertisers             |
| `ad-click-simulator`   | React / Vite | Simulates a page with clickable ads                        |
| `analytics-dashboard`  | React / Vite | Displays click metrics over time                           |

Plus infrastructure, not custom application code: Kafka, Postgres, and Kafka Connect
running the Debezium Postgres connector (config in [`debezium/`](debezium/README.md)) —
CDC for the `hot_ads` allow-list used by hot-key salting.

## Running locally

```
docker compose up --build
```

| Service              | URL                   |
| -------------------- | --------------------- |
| record-click-service | http://localhost:3001 |
| analytics-service    | http://localhost:3002 |
| ad-click-simulator   | http://localhost:5173 |
| analytics-dashboard  | http://localhost:5174 |
| Kafka                | localhost:9092        |
| Postgres             | localhost:5432        |

## Testing locally

Record a click:

```bash
curl -s -i -X POST http://localhost:3001/recordClick -H "Content-Type: application/json" -d '{"adId":"ad-001"}'
```

```powershell
# Windows PowerShell — `curl` is aliased to Invoke-WebRequest there, which doesn't
# understand curl's flags, so use this instead:
Invoke-RestMethod -Uri "http://localhost:3001/recordClick" -Method Post -ContentType "application/json" -Body '{"adId":"ad-001"}'
```

Sample `adId`s available from `db/seed.sql`: `ad-001` through `ad-005`.

Tail logs:

```
docker logs -f --tail 50 record-click-service
docker logs -f --tail 50 kafka
docker logs -f --tail 50 kafka-streams-app
```

Watch messages actually land on the `ad-clicks` topic:

```
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ad-clicks --from-beginning
```

Check click-to-Postgres latency (`kafka-streams-app` logs this on every write — see
[`kafka-streams-app/README.md`](kafka-streams-app/README.md#latency)):

```
docker logs kafka-streams-app --since 5m | grep staleness
```

Check what's landed in Postgres:

```
docker exec -it postgres psql -U postgres -d ad_click_aggregator
```

Then, at the `psql` prompt:

```sql
\dt                                      -- list tables
SELECT * FROM ads;                       -- seeded ad metadata
SELECT * FROM hot_ads;                   -- hot-key salting allow-list
SELECT * FROM ad_click_counts_minute;    -- aggregated click counts
\q                                        -- exit
```

## Load testing

See [`load-test/README.md`](load-test/README.md) — a small stress test tool that
spreads load across every ad in the database and verifies click-to-Postgres latency
stays within target.

## Database

Schema migrations live in `db/migrations` (Flyway), applied automatically by the
`flyway` service before any dependent service starts. Local-dev sample data
(`db/seed.sql` — a handful of sample ads) is kept separate from the versioned
migrations and applied by the `seed` service, since seed data isn't schema and
shouldn't run against a real deployment the same way migrations would.

## Status

- **The whole backend — `record-click-service`, `kafka-streams-app`, and
  `analytics-service` — plus `analytics-dashboard` are fully working, verified
  end-to-end, including under load and under real multi-instance failure scenarios.** A
  real `POST /recordClick` looks up the ad, produces a click event to Kafka, gets
  aggregated into 1-minute windows, lands in Postgres, and is queryable/chartable via
  `analytics-dashboard` — with observed click-to-Postgres latency staying well within a
  5-second target under load, cross-service request tracing (`requestId`, correlatable
  across `record-click-service` and `kafka-streams-app` logs), a real reproduced
  "zombie consumer" scenario (`docker pause`/`unpause`) confirming `kafka-streams-app`
  handles multi-instance rebalancing correctly, and a real reproduced hot-partition skew
  (83% of traffic on one partition) fixed via conditional hot-key salting, verified back
  down to a roughly even spread. See
  [`record-click-service/README.md`](record-click-service/README.md),
  [`kafka-streams-app/README.md`](kafka-streams-app/README.md) (see "Multi-instance
  correctness" for the rebalancing work and "Hot-key salting" for the partition-skew
  work),
  [`analytics-service/README.md`](analytics-service/README.md),
  [`analytics-dashboard/README.md`](analytics-dashboard/README.md),
  [`debezium/README.md`](debezium/README.md), and
  [`load-test/README.md`](load-test/README.md) for details.
- `ad-click-simulator` — the last unbuilt piece, still scaffold only.
