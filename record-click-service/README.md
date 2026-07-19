# record-click-service

Handles ad clicks: given an `adId`, looks up the advertiser's destination URL, records
the click (produced to Kafka for aggregation by `kafka-streams-app`), and returns the
URL for the client to redirect to.

## Stack

- Node.js / TypeScript, Express
- `pg` (node-postgres) — plain driver, no ORM/query builder. One simple lookup query
  (`ads` by `ad_id`) doesn't need one; ruled out Prisma for the same reason even though
  the user has prior experience with it
- `kafkajs` — Kafka producer
- `cookie-parser` — lightweight session tracking only. There's no auth/session store in
  this project's scope, so `userId` always stays `null`; `sessionId` is just an opaque
  `crypto.randomUUID()` set as a cookie on first visit, reused after that

## API

**`POST /recordClick`**
- Body: `{ "adId": string }`
- Looks up `destination_url` from the `ads` table; `404` if the `adId` doesn't exist
- Produces a `ClickEvent` to the `ad-clicks` Kafka topic, keyed by `adId` (capturing
  `timestamp`, `userId`, `sessionId`, `ipAddress`, `userAgent`, `referrerUrl`)
- Sets a `sessionId` cookie if the request doesn't already have one
- Returns `{ "destinationUrl": string }`

## Layout

- `index.ts` — Express app and the `/recordClick` route handler
- `db.ts` — Postgres pool + `getDestinationUrl`
- `kafka.ts` — Kafka producer, the `ClickEvent` type, and `sendClickEvent`

## Status

Fully working, verified end-to-end against the whole stack — a real click flows through
this service, Kafka, `kafka-streams-app`'s windowed aggregation, and lands correctly in
Postgres.

## Running locally

From the repo root:

```
docker compose up --build kafka postgres flyway seed kafka-streams-app record-click-service
```

Then, for example:

```bash
curl -s -i -X POST http://localhost:3001/recordClick -H "Content-Type: application/json" -d '{"adId":"ad-001"}'
```

```powershell
# Windows PowerShell — `curl` is aliased to Invoke-WebRequest there, which doesn't
# understand curl's flags, so use this instead:
Invoke-RestMethod -Uri "http://localhost:3001/recordClick" -Method Post -ContentType "application/json" -Body '{"adId":"ad-001"}'
```

Sample `adId`s available from `db/seed.sql`: `ad-001` through `ad-005`.
