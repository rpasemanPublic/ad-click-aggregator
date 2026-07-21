# analytics-service

Serves aggregated click metrics to advertisers, reading from the tables
`kafka-streams-app` writes to.

## Stack

- Node.js / TypeScript, Express
- `pg` (node-postgres) — plain driver, no ORM/query builder
- `cors` — needed once `analytics-dashboard` (a browser app, on a different port) started
  calling this service; Express doesn't send CORS headers by default, and browsers (unlike
  `curl`) block cross-origin `fetch` responses without them

## API

**`GET /analytics`**
- Query params: `adIds` (comma-separated, e.g. `ad-001,ad-002` — not repeated params;
  Express's query parser has a gotcha where a single repeated-param value doesn't parse
  as an array), `startTime`/`endTime` (ISO 8601 strings), `granularity` (only `minute` is
  wired up — `400` otherwise, since hour/day rollup tables don't exist yet)
- Grouping into per-ad series happens in **Postgres** (`json_agg`/`json_build_object`),
  not application code
- Sets `Cache-Control: no-store` so advertiser refreshes always hit the database fresh
  (GET is *cacheable*, not *cached-by-default* — this header is the actual fix, not
  switching to POST)
- Response: `{ granularity, series: [{ adId, data: [{ bucket, clicks }] }] }` — sparse,
  not positionally aligned (an ad with no clicks in a bucket just has no entry for it,
  rather than an explicit `0`)

**`GET /ads`**
- Returns `{ ads: [{ adId, destinationUrl }] }` — every ad, unfiltered. There's no
  per-advertiser scoping since there's no auth system in this project at all (same
  reasoning as `userId` staying `null` on `ClickEvent`)

Both routes' request/response are fully typed via Express's
`Request<Params, ResBody, ReqBody, ReqQuery>`/`Response<ResBody>` generics.

## Layout

- `index.ts` — Express app, the `/analytics` and `/ads` route handlers
- `db.ts` — Postgres pool, `getClickCounts`, `getAds`

## Status

Fully working, verified end-to-end — a real recorded click shows up correctly via
`/analytics`, and `/ads` correctly feeds `analytics-dashboard`'s ad picker.

## Running locally

From the repo root:

```
docker compose up --build kafka postgres flyway seed kafka-streams-app record-click-service analytics-service
```

Then, for example:

```bash
curl -s "http://localhost:3002/analytics?adIds=ad-001&startTime=2026-07-20T00:00:00Z&endTime=2026-07-21T00:00:00Z&granularity=minute"
curl -s http://localhost:3002/ads
```
