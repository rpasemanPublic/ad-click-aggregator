# analytics-dashboard

A small React dashboard for viewing click metrics: pick which ads to look at, refresh,
see a chart.

## Stack

- React / Vite / TypeScript
- Recharts — chosen over Chart.js since our `analytics-service` response is a sparse,
  per-ad series (not positionally aligned), which fits Recharts' handling of missing
  data points more naturally
- Plain `fetch` — no HTTP client library, just two simple `GET` calls

## What it does

- Fetches the ad list from `GET /ads` on load, renders checkboxes for each
- "Refresh" button (deliberate — matches `analytics-service`'s `Cache-Control: no-store`,
  no auto-fetch-on-selection-change) queries the last hour of data via `GET /analytics`
  for whichever ads are checked
- `toChartData` pivots the API's per-ad-series response into the shared-array-by-bucket
  shape Recharts' `<LineChart>` needs (any multi-series chart with a shared X-axis needs
  this same pivot, not just Recharts)

## Layout

- `src/App.tsx` — the whole app; small enough not to need splitting up yet

## Config

`VITE_ANALYTICS_API_URL` (Vite requires the `VITE_` prefix for env vars exposed to
client-side code, unlike Node's `process.env`) — falls back to `http://localhost:3002`
if unset.

## Status

Fully working, verified live: selecting an ad and clicking Refresh correctly charts a
real recorded click.

## Running locally

`analytics-service` needs to be running first. From the repo root:

```
docker compose up --build kafka postgres flyway seed kafka-streams-app record-click-service analytics-service
```

Then, from this directory, for fast iteration with hot reload (preferred over Docker for
day-to-day frontend work):

```
npm install
npm run dev
```
