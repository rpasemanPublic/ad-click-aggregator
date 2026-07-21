# debezium

Connector config for getting Postgres's `hot_ads` table (the hot-key salting allow-list —
see `kafka-streams-app/README.md`'s "Hot-key salting" section) into Kafka via CDC, instead
of a dual write from application code. Debezium reads directly off the Postgres
write-ahead log, so the topic can never drift from the table the way a dual write could
if a process died between two separate writes.

## `hot-ads-connector.json`

Registered against Kafka Connect (`debezium/connect`, running as the `kafka-connect`
service) via:

```
curl -X PUT -H "Content-Type: application/json" \
  -d @debezium/hot-ads-connector.json \
  http://localhost:8083/connectors/hot-ads-connector/config
```

`PUT .../config` rather than `POST /connectors` — idempotent create-or-update, so
re-running registration doesn't fail if it's already registered (same reasoning as
`db/seed.sql`'s `ON CONFLICT DO NOTHING`).

Notable config, and two encoding gotchas that cost real debugging time:

- **`plugin.name: pgoutput`** — Postgres's built-in logical decoding plugin (10+), no
  extra Postgres extension needed. Requires `wal_level=logical` on the `postgres`
  service in `docker-compose.yml`.
- **`publication.autocreate.mode: filtered`** — scopes the Postgres `PUBLICATION` to just
  `hot_ads`, not the whole database.
- **`transforms.unwrap` (`ExtractNewRecordState`) + `drop.tombstones: false`** — flattens
  Debezium's verbose before/after envelope down to just the new row
  (`{"ad_id": "ad-001"}`), while still letting a Postgres `DELETE` produce a real Kafka
  tombstone (key present, value `null`). That tombstone is what lets a downstream
  `GlobalKTable` actually *remove* an ad when it comes off the allow-list, not just stop
  updating it.
- **`transforms.extractKey` (`ExtractField$Key`, field `ad_id`)** — gotcha #1: by
  default, Debezium's *key* is a JSON struct matching the table's primary key
  (`{"ad_id": "ad-001"}`), not a bare string — only the value gets flattened by
  `ExtractNewRecordState`. Without this, a `GlobalKTable` join keyed on the plain `adId`
  string silently never matches anything (no error — it just always misses).
- **`key.converter: StringConverter`, not `JsonConverter`** — gotcha #2, one layer
  deeper: even after `ExtractField$Key` flattens the key to a bare string, Kafka
  Connect's `JsonConverter` still JSON-encodes a scalar string as a *quoted* string
  literal (the actual bytes are `"ad-001"`, 8 bytes, not `ad-001`, 6 bytes). A Kafka
  Streams `Serdes.String()` does a raw UTF-8 decode with no JSON-unquoting, so this also
  silently never matches. `StringConverter` writes a string's raw bytes with no JSON
  envelope, which is what's actually needed here. `value.converter` stays
  `JsonConverter`, since the value is a genuine JSON object.

Both gotchas fail *silently* — no error, no exception, just a `leftJoin` that always
takes the "no match" branch — which is why this took two rounds of "why isn't salting
happening" to actually find, verified by directly inspecting the topic's raw key bytes
via `kafka-console-consumer --print.key=true` rather than trusting the config.

## Changing the connector config after it's already run once

Kafka Connect stores each connector's progress (including "has the initial snapshot
completed") in its own internal `connect-offsets` topic, keyed by *connector name* — not
tied to the connector's registration lifecycle. Deleting a connector via the REST API
does **not** clear that stored offset. Re-registering a config change under the same
name will find the old "snapshot completed" marker and skip re-snapshotting existing
rows, silently keeping whatever encoding they had before the config change.

For a schema/table you can freely touch in dev, the simplest fix is generating a fresh
change event for the affected row(s) (e.g. `DELETE` + re-`INSERT`) so they flow through
the *streaming* path — which does pick up config changes immediately — rather than
fighting Kafka Connect's offset-reset internals or renaming the connector.
