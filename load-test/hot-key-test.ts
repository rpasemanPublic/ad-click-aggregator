import autocannon from "autocannon";
import { Pool } from "pg";

async function getAdIds(): Promise<string[]> {
  const pool = new Pool({
    connectionString:
      process.env.DATABASE_URL ??
      "postgres://postgres:postgres@localhost:5432/ad_click_aggregator",
  });

  const result = await pool.query<{ ad_id: string }>("SELECT ad_id FROM ads");
  await pool.end();

  return result.rows.map((row) => row.ad_id);
}

function makeRequest(adId: string) {
  return {
    method: "POST",
    path: "/recordClick",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ adId }),
  };
}

async function main() {
  const adIds = await getAdIds();
  const [hotAdId, ...restAdIds] = adIds;

  // Heavily skew traffic toward one ad (~20:1 against each of the others),
  // so its Kafka partition gets far more messages than the rest.
  const requests = [
    ...Array(20).fill(makeRequest(hotAdId)),
    ...restAdIds.map(makeRequest),
  ];

  console.log(`Hammering ${hotAdId} disproportionately...`);

  const result = await autocannon({
    url: "http://localhost:3001",
    connections: 20,
    duration: 30,
    requests,
  });

  console.log(autocannon.printResult(result));
}

main();
