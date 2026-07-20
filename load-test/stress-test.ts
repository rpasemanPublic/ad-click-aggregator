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

async function main() {
  const adIds = await getAdIds();

  const result = await autocannon({
    url: "http://localhost:3001",
    connections: 20,
    duration: 15,
    requests: adIds.map((adId) => ({
      method: "POST",
      path: "/recordClick",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ adId }),
    })),
  });

  console.log(autocannon.printResult(result));
}

main();
