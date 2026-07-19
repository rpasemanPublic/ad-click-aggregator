import { Pool } from "pg";

export const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ??
    "postgres://postgres:postgres@localhost:5432/ad_click_aggregator",
});

export async function getDestinationUrl(adId: string): Promise<string | null> {
  const result = await pool.query<{ destination_url: string }>(
    "SELECT destination_url FROM ads WHERE ad_id = $1",
    [adId],
  );
  return result.rows[0]?.destination_url ?? null;
}
