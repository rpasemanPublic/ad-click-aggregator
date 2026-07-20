import { Pool } from "pg";

export const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ??
    "postgres://postgres:postgres@localhost:5432/ad_click_aggregator",
});

interface AdSeries {
  ad_id: string;
  data: { bucket: string; clicks: number }[];
}

export async function getClickCounts(
  adIds: string[],
  startTime: string,
  endTime: string,
): Promise<AdSeries[]> {
  const result = await pool.query<AdSeries>(
    `SELECT
       ad_id,
       json_agg(json_build_object('bucket', window_start, 'clicks', click_count) ORDER BY window_start) AS data
     FROM ad_click_counts_minute
     WHERE ad_id = ANY($1) AND window_start >= $2 AND window_start < $3
     GROUP BY ad_id`,
    [adIds, startTime, endTime],
  );
  return result.rows;
}
