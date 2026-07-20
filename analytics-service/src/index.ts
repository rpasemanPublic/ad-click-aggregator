import express from "express";
import { getClickCounts } from "./db.js";

import type { Request, Response } from "express";

const app = express();

interface AnalyticsQuery {
  adIds: string;
  startTime: string;
  endTime: string;
  granularity: string;
}

interface AnalyticsResponse {
  granularity: string;
  series: { adId: string; data: { bucket: string; clicks: number }[] }[];
}

interface ErrorResponse {
  error: string;
}

const PORT = process.env.PORT ?? 3002;

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.get(
  "/analytics",
  async (
    req: Request<
      {},
      AnalyticsResponse | ErrorResponse,
      unknown,
      AnalyticsQuery
    >,
    res: Response<AnalyticsResponse | ErrorResponse>,
  ) => {
    const adIds = req.query.adIds.split(",");
    const { startTime, endTime, granularity } = req.query;

    if (granularity !== "minute") {
      res
        .status(400)
        .json({ error: "only granularity=minute is currently supported" });
      return;
    }

    const rows = await getClickCounts(adIds, startTime, endTime);

    res.set("Cache-Control", "no-store");
    res.json({
      granularity,
      series: rows.map((row) => ({ adId: row.ad_id, data: row.data })),
    });
  },
);

app.listen(PORT, () => {
  console.log(`analytics-service listening on port ${PORT}`);
});
