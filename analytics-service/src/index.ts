import express from "express";

const app = express();

const PORT = process.env.PORT ?? 3002;

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

// TODO: implement once the aggregated data store is designed
app.get("/analytics", (_req, res) => {
  res.status(501).json({ error: "not implemented" });
});

app.listen(PORT, () => {
  console.log(`analytics-service listening on port ${PORT}`);
});
