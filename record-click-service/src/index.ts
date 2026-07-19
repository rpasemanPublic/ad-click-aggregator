import express from "express";

const app = express();
app.use(express.json());

const PORT = process.env.PORT ?? 3001;

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

// TODO: implement once the click event / Kafka topology is designed
app.post("/recordClick", (_req, res) => {
  res.status(501).json({ error: "not implemented" });
});

app.listen(PORT, () => {
  console.log(`record-click-service listening on port ${PORT}`);
});
