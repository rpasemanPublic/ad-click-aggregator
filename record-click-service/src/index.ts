import cookieParser from "cookie-parser";
import express from "express";
import { ClickEvent, producer, sendClickEvent } from "./kafka.js";
import { getDestinationUrl } from "./db.js";
import { randomUUID } from "crypto";

import type { Request, Response } from "express";

const app = express();
app.use(express.json());
app.use(cookieParser());

const PORT = process.env.PORT ?? 3001;
const SESSION_COOKIE_NAME = "sessionId";

interface RecordClickBody {
  adId: string;
}

app.get("/health", (_req, res) => {
  res.json({ status: "ok" });
});

app.post(
  "/recordClick",
  async (req: Request<{}, unknown, RecordClickBody>, res: Response) => {
    const { adId } = req.body;

    const destinationUrl = await getDestinationUrl(adId);
    if (destinationUrl === null) {
      res.status(404).json({ error: "ad not found" });
      return;
    }

    let sessionId = req.cookies[SESSION_COOKIE_NAME];
    if (!sessionId) {
      sessionId = randomUUID();
      res.cookie(SESSION_COOKIE_NAME, sessionId);
    }

    const clickEvent: ClickEvent = {
      adId,
      requestId: randomUUID(),
      timestamp: Date.now(),
      userId: null,
      sessionId,
      ipAddress: req.ip ?? "",
      userAgent: req.headers["user-agent"] ?? "",
      referrerUrl: req.headers.referer ?? null,
    };

    await sendClickEvent(clickEvent);

    res.json({ destinationUrl });
  },
);

async function main() {
  await producer.connect();
  app.listen(PORT, () => {
    console.log(`record-click-service listening on port ${PORT}`);
  });
}

main();
