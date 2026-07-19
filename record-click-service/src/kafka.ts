import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "record-click-service",
  brokers: [process.env.KAFKA_BROKER ?? "localhost:9092"],
});

export const producer = kafka.producer();

export interface ClickEvent {
  adId: string;
  timestamp: number;
  userId: string | null;
  sessionId: string;
  ipAddress: string;
  userAgent: string;
  referrerUrl: string | null;
}

export async function sendClickEvent(clickEvent: ClickEvent): Promise<void> {
  await producer.send({
    topic: "ad-clicks",
    messages: [
      {
        key: clickEvent.adId,
        value: JSON.stringify(clickEvent),
      },
    ],
  });
}
