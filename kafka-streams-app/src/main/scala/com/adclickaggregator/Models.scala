package com.adclickaggregator

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.apache.kafka.common.serialization.Serde

case class ClickEvent(
    adId: String,
    timestamp: Long,
    userId: Option[String],
    sessionId: String,
    ipAddress: String,
    userAgent: String,
    referrerUrl: Option[String],
)

object ClickEvent {
  implicit val decoder: Decoder[ClickEvent]       = deriveDecoder[ClickEvent]
  implicit val encoder: Encoder[ClickEvent]       = deriveEncoder[ClickEvent]
  implicit val clickEventSerde: Serde[ClickEvent] = JsonSerde[ClickEvent]
}
