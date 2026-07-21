package com.adclickaggregator

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import org.apache.kafka.common.serialization.Serde

case class ClickEvent(
    adId: String,
    requestId: String,
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

case class ClickEventWithOffset(event: ClickEvent, offset: Long)
object ClickEventWithOffset {
  implicit val decoder: Decoder[ClickEventWithOffset] = deriveDecoder[ClickEventWithOffset]
  implicit val encoder: Encoder[ClickEventWithOffset] = deriveEncoder[ClickEventWithOffset]
  implicit val serde: Serde[ClickEventWithOffset]     = JsonSerde[ClickEventWithOffset]
}

case class ClickCountAggregate(count: Long, maxTimestamp: Long, lastOffset: Long)
object ClickCountAggregate {
  implicit val decoder: Decoder[ClickCountAggregate] = deriveDecoder[ClickCountAggregate]
  implicit val encoder: Encoder[ClickCountAggregate] = deriveEncoder[ClickCountAggregate]
  implicit val serde: Serde[ClickCountAggregate]     = JsonSerde[ClickCountAggregate]
}

case class MergedClickCountAggregate(shardAggregates: Map[String, ClickCountAggregate])
object MergedClickCountAggregate {
  implicit val decoder: Decoder[MergedClickCountAggregate] =
    deriveDecoder[MergedClickCountAggregate]
  implicit val encoder: Encoder[MergedClickCountAggregate] =
    deriveEncoder[MergedClickCountAggregate]
  implicit val serde: Serde[MergedClickCountAggregate] = JsonSerde[MergedClickCountAggregate]
}

case class PartialWithShard(shardKey: String, aggregate: ClickCountAggregate)
object PartialWithShard {
  implicit val decoder: Decoder[PartialWithShard] = deriveDecoder[PartialWithShard]
  implicit val encoder: Encoder[PartialWithShard] = deriveEncoder[PartialWithShard]
  implicit val serde: Serde[PartialWithShard]     = JsonSerde[PartialWithShard]
}
