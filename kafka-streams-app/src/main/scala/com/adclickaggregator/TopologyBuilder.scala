package com.adclickaggregator

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Materialized, TimeWindows}
import org.apache.kafka.streams.state.WindowStore
import org.slf4j.LoggerFactory

import java.time.Duration

final case class TopologyBuilder private (clickCountWriter: Option[ClickCountWriter] = None) {
  private val logger = LoggerFactory.getLogger(getClass)

  def withClickCountWriter(writer: ClickCountWriter): TopologyBuilder =
    copy(clickCountWriter = Some(writer))

  def build(): Topology = {
    val writer =
      clickCountWriter.getOrElse(throw new IllegalStateException("ClickCountWriter is required"))

    val builder = new StreamsBuilder()

    val clicks = builder
      .stream(
        "ad-clicks",
        Consumed.`with`(Serdes.String(), ClickEvent.clickEventSerde),
      )
      .peek { (key, event) =>
        if (key != event.adId) {
          logger.error(s"Key/value mismatch on ad-clicks: key=$key, event.adId=${event.adId}")
        }
        logger.info(
          s"""{"requestId":"${event.requestId}","adId":"${event.adId}","event":"consumed","timestamp":${System
              .currentTimeMillis()}}""",
        )
      }
      .filter { (key, event) => key == event.adId }

    val clickCountAggregates = clicks.groupByKey
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
      .aggregate(
        () => ClickCountAggregate(count = 0, maxTimestamp = 0),
        (_, event: ClickEvent, agg: ClickCountAggregate) =>
          ClickCountAggregate(
            count = agg.count + 1,
            maxTimestamp = math.max(agg.maxTimestamp, event.timestamp),
          ),
        Materialized.`with`[String, ClickCountAggregate, WindowStore[Bytes, Array[Byte]]](
          Serdes.String(),
          ClickCountAggregate.serde,
        ),
      )

    clickCountAggregates.toStream().foreach { (windowedKey, clickAgg) =>
      val adId        = windowedKey.key()
      val windowStart = windowedKey.window().start() // epoch millis
      writer.write(
        adId = adId,
        windowStart = windowStart,
        count = clickAgg.count,
        maxTimestamp = clickAgg.maxTimestamp,
      )
    }

    builder.build()
  }
}

object TopologyBuilder {
  def apply(): TopologyBuilder = new TopologyBuilder()
}
