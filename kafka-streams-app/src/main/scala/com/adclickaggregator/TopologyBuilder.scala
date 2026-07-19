package com.adclickaggregator

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.{Consumed, KStream, TimeWindows}
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
      }
      .filter { (key, event) => key == event.adId }

    val counts = clicks.groupByKey
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
      .count()

    counts.toStream().foreach { (windowedKey, count) =>
      val adId        = windowedKey.key()
      val windowStart = windowedKey.window().start() // epoch millis
      writer.write(adId = adId, windowStart = windowStart, count = count)
    }

    builder.build()
  }
}

object TopologyBuilder {
  def apply(): TopologyBuilder = new TopologyBuilder()
}
