package com.adclickaggregator

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.{Consumed, KStream}

object TopologyBuilder {
  def build(): Topology = {
    val builder = new StreamsBuilder()

    val clicks: KStream[String, ClickEvent] = builder.stream(
      "ad-clicks",
      Consumed.`with`(Serdes.String(), ClickEvent.clickEventSerde),
    )

    builder.build()
  }
}
