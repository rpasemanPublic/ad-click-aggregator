package com.adclickaggregator

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream.{
  Aggregator,
  Branched,
  Consumed,
  GlobalKTable,
  Grouped,
  Initializer,
  KStream,
  Materialized,
  Named,
  TimeWindows,
}
import org.apache.kafka.streams.processor.api.{
  FixedKeyProcessor,
  FixedKeyProcessorContext,
  FixedKeyProcessorSupplier,
  FixedKeyRecord,
}
import org.apache.kafka.streams.state.{KeyValueStore, WindowStore}
import org.slf4j.LoggerFactory

import java.time.Duration
import scala.jdk.OptionConverters.RichOptional
import scala.util.Random

final case class TopologyBuilder private (
    clickCountWriter: Option[ClickCountWriter] = None,
    hotAdShardCount: Option[Int] = None,
) {
  private val logger = LoggerFactory.getLogger(getClass)

  def withClickCountWriter(writer: ClickCountWriter): TopologyBuilder =
    copy(clickCountWriter = Some(writer))

  def withHotAdShardCount(n: Int): TopologyBuilder =
    copy(hotAdShardCount = Some(n))

  def build(): Topology = {
    val writer =
      clickCountWriter.getOrElse(throw new IllegalStateException("ClickCountWriter is required"))

    val builder = new StreamsBuilder()

    val clicks: KStream[String, ClickEvent] = builder
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

    val hotAds: GlobalKTable[String, String] =
      builder.globalTable(
        "adclickaggregator.public.hot_ads",
        Consumed.`with`(Serdes.String(), Serdes.String()),
      )

    val processor: FixedKeyProcessorSupplier[String, ClickEvent, ClickEventWithOffset] = () =>
      new FixedKeyProcessor[String, ClickEvent, ClickEventWithOffset] {
        private var context: FixedKeyProcessorContext[String, ClickEventWithOffset] = _

        override def init(context: FixedKeyProcessorContext[String, ClickEventWithOffset]): Unit =
          this.context = context

        override def process(record: FixedKeyRecord[String, ClickEvent]): Unit = {
          val offset = context.recordMetadata().toScala.map(_.offset()).getOrElse(-1L)
          context.forward(record.withValue(ClickEventWithOffset(record.value(), offset)))
        }
      }

    val clicksWithOffset: KStream[String, ClickEventWithOffset] = clicks.processValues(processor)
    val clicksWithHotFlag: KStream[String, (ClickEventWithOffset, Boolean)] =
      clicksWithOffset.leftJoin(
        hotAds,
        (adId: String, _: ClickEventWithOffset) => adId,
        (event: ClickEventWithOffset, hotMarker: String) => (event, hotMarker != null),
      )

    val branchedClicksWithOffset = clicksWithHotFlag
      .split(Named.as("hot-split-"))
      .branch((_, c) => c._2, Branched.as("hot"))
      .defaultBranch(Branched.as("cold"))

    val initializer: Initializer[ClickCountAggregate] = () =>
      ClickCountAggregate(count = 0, maxTimestamp = 0, lastOffset = -1)

    val aggregator: Aggregator[String, ClickEventWithOffset, ClickCountAggregate] =
      (_, eventWithOffset, agg) =>
        ClickCountAggregate(
          count = agg.count + 1,
          maxTimestamp = math.max(agg.maxTimestamp, eventWithOffset.event.timestamp),
          lastOffset = eventWithOffset.offset,
        )

    val materializedClickCountAggregate
        : Materialized[String, ClickCountAggregate, WindowStore[Bytes, Array[Byte]]] =
      Materialized.`with`[String, ClickCountAggregate, WindowStore[Bytes, Array[Byte]]](
        Serdes.String(),
        ClickCountAggregate.serde,
      )

    val hotClickCountAggregates =
      branchedClicksWithOffset
        .get("hot-split-hot")
        .mapValues((_, v) => v._1)
        .selectKey((k, _) =>
          s"${k}#${Random.nextInt(hotAdShardCount.getOrElse(throw new IllegalStateException("hotAdShardCount must be set")))}",
        )
        .groupByKey(Grouped.`with`(Serdes.String(), ClickEventWithOffset.serde))
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .aggregate(
          initializer,
          aggregator,
          Named.as("click-count-aggregate-hot"),
          materializedClickCountAggregate,
        )

    val coldClickCountAggregates =
      branchedClicksWithOffset
        .get("hot-split-cold")
        .mapValues((_, v) => v._1)
        .groupByKey()
        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        .aggregate(
          initializer,
          aggregator,
          Named.as("click-count-aggregate-cold"),
          materializedClickCountAggregate,
        )

    val mergeInitializer: Initializer[MergedClickCountAggregate] = () =>
      MergedClickCountAggregate(shardAggregates = Map.empty)

    val mergeAggregator: Aggregator[String, PartialWithShard, MergedClickCountAggregate] =
      (_, partial, acc) =>
        acc.copy(shardAggregates = acc.shardAggregates.updated(partial.shardKey, partial.aggregate))

    val hotCounts = hotClickCountAggregates
      .toStream()
      .map { (windowedKey, agg) =>
        val shardKey  = windowedKey.key() // e.g. "ad-001#3" or "ad-002"
        val plainAdId = shardKey.takeWhile(_ != '#')
        val newKey    = s"$plainAdId|${windowedKey.window().start()}"
        KeyValue.pair(newKey, PartialWithShard(shardKey, agg))
      }
      .groupByKey(Grouped.`with`(Serdes.String(), PartialWithShard.serde))
      .aggregate(
        mergeInitializer,
        mergeAggregator,
        Materialized.`with`[String, MergedClickCountAggregate, KeyValueStore[Bytes, Array[Byte]]](
          Serdes.String(),
          MergedClickCountAggregate.serde,
        ),
      )

    val coldCounts: KStream[String, MergedClickCountAggregate] = coldClickCountAggregates
      .toStream()
      .map { (windowedKey, agg) =>
        val adId   = windowedKey.key()
        val newKey = s"$adId|${windowedKey.window().start()}"
        KeyValue.pair(newKey, MergedClickCountAggregate(shardAggregates = Map(adId -> agg)))
      }

    val mergedCounts = hotCounts.toStream().merge(coldCounts)

    mergedCounts.foreach { (compositeKey, merged) =>
      val Array(adId, windowStartStr) = compositeKey.split("\\|", 2)
      val windowStart                 = windowStartStr.toLong
      val shardValues                 = merged.shardAggregates.values
      writer.write(
        adId = adId,
        windowStart = windowStart,
        count = shardValues.map(_.count).sum,
        maxTimestamp = shardValues.map(_.maxTimestamp).max,
        shardOffsets = merged.shardAggregates.view.mapValues(_.lastOffset).toMap,
      )
    }

    builder.build()
  }
}

object TopologyBuilder {
  def apply(): TopologyBuilder = new TopologyBuilder()
}
