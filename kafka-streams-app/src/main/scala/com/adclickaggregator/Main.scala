package com.adclickaggregator

import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}
import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]): Unit = {

    val kafkaBrokerEndpoint = sys.env.getOrElse("KAFKA_BROKER", "localhost:9092")

    val partitionCount = configureTopicsAndGetPartitionCount(kafkaBrokerEndpoint)

    val streamProps = new Properties()

    streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "ad-click-aggregator")
    streamProps.put(
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
      kafkaBrokerEndpoint,
    )
    streamProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "2000")

    val topology = TopologyBuilder()
      .withClickCountWriter(new PostgresClickCountWriter(Database.dataSource))
      .withHotAdShardCount(4 * partitionCount)
      .build()
    val streams = new KafkaStreams(topology, streamProps)
    streams.start()
    sys.addShutdownHook {
      streams.close()
    }
  }

  private def configureTopicsAndGetPartitionCount(kafkaBrokerEndpoint: String): Int = {
    // We first try to create the ad-clicks topic first
    val adminProps = new Properties()
    adminProps.put(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      kafkaBrokerEndpoint,
    )

    val admin = Admin.create(adminProps)
    // TODO: partitions and replication factor should be different between production and development
    val topic = new NewTopic("ad-clicks", 4, 1.toShort)

    Try(admin.createTopics(Collections.singletonList(topic)).all().get()) match {
      case Success(_)                       => ()
      case Failure(_: TopicExistsException) => ()
      case Failure(e: ExecutionException) if e.getCause.isInstanceOf[TopicExistsException] =>
        ()
      case Failure(e) => throw e
    }

    val partitionCount = admin
      .describeTopics(Collections.singletonList("ad-clicks"))
      .allTopicNames()
      .get()
      .get("ad-clicks")
      .partitions()
      .size()
    admin.close()
    partitionCount
  }
}
