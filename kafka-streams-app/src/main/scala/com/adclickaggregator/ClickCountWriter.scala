package com.adclickaggregator

import org.slf4j.LoggerFactory

import javax.sql.DataSource
import scala.util.Using

trait ClickCountWriter {
  def write(adId: String, windowStart: Long, count: Long, maxTimestamp: Long): Unit
}

class PostgresClickCountWriter(dataSource: DataSource) extends ClickCountWriter {
  private val logger = LoggerFactory.getLogger(getClass)

  override def write(adId: String, windowStart: Long, count: Long, maxTimestamp: Long): Unit =
    Using
      .Manager { use =>
        val connection = use(dataSource.getConnection())
        val stmt = use(
          connection.prepareStatement(
            """INSERT INTO ad_click_counts_minute (ad_id, window_start, click_count)
            |VALUES (?, ?, ?)
            |ON CONFLICT (ad_id, window_start) DO UPDATE SET click_count = EXCLUDED.click_count""".stripMargin,
          ),
        )
        stmt.setString(1, adId)
        stmt.setTimestamp(2, new java.sql.Timestamp(windowStart))
        stmt.setLong(3, count)
        stmt.executeUpdate()
        val staleness = System.currentTimeMillis() - maxTimestamp
        logger.info(
          s"Wrote adId=$adId windowStart=$windowStart count=$count staleness=${staleness}ms",
        )
      }
      .recover { case e => logger.error(s"Failed to write click count for adId=$adId", e) }
}
