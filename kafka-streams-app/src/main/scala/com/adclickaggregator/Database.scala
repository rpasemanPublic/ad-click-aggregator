package com.adclickaggregator

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

object Database {
  private val config = new HikariConfig()
  config.setJdbcUrl(
    sys.env.getOrElse("DB_URL", "jdbc:postgresql://localhost:5432/ad_click_aggregator"),
  )
  config.setUsername(sys.env.getOrElse("DB_USER", "postgres"))
  config.setPassword(sys.env.getOrElse("DB_PASSWORD", "postgres"))

  val dataSource = new HikariDataSource(config)
}
