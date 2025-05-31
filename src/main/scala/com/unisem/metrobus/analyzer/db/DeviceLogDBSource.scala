package com.unisem.metrobus.analyzer.db

import java.sql.{Connection, SQLException}

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}


object DeviceLogDBSource {
	private val appConf = ConfigFactory.load()
	private val dbConfig = appConf.getConfig("DeviceLogDB")
	private val JDBConnInfo = dbConfig.getString("db.url")
	private val dataSourceClassName = dbConfig.getString("db.dataSourceClass")
	private val userId = dbConfig.getString("db.user")
	private val password = dbConfig.getString("db.password")
	private val initialSize = 5
	private val maxPoolSize = 20

	private val config = new HikariConfig
	private var ds: HikariDataSource = _


	//#---------------------------------------
	//# DB config
	//#----------------
	def apply() = {
		config.setPoolName("DeviceLog-DB-Pool")
		config.setJdbcUrl(JDBConnInfo)
		config.setDataSourceClassName(dataSourceClassName)
		config.setMinimumIdle(initialSize)
		config.setMaximumPoolSize(maxPoolSize)
		config.addDataSourceProperty("url", JDBConnInfo)
		config.addDataSourceProperty("user", userId)
		config.addDataSourceProperty("password", password)

		ds = new HikariDataSource(config)
	}


	@throws[SQLException]
	def getConnection(actorId: Int): Connection = {
		if (ds == null) {
			config.setPoolName("DeviceLog-DB-Pool")
			config.setJdbcUrl(JDBConnInfo)
			config.setDataSourceClassName(dataSourceClassName)
			config.setMinimumIdle(initialSize)
			config.setMaximumPoolSize(maxPoolSize)
			config.addDataSourceProperty("url", JDBConnInfo)
			config.addDataSourceProperty("user", userId)
			config.addDataSourceProperty("password", password)

			ds = new HikariDataSource(config)
		}
		ds.getConnection
	}

	def close(): Unit = {
		if (ds != null && !ds.isClosed) {
			ds.close()
		}
	}
}
