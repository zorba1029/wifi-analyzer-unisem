package com.unisem.metrobus.analyzer.db

import java.sql.{Connection, SQLException}

import com.typesafe.config.ConfigFactory
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}


object DBDataSource {
	private val appConf = ConfigFactory.load()
//	private val JDBConnInfo = appConf.getString("mariaDB.db.url")
//	private val dataSourceClassName = appConf.getString("mariaDB.db.dataSourceClass")
//	private val initialSize = 5
//	private val maxPoolSize = 20
//	private val userId = appConf.getString("mariaDB.db.user")
//	private val password = appConf.getString("mariaDB.db.password")
	private val dbConf = appConf.getConfig("AnalyzerMysqlDB")
	private val JDBConnInfo = dbConf.getString("db.url")
	private val dataSourceClassName = dbConf.getString("db.dataSourceClass")
	private val userId = dbConf.getString("db.user")
	private val password = dbConf.getString("db.password")
	private val initialSize = 5
	private val maxPoolSize = 20

	private val config = new HikariConfig
	private var ds: HikariDataSource = _


	//#---------------------------------------
	//# DB config
	//#----------------
	def apply() = {
		config.setPoolName("Analyzer-DB-Pool")
		config.setJdbcUrl(JDBConnInfo)
		config.setDataSourceClassName(dataSourceClassName)
		config.addDataSourceProperty("url", JDBConnInfo)
		config.addDataSourceProperty("user", userId)
		config.addDataSourceProperty("password", password)
		config.setMinimumIdle(initialSize)
		config.setMaximumPoolSize(maxPoolSize)

		ds = new HikariDataSource(config)
	}


	@throws[SQLException]
	def getConnection(actorId: Int): Connection = {
		if (ds == null) {
			config.setPoolName("Analyzer-DB-Pool")
			config.setJdbcUrl(JDBConnInfo)
			config.setDataSourceClassName(dataSourceClassName)
			config.addDataSourceProperty("url", JDBConnInfo)
			config.addDataSourceProperty("user", userId)
			config.addDataSourceProperty("password", password)
			config.setMinimumIdle(initialSize)
			config.setMaximumPoolSize(maxPoolSize)

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
