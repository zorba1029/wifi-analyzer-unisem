package com.unisem.metrobus.analyzer

import java.io.{File, RandomAccessFile}
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}


object InstanceChecker {
	val logger: Logger = LoggerFactory.getLogger(InstanceChecker.getClass)
	def lockInstance(lockFile: String): Boolean = {
		try {
			val file = new File(lockFile)
			val randomAccessFile = new RandomAccessFile(file, "rw")
			val fileLock = randomAccessFile.getChannel.tryLock()

			if (fileLock != null) {
				Runtime.getRuntime.addShutdownHook(new Thread(() => {
					logger.warn("Shutdown: lockInstance fileLock...")
					try {
						fileLock.release()
						randomAccessFile.close()
						file.delete()
					} catch {
						case ex: Exception =>
							logger.error(s"Unable to remove lock file: $lockFile - $ex")
					}
				}))
				return true
			}
		} catch {
			case ex: Exception =>
				logger.error(s"Unable to create and/or lock file: $lockFile - $ex")
		}
		false
	}

	def printConfigs() = {
		val appConfig = ConfigFactory.load()
		//-- analyzer/consumer
		val consumerConf = appConfig.getConfig("analyzer.consumer")
		val consumerTopic = consumerConf.getString("analyzer_topic")
		val consumerGroupId = consumerConf.getString("analyzer_group")
		val consumerBrokerInfo = consumerConf.getString("analyzer_broker_info")
		val consumerCluserInfo = consumerConf.getString("analyzer_cluster_info")
		val consumerCount = consumerConf.getInt("consumer_count")
		val analyzerLockfile = consumerConf.getString("analyzer_lock_file")
		val deviceLogTopic = consumerConf.getString("device_log_topic")
		val deviceLogGroupId = consumerConf.getString("device_log_group")
		val devLogConsumerCount = consumerConf.getInt("device_logger_count")

//		val JDBCConnInfo = appConfig.getString("mariaDB.db.url")
//		val dataSourceClassName = appConfig.getString("mariaDB.db.dataSourceClass")
//		val dbName = appConfig.getString("mariaDB.db.databaseName")
//		val userId = appConfig.getString("mariaDB.db.user")
//		val maxConnections = appConfig.getString("mariaDB.maxConnections")
//		//		val numThreads = appConfig.getString("mariaDB.numThreads")

		//---  [Analyzer DB] -----
		val MysqlConnInfo = appConfig.getString("AnalyzerMysqlDB.db.url")
		val MysqlDataSourceClassName = appConfig.getString("AnalyzerMysqlDB.db.dataSourceClass")
		val MysqlDBName = appConfig.getString("AnalyzerMysqlDB.db.databaseName")
		val MysqlUserId = appConfig.getString("AnalyzerMysqlDB.db.user")
		val MySqlMaxConnections = appConfig.getString("AnalyzerMysqlDB.maxConnections")

		//--- [LogRecord DB] ----
		val JDBCConnInfo = appConfig.getString("DeviceLogDB.db.url")
		val dataSourceClassName = appConfig.getString("DeviceLogDB.db.dataSourceClass")
		val dbName = appConfig.getString("DeviceLogDB.db.databaseName")
		val userId = appConfig.getString("DeviceLogDB.db.user")
		val maxConnections = appConfig.getString("DeviceLogDB.maxConnections")
		//		val numThreads = appConfig.getString("mariaDB.numThreads")


		logger.info("===========================================================")

		logger.info("---- [Analyzer Topic] -------------------------------------")
		logger.info("analyzer_topic         = " + consumerTopic)
		logger.info("analyzer_group_id      = " + consumerGroupId)
		logger.info("analyzer_broker_info   = " + consumerBrokerInfo)
		logger.info("analyzer_cluster_info  = " + consumerCluserInfo)
		logger.info("analyzer_lock_file     = " + analyzerLockfile)
		logger.info("consumer_count         = " + consumerCount)

		logger.info("---- [DeviceLog Topic ]  ----------------------------------")
		logger.info("deviceLogTopic         = " + deviceLogTopic)
		logger.info("deviceLogGroupId       = " + deviceLogGroupId)
		logger.info("devLogConsumerCount    = " + devLogConsumerCount)

		logger.info("---- [Analyzer DB] ----------------------------------------")
		logger.info(" jdbcURL               = " + MysqlConnInfo)
		logger.info(" dataSourceClass       = " + MysqlDataSourceClassName)
		logger.info(" db_name               = " + MysqlDBName)
		logger.info(" userId                = " + MysqlUserId)
		logger.info(" db_max_pool           = " + MySqlMaxConnections)
		logger.info("---- [LogRecord DB] ---------------------------------------")
		logger.info(" jdbcURL               = " + JDBCConnInfo)
		logger.info(" dataSourceClass       = " + dataSourceClassName)
		logger.info(" db_name               = " + dbName)
		logger.info(" userId                = " + userId)
		logger.info(" db_max_pool           = " + maxConnections)
		logger.info("===========================================================")
	}
}
