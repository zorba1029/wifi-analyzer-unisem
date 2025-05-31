package com.unisem.metrobus.analyzer.scheduler

import java.sql.{Connection, SQLException}
import java.time.{LocalDate, LocalDateTime}

import com.typesafe.config.ConfigFactory
import com.unisem.metrobus.analyzer.db.DBDataSource
import org.slf4j.LoggerFactory


object DeviceTablePartitionManager {
	def apply() = {
		new DeviceTablePartitionManager()
	}
}


class DeviceTablePartitionManager extends PartitionManager {
	private val logger = LoggerFactory.getLogger(classOf[DeviceTablePartitionManager])
	val appConfig = ConfigFactory.load()
	val DB_NAME = appConfig.getString("analyzer.DB.db_name")
	val TABLE_NAME = "global_dev_tbl"


	def process() = {
		logger.info(s"DeviceTablePartitionManager ---> process() ------------------------- ")
		val currentDateTime = LocalDateTime.now
		//-----------------------------------------------
		// 2018/4/25, ADDED timezone_delta hour: UTC + 3
		//-- adjust current DateTime: Turkey Metro Bus
		val adjustedDT = currentDateTime.plusHours(appConfig.getInt("analyzer.timezone_delta"))

		//-- create a partition for TODAY
		processTablePartition(adjustedDT.toLocalDate)

		//-- create a partition for TOMORROW
		processTablePartition(adjustedDT.toLocalDate.plusDays(1))

		//-- delete partitions that are 3 and 4 days ago
		deletePartitions(DB_NAME, TABLE_NAME, adjustedDT.toLocalDate)
	}


	private def processTablePartition(adjustedDT: LocalDate): Unit = {
		val partName = getPartitionNameForDate(adjustedDT)
		val partCond = getPartitionConditionForDate(adjustedDT.plusDays(1))

		//-- check if the Table exist & create Table
		if (isTableExist != true) {
			if (createTable(adjustedDT)) {
				logger.debug("processTablePartition() - global_dev_tbl TABLE CREATED.")
			}
		} else {
			logger.debug("processTablePartition() - global_dev_tbl TABLE ALREADY EXIST.")
		}

		if (isTableExist) {
			//-- check if partition exist for the <adjustedDT> date
			if (hasASpecificPartition(partName) != true) {
				//-- CREATE a new Partition for today
				val exist = hasPartitionForADate(adjustedDT)
				if (exist != true) {
					val success = addPartition(DB_NAME, TABLE_NAME, partName, partCond)
					if (success) {
						logger.debug("processTablePartition() - global_dev_tbl PARTITION ADDED: " + partName)
					}
					else {
						logger.error(s"processTablePartition() - ^^^^^^^^^^^^^^ [$partName] ^^^^^^^^^^^^^^^^")
						logger.error("processTablePartition() -  1) global_dev_tbl  Partition - NOT CREATED. Exit...")
						logger.error("processTablePartition() - ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
						System.exit(-1)
					}
				}
				else {
					logger.warn(s"processTablePartition() - Partition: [$partName] ALREADY EXIST")
				}
			}
			else {
				//-- ALREADY CREATED: SKIP partition creation
				logger.warn(s"processTablePartition() - Partition: [$partName] ALREADY EXIST")
			}
		}
		else {
			logger.error(s"processTablePartition() - ^^^^^^^^^^^^^^^^^^^ [$partName] ^^^^^^^^^^^^^^^^^")
			logger.error("processTablePartition() -  2) global_dev_tbl NOT CREATED. Exit...")
			logger.error("processTablePartition() - ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
			System.exit(-1)
		}
	}


	private def deletePartitions(dbName: String, tblName: String, startDT: LocalDate) = { //---------------------------------------------------------
		// delete PARTITIONS - 3, 4 days past partitions
		//-----------------------------------
		val three_days_ago = startDT.minusDays(3)
		val three_days_ago_PartitionName = getPartitionNameForDate(three_days_ago)
		val four_days_ago = startDT.minusDays(4)
		val four_days_ago_PartitionName = getPartitionNameForDate(four_days_ago)

		var del_four_days_ago = false

		if (hasASpecificPartition(four_days_ago_PartitionName)) {
			del_four_days_ago = deletePartition(dbName, tblName, four_days_ago_PartitionName)
			logger.debug(s"deletePartitions() - global_dev_tbl - PARTITION ** DELETED **, pName = $four_days_ago_PartitionName")
		}
		else {
			logger.debug(s"deletePartitions() - global_dev_tbl - PARTITION ** NOT ** EXIST, pName = $four_days_ago_PartitionName")
		}

		var del_three_days_ago = false

		if (hasASpecificPartition(three_days_ago_PartitionName)) {
			del_three_days_ago = deletePartition(dbName, tblName, three_days_ago_PartitionName)
			logger.debug(s"deletePartitions() - global_dev_tbl - PARTITION ** DELETED **, pName = $three_days_ago_PartitionName")
		}
		else {
			logger.debug(s"deletePartitions() - global_dev_tbl - PARTITION ** NOT ** EXIST, pName = $three_days_ago_PartitionName")
		}
		del_four_days_ago && del_three_days_ago
	}


	private def doExecOperation(qry: String): Boolean = {
		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(qry)
			preparedStmt.execute
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: ${sqlEx.getMessage}")
				return false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: ${ex.getMessage}")
				return false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		true
	}


	//-----------------------------------------------------------------------
	//	DROP TABLE IF EXISTS `global_dev_tbl`;
	// ---------
	//	CREATE TABLE IF NOT EXISTS global_dev_tbl (
	//    `dev_id`         varchar(32) NOT NULL,
	//    `start_date`     date NOT NULL,
	//		`last_datetime`  datetime NOT NULL,
	//		`station_id`     varchar(32) NOT NULL,
	//
	//	   PRIMARY KEY (`dev_id`, `start_date`)
	// ) ENGINE=TokuDB AUTO_INCREMENT=1 DEFAULT CHARSET='utf8';
	//---------------------------------------------------------
	private def isTableExist = {
		val selectQry = s""" SELECT count(TABLE_NAME)
			                 | FROM  information_schema.tables
			                 | WHERE TABLE_SCHEMA = '${DB_NAME}'
			                 |       AND TABLE_NAME = '${TABLE_NAME}' LIMIT 1
											 | ;
										""".stripMargin
		//	+-------------------+
		//	| count(TABLE_NAME) |
		//	+-------------------+
		//	|                 1 |
		//	+-------------------+

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery
			var count = -1

			if (rs.next) {
				count = rs.getInt(1)
			}

			if (count == 1) {
				true
			} else {
				false
			}
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: ${sqlEx.getMessage}")
				false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: ${ex.getMessage}")
				false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}


	private def createTable(startDT: LocalDate) = {
		val oneday_ago = startDT.minusDays(1)
		val oneday_ago_PartitionName = getPartitionNameForDate(oneday_ago)
		val oneday_ago_PartCond = getPartitionConditionForDate(oneday_ago.plusDays(1))

		val twoDaysAgo = startDT.minusDays(2)
		val twoDaysAgoPartitionName = getPartitionNameForDate(twoDaysAgo)
		val twoDaysAgoPartCond = getPartitionConditionForDate(twoDaysAgo.plusDays(1))

		val createQuery = s""" CREATE TABLE IF NOT EXISTS `${TABLE_NAME}` (
			                   | dev_id                VARCHAR(40)  NOT NULL COMMENT  'Device ID',
			                   | start_date            DATE         NOT NULL COMMENT  'Start Date',
			                   | last_datetime         DATETIME     NOT NULL COMMENT  'Last DateTime',
			                   | station_id            VARCHAR(32)  NOT NULL,
			                   | PRIMARY KEY (`dev_id`,`start_date`)
			                   | ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET='utf8'
			                   | COMMENT='Active Devices Table'
			                   | PARTITION BY RANGE COLUMNS (start_date) (
			                   | PARTITION ${twoDaysAgoPartitionName} VALUES LESS THAN ('${twoDaysAgoPartCond}'),
			                   | PARTITION ${oneday_ago_PartitionName} VALUES LESS THAN ('${oneday_ago_PartCond}')
			                   | ) ;
												""".stripMargin

		logger.info(s"[DB] Create Table - global_dev_tbl : date  = $startDT")
		logger.info(s"[DB] Create Table - global_dev_tbl : pName = $twoDaysAgoPartitionName, pCond = $twoDaysAgoPartCond")
		logger.info(s"[DB] Create Table - global_dev_tbl : pName = $oneday_ago_PartitionName, pCond = $oneday_ago_PartCond")
		doExecOperation(createQuery)
	}


	private def hasASpecificPartition(pName: String): Boolean = {
		val selectQry = s""" SELECT PARTITION_NAME
			                 | FROM  INFORMATION_SCHEMA.PARTITIONS
			                 | WHERE TABLE_SCHEMA = '${DB_NAME}'
			                 |       AND TABLE_NAME = '${TABLE_NAME}'
			                 |       AND PARTITION_NAME = '${pName}'
			                 | ;
			                 """.stripMargin
		//-- ex) -------------------------------------------------------
		// 		SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS
		// 		WHERE TABLE_SCHEMA = 'metro_istanbul'
		// 				AND TABLE_NAME = 'global_dev_tbl'
		// 				AND PARTITION_NAME = 'p2017_12_07';
		//		+----------------+
		//		| PARTITION_NAME |
		//		+----------------+
		//		| p2017_12_07    |
		//		+----------------+
		// 	+----------------+----------------+
		// 	| PARTITION_NAME | TABLE_NAME     |
		// 	+----------------+----------------+
		// 	| p2018_03_31    | global_dev_tbl |
		// 	| p2018_04_01    | global_dev_tbl |
		// 	| p2018_04_02    | global_dev_tbl |
		// 	| p2018_04_03    | global_dev_tbl |
		// 	+----------------+----------------+
		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery

			if (rs.next) {
				val partitionName = rs.getString("PARTITION_NAME")
				if (partitionName != null) {
					return true
				}
				else {
					return false
				}
			}
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: ${sqlEx.getMessage}")
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: ${ex.getMessage}")
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		false
	}


	private def hasPartitionForADate(dt: LocalDate) = {
		val pName = getPartitionNameForDate(dt)
		hasASpecificPartition(pName)
	}


	def getPartitionNameForDate(adjustedDT: LocalDate): String = {
		"p%04d_%02d_%02d".format(adjustedDT.getYear, adjustedDT.getMonth.getValue, adjustedDT.getDayOfMonth)
	}


	private def getPartitionConditionForDate(adjustedDT: LocalDate) = {
		"%04d-%02d-%02d".format(adjustedDT.getYear, adjustedDT.getMonth.getValue, adjustedDT.getDayOfMonth)
	}


	//-- Add a Partition
	private def addPartition(dbName: String, tblName: String, pName: String, pCond: String) = {
		val query = s""" ALTER TABLE ${dbName}.${tblName}
			             | ADD PARTITION (PARTITION ${pName} VALUES LESS THAN ('${pCond}'))
			             | ;
								""".stripMargin

		logger.info(s"[DB] Add partition : $query")

		doExecOperation(query)
	}


	//-- Delete a Partition
	private def deletePartition(dbName: String, tblName: String, pName: String) = {
		val query = s""" ALTER TABLE ${dbName}.${tblName}
			             | DROP PARTITION ${pName}
									 | ;
								""".stripMargin

		logger.info(s"[DB] Delete partition : $query")

		doExecOperation(query)
	}
}

