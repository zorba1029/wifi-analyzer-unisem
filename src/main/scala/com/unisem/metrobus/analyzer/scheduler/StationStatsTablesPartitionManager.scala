package com.unisem.metrobus.analyzer.scheduler

import java.sql.{Connection, SQLException}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory
import com.unisem.metrobus.analyzer.db.DBDataSource
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer


object StationStatsTablesPartitionManager {
	def apply() = {
		new StationStatsTablesPartitionManager()
	}
}


class StationStatsTablesPartitionManager extends PartitionManager {
	private val logger = LoggerFactory.getLogger(classOf[StationStatsTablesPartitionManager])
	private val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
	private val appConfig = ConfigFactory.load()
	private val DB_NAME = appConfig.getString("analyzer.DB.db_name")
	private val TABLE_NAME = "station_stats_hourly"


	def process(): Unit = {
		logger.info(s"StationStatsTablesPartitionManager ---> process() ------------------------- ")
		val currentDateTime = LocalDateTime.now
		//-----------------------------------------------
		// 2018/4/25, ADDED timezone_delta hour: UTC + 3
		//-- adjust current DateTime: Turkey Metro Bus
		val adjustedDT = currentDateTime.plusHours(appConfig.getInt("analyzer.timezone_delta")).toLocalDate

		//-- create a partition for TODAY
		processTablePartition(adjustedDT)

		//-- create a partition for TOMORROW
		processTablePartition(adjustedDT.plusDays(1))
	}


	private def processTablePartition(dt: LocalDate): Unit = {
		val partName: String = getPartitionNameForDate(dt)
		val partCond: String = getPartitionConditionForDate(dt.plusDays(1))

		//-- check if the Table exist & if not, create Table
		if (isTableExist() != true) {
			if (createTable(dt)) {
				logger.debug("processTablePartition() - station_stats_hourly TABLE CREATED.")
			}
		} else {
			logger.debug("processTablePartition() - station_stats_hourly TABLE ALREADY EXIST.")
		}

		if (isTableExist()) {
			//-- check if partition exist for today
			if (hasASpecificPartition(partName) != true) {
				//-- CREATE a new Partition for today
				val exist: Boolean = hasPartitionForADate(dt)
				if (exist != true) {
					val success: Boolean = addPartition(DB_NAME, TABLE_NAME, partName, partCond)
					if (success) {
						//-- init table data for a day (today)
						//-- 2018/9/6, Modified getStationIdList()
						//--val stationIdList: List[String] = getStationIdList()
						val stationIdList: List[(String,String)] = getStationIdList()
						initStationStatsTableForDate(dt, stationIdList)
					}
					else {
						logger.error(s"processTablePartition() - ^^^^^^^^^^^^^^ [$partName]  ^^^^^^^^^^^^^^")
						logger.error("processTablePartition() -  1) station_stats_hourly TABLE NOT CREATED. Exit...")
						logger.error("processTablePartition() - ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
						System.exit(-1)
					}
				}
				else {
					logger.debug(s"processTablePartition() - Partition: [$partName]  ALREADY EXIST - INIT TABLE RECORDS..")
					val stationIdList = getStationIdList()
					initStationStatsTableForDate(dt, stationIdList)
				}
			}
			else {
				//-- ALREADY CREATED: SKIP partition creation
				logger.debug(s"processTablePartition() - Partition: [$partName]  ALREADY EXIST")
				val stationIdList = getStationIdList()
				initStationStatsTableForDate(dt, stationIdList)
			}
		}
		else {
			logger.error(s"processTablePartition() - ^^^^^^^^^^^^^^ [$partName]  ^^^^^^^^^^^^^^")
			logger.error("processTablePartition() -  2) station_stats_hourly TABLE NOT CREATED. Exit...")
			logger.error("processTablePartition() - ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
			System.exit(-1)
		}
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


	//-- 2018/9/6, Commented Out
	//	private def getStationIdList(): List[String] = {
	//		val stationIdList = ListBuffer[String]()
	//				val selectQry = """ SELECT station_id
	//					                | FROM  station
	//					                | GROUP BY station_id
	//					                | ;
	//				                """.stripMargin
	//
	//		var conn: Connection = null
	//		try {
	//			conn = DBDataSource.getConnection(-1)
	//			val preparedStmt = conn.prepareStatement(selectQry)
	//			val rs = preparedStmt.executeQuery
	//
	//			while (rs.next) {
	//				val stationId = rs.getString("station_id")
	//				//				--logger.debug(s"ActorId=[-1]  getStationIdList, stationId = $stationId")
	//				stationIdList += stationId
	//			}
	//			stationIdList.toList
	//		} catch {
	//			case sqlEx: SQLException =>
	//				logger.error(s"ActorId=[-1] SQLException: ${sqlEx.getMessage}")
	//				stationIdList.toList
	//			case ex: Throwable =>
	//				logger.error(s"ActorId=[-1]  Exception: ${ex.getMessage}")
	//				stationIdList.toList
	//		} finally {
	//			if (conn != null) {
	//				conn.close()
	//			}
	//		}
	//	}

	//	mysql> select * from STATION;
	//	+-----------+-----------------------------+
	//	| StationId | StationName                 |
	//	+-----------+-----------------------------+
	//	|         1 | Sogutlucesme                |
	//	|         2 | Fikirtepe                   |
	//	|         3 | Uzuncayir                   |
	//	|         4 | Acibadem                    |
	//	|         5 | Altunizade                  |
	//	|         6 | Burhaniye                   |
	//	|         7 | 15-temmuz-sehitler-koprusu  |
	//	|         8 | Zincirlikuyu                |
	//	|         9 | Mecidiyekoy                 |
	//	|        10 | Caglayan                    |
	//	|        11 | Okmeydani-hastane           |
	//	|        12 | Darulaceze-perpa            |
	//	|        13 | Okmeydani                   |
	//	|        14 | Halicioglu                  |
	//	|        15 | Ayvansaray-eyup-sultan      |
	//	|        16 | Edirnekapi                  |
	//	|        17 | Bayrampasa-maltepe          |
	//	|        18 | Topkapi                     |
	//	|        19 | Cevizlibag                  |
	//	|        20 | Merter                      |
	//	|        21 | Zeytinburnu                 |
	//	|        22 | Incirli                     |
	//	|        23 | Bahcelievler                |
	//	|        24 | Sirinevler                  |
	//	|        25 | Yenibosna                   |
	//	|        26 | Sefakoy                     |
	//	|        27 | Besyol                      |
	//	|        28 | Florya                      |
	//	|        29 | Cennet-mahallesi            |
	//	|        30 | Kucukcekmece                |
	//	|        31 | B-sehir-bld-sosyal-tes      |
	//	|        32 | Sukrubey                    |
	//	|        33 | Avcilar-merkez-univ-kampusu |
	//	|        34 | Cihangir-univ-mahallesi     |
	//	|        35 | Mustafa-kemal-pasa          |
	//	|        36 | Saadetdere-mahallesi        |
	//	|        37 | Haramidere-sanayi           |
	//	|        38 | Haramidere                  |
	//	|        39 | Guzelyurt                   |
	//	|        40 | Beylikduzu                  |
	//	|        41 | Beylikduzu-belediyesi       |
	//	|        42 | Cumhuriyet-mahallesi        |
	//	|        43 | Hadimkoy                    |
	//	|        44 | Beylikduzu-sondurak         |
	//	+-----------+-----------------------------+
	private def getStationIdList(): List[(String,String)] = {
		val stationIdList = ListBuffer[(String,String)]()
		//		val selectQry = """ SELECT station_id
		//			                | FROM  station
		//			                | GROUP BY station_id
		//			                | ;
		//		                """.stripMargin
		//-- 2018/9/6, use STATION table instead of station table
		val selectQry = """ SELECT StationId, StationName
			                | FROM  STATION
			                | ;
		                """.stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery

			while (rs.next) {
				val stationId = rs.getString("StationId")
				val stationName = rs.getString("StationName")
//				--logger.debug(s"ActorId=[-1]  getStationIdList, stationId = $stationId")
				stationIdList += (stationId -> stationName)
			}
			stationIdList.toList
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: ${sqlEx.getMessage}")
				stationIdList.toList
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: ${ex.getMessage}")
				stationIdList.toList
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}


	private def initStationStatsTableForDate(day: LocalDate, stationIdList: List[(String,String)]): Boolean = {
		val pName = getPartitionNameForDate(day)
		val localDateStr = day.format(DATE_FORMATTER)

		//-- SELECT --------
		val selectQuery = s""" SELECT *
			                   | FROM  ${TABLE_NAME}  PARTITION (${pName})
			                   | WHERE station_id = ? AND stat_date = ?
			                   | ;
											""".stripMargin

		//-- INSERT --------
		val insertQry = s""" INSERT INTO ${TABLE_NAME}  PARTITION (${pName})
			                 |  (station_id, stat_date,
			                 |   hour_00, hour_01, hour_02, hour_03, hour_04,
			                 |   hour_05, hour_06, hour_07, hour_08, hour_09,
			                 |   hour_10, hour_11, hour_12, hour_13, hour_14,
			                 |   hour_15, hour_16, hour_17, hour_18, hour_19,
			                 |   hour_20, hour_21, hour_22, hour_23)
			                 | VALUES (?, ?,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0 )
			                 | ;
										""".stripMargin

		logger.debug(s"ActorId=[-1] INSERT INTO ${TABLE_NAME}, pName = $pName")

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val selectStmt = conn.prepareStatement(selectQuery)
			val insertStmt = conn.prepareStatement(insertQry)
			conn.setAutoCommit(false)

			for ((stationId, stationName) <- stationIdList) {
				//-- SELECT
				selectStmt.setString(1, stationId)
				selectStmt.setString(2, localDateStr)
				val rs = selectStmt.executeQuery

				if (rs.next != true) {
					//-- NOT EXIST -> INSERT
					logger.debug(s"ActorId=[-1] ${TABLE_NAME} - INSERT RECORD($stationId [$stationName],$localDateStr) - $day, Partition = $pName")
					insertStmt.setString(1, stationId)
					insertStmt.setString(2, localDateStr)
					insertStmt.executeUpdate
				}
				else {
					//-- ALREADY EXIST -> NO OP
					//--logger.debug(s"ActorId=[-1] ${TABLE_NAME} - NO_OP RECORD($stationId [$stationName], $localDateStr) - $day, Partition = $pName")
				}
			}
			conn.commit()
			logger.debug(s"ActorId=[-1] ${TABLE_NAME} - initialized for $day, Partition = $pName")
			return true
		} catch {
			case sqlEx: SQLException =>
				logger.warn(s"ActorId=[-1] *** SQLException *** : INSERT ${TABLE_NAME} - ${sqlEx.getMessage}")
				return false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: ${ex.getMessage}")
				return false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		false
	}


	private def isTableExist(): Boolean = {
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
				logger.error(s"ActorId=[-1] Exception: ${ex.getMessage}")
				false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}


	private def createTable(startDT: LocalDate): Boolean = {
		val oneday_ago = startDT.minusDays(1)
		val oneday_ago_PartitionName = getPartitionNameForDate(oneday_ago)
		val oneday_ago_PartCond = getPartitionConditionForDate(oneday_ago.plusDays(1))

		val twoDaysAgo = startDT.minusDays(2)
		val twoDaysAgoPartitionName = getPartitionNameForDate(twoDaysAgo)
		val twoDaysAgoPartCond = getPartitionConditionForDate(twoDaysAgo.plusDays(1))

		val createQry = s""" CREATE TABLE IF NOT EXISTS `${TABLE_NAME}` (
			                 | station_id            VARCHAR(32)  NOT NULL COMMENT  'Station ID',
			                 | stat_date             DATE         NOT NULL COMMENT  'Statistic Date',
			                 | hour_00               INT          NULL DEFAULT 0 COMMENT 'hour 00',
			                 | hour_01               INT          NULL DEFAULT 0 COMMENT 'hour 01',
			                 | hour_02               INT          NULL DEFAULT 0 COMMENT 'hour 02',
			                 | hour_03               INT          NULL DEFAULT 0 COMMENT 'hour 03',
			                 | hour_04               INT          NULL DEFAULT 0 COMMENT 'hour 04',
			                 | hour_05               INT          NULL DEFAULT 0 COMMENT 'hour 05',
			                 | hour_06               INT          NULL DEFAULT 0 COMMENT 'hour 06',
			                 | hour_07               INT          NULL DEFAULT 0 COMMENT 'hour 07',
			                 | hour_08               INT          NULL DEFAULT 0 COMMENT 'hour 08',
			                 | hour_09               INT          NULL DEFAULT 0 COMMENT 'hour 09',
			                 | hour_10               INT          NULL DEFAULT 0 COMMENT 'hour 10',
			                 | hour_11               INT          NULL DEFAULT 0 COMMENT 'hour 11',
			                 | hour_12               INT          NULL DEFAULT 0 COMMENT 'hour 12',
			                 | hour_13               INT          NULL DEFAULT 0 COMMENT 'hour 13',
			                 | hour_14               INT          NULL DEFAULT 0 COMMENT 'hour 14',
			                 | hour_15               INT          NULL DEFAULT 0 COMMENT 'hour 15',
			                 | hour_16               INT          NULL DEFAULT 0 COMMENT 'hour 16',
			                 | hour_17               INT          NULL DEFAULT 0 COMMENT 'hour 17',
			                 | hour_18               INT          NULL DEFAULT 0 COMMENT 'hour 18',
			                 | hour_19               INT          NULL DEFAULT 0 COMMENT 'hour 19',
			                 | hour_20               INT          NULL DEFAULT 0 COMMENT 'hour 20',
			                 | hour_21               INT          NULL DEFAULT 0 COMMENT 'hour 21',
			                 | hour_22               INT          NULL DEFAULT 0 COMMENT 'hour 22',
			                 | hour_23               INT          NULL DEFAULT 0 COMMENT 'hour 23',
			                 | PRIMARY KEY (station_id, stat_date)
			                 | ) ENGINE=InnoDB DEFAULT CHARSET='utf8'
			                 |   COMMENT='Hourly Statistics of Station Passengers'
			                 | PARTITION BY RANGE COLUMNS (stat_date) (
			                 |  PARTITION ${twoDaysAgoPartitionName} VALUES LESS THAN ('${twoDaysAgoPartCond}'),
			                 |  PARTITION ${oneday_ago_PartitionName} VALUES LESS THAN ('${oneday_ago_PartCond}')
			                 | ) ;
			                """.stripMargin

		logger.info(s"[DB] Create Table - ${TABLE_NAME} : date = $startDT")
		logger.info(s"[DB] Create Table - ${TABLE_NAME} : pName = $twoDaysAgoPartitionName, pCond = $twoDaysAgoPartCond")
		logger.info(s"[DB] Create Table - ${TABLE_NAME} : pName = $oneday_ago_PartitionName, pCond = $oneday_ago_PartCond")

		doExecOperation(createQry)
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
		// 				AND TABLE_NAME = 'station_stats_hourly'
		// 				AND PARTITION_NAME = 'p2017_12_07';
		//		+----------------+
		//		| PARTITION_NAME |
		//		+----------------+
		//		| p2017_12_07    |
		//		+----------------+

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery

			if (rs.next) {
				val partitionName = rs.getString("PARTITION_NAME")
				//-- logger.debug(s"ActorId=[-1] hasASpecificPartition: partitionName = $partitionName")
				if (partitionName != null) {
					return true
				} else {
					return false
				}
			}
			false
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


	private def hasPartitionForADate(dt: LocalDate) = {
		val pName = getPartitionNameForDate(dt)
		hasASpecificPartition(pName)
	}


	private def getPartitionNameForDate(dt: LocalDate) = {
		"p%04d_%02d_%02d".format(dt.getYear, dt.getMonth.getValue, dt.getDayOfMonth)
	}


	private def getPartitionConditionForDate(dt: LocalDate) = {
		"%04d-%02d-%02d".format(dt.getYear, dt.getMonth.getValue, dt.getDayOfMonth)
	}


	//-- Add a Partition
	private def addPartition(dbName: String, tblName: String, pName: String, pCond: String) = {
		val query = s""" ALTER TABLE ${dbName}.${tblName}
			             | ADD PARTITION (PARTITION ${pName}  VALUES LESS THAN ('${pCond}'))
			             | ;
								""".stripMargin

		logger.info(s"[DB] Add partition : $query")

		doExecOperation(query)
	}

}

