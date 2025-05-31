/**
	* Copyright (C) 2017 Unisem Inc. <http://www.unisem.co.kr>
	*/
package com.unisem.metrobus.analyzer.cronjob

import org.slf4j.LoggerFactory
import java.sql.{Connection, SQLException}

import com.unisem.metrobus.analyzer.db.DBDataSource
import com.unisem.metrobus.analyzer.common.DBRecords._

import scala.collection.mutable.ListBuffer


object MonthlyStatsDBI {
	private val logger = LoggerFactory.getLogger(MonthlyStatsDBI.getClass)

	def createStationStatsMonthlyTable(): Boolean = {
		val createQry = """ CREATE TABLE IF NOT EXISTS `station_stats_monthly` (
			                | station_id    VARCHAR(32)  NOT NULL COMMENT  'Station ID',
			                | stat_date     DATE         NOT NULL COMMENT  'Visit Date',
			                | count         INT          NULL DEFAULT 0 COMMENT 'count',
			                | PRIMARY KEY (station_id, stat_date)
			                | ) ENGINE=InnoDB DEFAULT CHARSET='utf8'
			                |   COMMENT='Monthly Statistics of Station Passengers'
			                | ;
		                """.stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(createQry)
			preparedStmt.execute
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: createStationStatsMonthlyTable() - ${sqlEx.getMessage}")
				return false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: createStationStatsMonthlyTable() - ${ex.getMessage}")
				return false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		true
	}


	def createFlowsStatsMonthlyTable(): Boolean = {
		val createQry = """ CREATE TABLE IF NOT EXISTS `flows_stats_monthly` (
			                | src_station_id  VARCHAR(32)  NOT NULL COMMENT  'Source Station ID',
			                | dst_station_id  VARCHAR(32)  NOT NULL COMMENT  'Destination Station ID',
			                | stat_date       DATE         NOT NULL COMMENT  'Visit Date',
			                | count           INT          NULL DEFAULT 0 COMMENT 'count',
			                | PRIMARY KEY (src_station_id, dst_station_id, stat_date)
			                | ) ENGINE=InnoDB DEFAULT CHARSET='utf8'
			                |   COMMENT='Monthly Flows Statistics between Stations'
			                | ;
		                """.stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(createQry)
			preparedStmt.execute
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: createFlowsStatsMonthlyTable() - ${sqlEx.getMessage}")
				return false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: createFlowsStatsMonthlyTable() - ${ex.getMessage}")
				return false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		true
	}


	//===========================================================================
	//MariaDB [metro_istanbul]>
	//	 SELECT station_id, stat_date,
	//	  (SUM(hour_00)+SUM(hour_01)+SUM(hour_02)+SUM(hour_03)+SUM(hour_04)+
	//	   SUM(hour_05)+SUM(hour_06)+SUM(hour_07)+SUM(hour_08)+SUM(hour_09)+
	//	   SUM(hour_10)+SUM(hour_11)+SUM(hour_12)+SUM(hour_13)+SUM(hour_14)+
	//	   SUM(hour_15)+SUM(hour_16)+SUM(hour_17)+SUM(hour_18)+SUM(hour_19)+
	//	   SUM(hour_20)+SUM(hour_21)+SUM(hour_22)+SUM(hour_23)) as count
	//	 FROM station_stats_hourly
	//	 WHERE YEAR(stat_date) = 2018 AND MONTH(stat_date) = 1
	//	 GROUP BY station_id;
	//+------------+------------+-------+
	//| station_id | stat_date  | count |
	//+------------+------------+-------+
	//| station-1  | 2018-01-01 |    10 |
	//| station-2  | 2018-01-01 |     5 |
	//| station-3  | 2018-01-01 |     0 |
	//| station-4  | 2018-01-01 |     0 |
	//+------------+------------+-------+
	//4 rows in set (0.00 sec)
	def getStationStatsMonthly(year: Int, month: Int): List[StationStatsMonthlyRecord] = {
		val recordList = ListBuffer[StationStatsMonthlyRecord]()
		val selectQry = s""" SELECT station_id, stat_date,
			                 |     (SUM(hour_00)+SUM(hour_01)+SUM(hour_02)+SUM(hour_03)+SUM(hour_04)
			                 |     +SUM(hour_05)+SUM(hour_06)+SUM(hour_07)+SUM(hour_08)+SUM(hour_09)
			                 |     +SUM(hour_10)+SUM(hour_11)+SUM(hour_12)+SUM(hour_13)+SUM(hour_14)
			                 |     +SUM(hour_15)+SUM(hour_16)+SUM(hour_17)+SUM(hour_18)+SUM(hour_19)
			                 |     +SUM(hour_20)+SUM(hour_21)+SUM(hour_22)+SUM(hour_23)) as count
			                 | FROM  station_stats_hourly
			                 | WHERE YEAR(stat_date) = ${year} AND MONTH(stat_date) = ${month}
											 | GROUP BY station_id
			                 | ;
		""".stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery

			while (rs.next) {
				val station_id = rs.getString("station_id")
				val stat_date = rs.getString("stat_date")
				val total_count = rs.getLong("count")
				val record = StationStatsMonthlyRecord(station_id, stat_date, total_count)
				recordList += record
				logger.info("MonthlyJob:: Station Stats (%d, %d): |%s| |%s| |%d|".format(year, month, station_id, stat_date, total_count))
			}
			recordList.toList
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] SQLException: getStationStatsMonthly() - ${sqlEx.getMessage}")
				recordList.toList
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Throwable: getStationStatsMonthly() -  ${ex.getMessage}")
				recordList.toList
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}


	def saveStationStatsMonthly(stationStatsList: List[StationStatsMonthlyRecord], year: Int, month: Int): Unit = {
		val updateQry = s""" UPDATE station_stats_monthly
			                | SET  count = ?
			                | WHERE station_id = ?
			                |       AND YEAR(stat_date) = ${year}
			                |       AND MONTH(stat_date) = ${month}
			                | ;
		                """.stripMargin

		val insertQry = """ INSERT INTO station_stats_monthly
			                |            (station_id, stat_date, count)
			                | VALUES (?, ?, ?)
			                | ;
		                """.stripMargin

		//-- SELECT
		val existAlready = selectStationStatsMonthly(year, month)

		//-- UPDATE OR INSERT
		if (existAlready == true) {
			//-- UPDATE
			var conn: Connection = null
			try {
				conn = DBDataSource.getConnection(-1)
				val updateStmt = conn.prepareStatement(updateQry)
				conn.setAutoCommit(false)

				for (rec <- stationStatsList) {
					updateStmt.setLong(1, rec.total_count)   //--rec.getTotalCount)
					updateStmt.setString(2, rec.station_id)  //--rec.getStationId)
					updateStmt.executeUpdate
				}
				conn.setAutoCommit(true)
			} catch {
				case sqlEx: SQLException =>
					logger.error(s"ActorId=[-1] SQLException - saveStationStatsMonthly() : ${sqlEx.getMessage}")
				case ex: Throwable =>
					logger.error(s"ActorId=[-1]  Exception: saveStationStatsMonthly() - ${ex.getMessage}")
			} finally {
				if (conn != null) {
					conn.close()
				}
			}
		}
		else {
			//-- INSERT
			var conn: Connection = null
			try {
				conn = DBDataSource.getConnection(-1)
				val insertStmt = conn.prepareStatement(insertQry)
				conn.setAutoCommit(false)

				for (rec <- stationStatsList) {
					insertStmt.setString(1, rec.station_id)  //--rec.getStationId)
					insertStmt.setString(2, rec.stat_date)   //--rec.getStatDate)
					insertStmt.setLong(3, rec.total_count)   //--rec.getTotalCount)
					val rs = insertStmt.executeQuery
				}
				conn.setAutoCommit(true)
			} catch {
				case sqlEx: SQLException =>
					logger.error(s"ActorId=[-1] SQLException - saveStationStatsMonthly() : ${sqlEx.getMessage}")
				case ex: Throwable =>
					logger.error(s"ActorId=[-1]  Exception: saveStationStatsMonthly() - ${ex.getMessage}")
			} finally {
				if (conn != null) {
					conn.close()
				}
			}
		}
	}


	private def selectStationStatsMonthly(year: Int, month: Int) = {
		val selectQry = s""" SELECT station_id, stat_date
			                 | FROM  station_stats_monthly
			                 | WHERE YEAR(stat_date) = ${year}
			                 |       AND MONTH(stat_date) = ${month}
			                 | GROUP BY station_id
			                 | ;
		                """.stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery
			if (rs.next) {
				logger.info("MonthlyJob::selectStationStatsMonthly() - TRUE")
				true
			}
			else {
				logger.info("MonthlyJob::selectStationStatsMonthly() - FALSE")
				false
			}
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] : selectStationStatsMonthly(year: Int, month: Int) - ${sqlEx.getMessage}")
				logger.info("MonthlyJob::selectStationStatsMonthly() - FALSE")
				false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: selectStationStatsMonthly(year: Int, month: Int) - ${ex.getMessage}")
				false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}


	//	MariaDB [metro_istanbul]>
	// SELECT src_station_id, dst_station_id, stat_date,
	// FROM flows_stats_hourly
	// WHERE YEAR(stat_date) = 2017 AND MONTH(stat_date) = 12
	// GROUP BY src_station_id, dst_station_id;
	//	+----------------+----------------+------------+-------+
	//	| src_station_id | dst_station_id | stat_date  | count |
	//	+----------------+----------------+------------+-------+
	//	| station-1      | station-2      | 2017-12-29 |    23 |
	//	| station-1      | station-3      | 2017-12-29 |     9 |
	//	| station-1      | station-4      | 2017-12-29 |     0 |
	//	| station-2      | station-1      | 2017-12-29 |     5 |
	//	| station-2      | station-3      | 2017-12-29 |    30 |
	//	| station-2      | station-4      | 2017-12-29 |     0 |
	//	| station-3      | station-1      | 2017-12-29 |    33 |
	//	| station-3      | station-2      | 2017-12-29 |    13 |
	//	| station-3      | station-4      | 2017-12-29 |     0 |
	//	| station-4      | station-1      | 2017-12-29 |     0 |
	//	| station-4      | station-2      | 2017-12-29 |     0 |
	//	| station-4      | station-3      | 2017-12-29 |     0 |
	//	+----------------+----------------+------------+-------+
	//	12 rows in set (0.00 sec)
	def getFlowsStatsMonthly(year: Int, month: Int): List[FlowsStatsMonthlyRecord] = {
		val recordList = ListBuffer[FlowsStatsMonthlyRecord]()
		//		val selectQry = s""" SELECT src_station_id, dst_station_id, stat_date,
		//			                 |     (SUM(hour_00)+SUM(hour_01)+SUM(hour_02)+SUM(hour_03)+SUM(hour_04)
		//			                 |     +SUM(hour_05)+SUM(hour_06)+SUM(hour_07)+SUM(hour_08)+SUM(hour_09)
		//			                 |     +SUM(hour_10)+SUM(hour_11)+SUM(hour_12) +SUM(hour_13)+SUM(hour_14)
		//			                 |     +SUM(hour_15)+SUM(hour_16) +SUM(hour_17)+SUM(hour_18)+SUM(hour_19)
		//			                 |     +SUM(hour_20) +SUM(hour_21)+SUM(hour_22)+SUM(hour_23)) as count
		//			                 |  FROM flows_stats_hourly
		//			                 |  WHERE YEAR(stat_date) = ${year} AND MONTH(stat_date) = ${month}
		//			                 |  GROUP BY src_station_id, dst_station_id
		//			                 |  ;
		//										""".stripMargin
		//-- 2018/9/5, MODIFIED query
		val selectQry = s""" SELECT src_station_id, dst_station_id, stat_date,
			                 |     SUM(hour_00+hour_01+hour_02+hour_03+hour_04
			                 |     +hour_05+hour_06+hour_07+hour_08+hour_09
			                 |     +hour_10+hour_11+hour_12 +hour_13+hour_14
			                 |     +hour_15+hour_16 +hour_17+hour_18+hour_19
			                 |     +hour_20 +hour_21+hour_22+hour_23) as count
			                 |  FROM flows_stats_hourly
			                 |  WHERE YEAR(stat_date) = ${year} AND MONTH(stat_date) = ${month}
											 |  GROUP BY src_station_id, dst_station_id
			                 |  ;
										""".stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery

			while (rs.next) {
				val src_station_id = rs.getString("src_station_id")
				val dst_station_id = rs.getString("dst_station_id")
				val stat_date = rs.getString("stat_date")
				val total_count = rs.getLong("count")
				val record = FlowsStatsMonthlyRecord(src_station_id, dst_station_id, stat_date, total_count)
				recordList += record
				logger.info("MonthlyJob:: Flows Stats (%d, %d):   |%s| |%s| |%s| |%d|".format(year, month, src_station_id, dst_station_id, stat_date, total_count))
			}
			recordList.toList
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1] : getFlowsStatsMonthly() - ${sqlEx.getMessage}")
				recordList.toList
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: getFlowsStatsMonthly() - ${ex.getMessage}")
				recordList.toList
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}


	def saveFlowsStatsMonthly(flowsStatsList: List[FlowsStatsMonthlyRecord], year: Int, month: Int): Unit = {
		val updateQry = s""" UPDATE flows_stats_monthly
			                 | SET   count = ?
			                 | WHERE src_station_id = ? AND dst_station_id = ?
			                 |       AND YEAR(stat_date) = ${year}
			                 |       AND MONTH(stat_date) = ${month}
			                 | ;
			                 |""".stripMargin

		val insertQry = s""" INSERT INTO flows_stats_monthly
			                 |        (src_station_id, dst_station_id, stat_date, count)
			                 | VALUES (?, ?, ?, ?)
			                 | ;
			                 |""".stripMargin

		val existAlready = selectFlowsStatsMonthly(year, month)

		if (existAlready == true) {
			var conn: Connection = null
			try {
				conn = DBDataSource.getConnection(-1)
				val updateStmt = conn.prepareStatement(updateQry)
				conn.setAutoCommit(false)

				for (rec <- flowsStatsList) {
					updateStmt.setLong(1, rec.total_count)
					updateStmt.setString(2, rec.src_station_id)
					updateStmt.setString(3, rec.dst_station_id)
					updateStmt.executeUpdate
				}
				conn.setAutoCommit(true)
			} catch {
				case sqlEx: SQLException =>
					logger.error(s"ActorId=[-1] SQLException - saveFlowsStatsMonthly() : ${sqlEx.getMessage}")
				case ex: Throwable =>
					logger.error(s"ActorId=[-1]  Exception: saveFlowsStatsMonthly() - ${ex.getMessage}")
			} finally {
				if (conn != null) {
					conn.close()
				}
			}
		}
		else {
			var conn: Connection = null
			try {
				conn = DBDataSource.getConnection(-1)
				val insertStmt = conn.prepareStatement(insertQry)
				conn.setAutoCommit(false)

				for (rec <- flowsStatsList) {
					insertStmt.setString(1, rec.src_station_id)
					insertStmt.setString(2, rec.dst_station_id)
					insertStmt.setString(3, rec.stat_date)
					insertStmt.setLong(4, rec.total_count)
					insertStmt.executeQuery
				}
				conn.setAutoCommit(true)
			} catch {
				case sqlEx: SQLException =>
					logger.error(s"ActorId=[-1] SQLException: saveFlowsStatsMonthly() - ${sqlEx.getMessage}")
				case ex: Throwable =>
					logger.error(s"ActorId=[-1] Exception: saveFlowsStatsMonthly() - ${ex.getMessage}")
			} finally {
				if (conn != null) {
					conn.close()
				}
			}
		}
	}


	private def selectFlowsStatsMonthly(year: Int, month: Int) = {
		val selectQry = s""" SELECT src_station_id, dst_station_id, stat_date
			                 | FROM flows_stats_monthly
			                 | WHERE YEAR(stat_date) = ${year}
			                 |       AND MONTH(stat_date) = ${month}
			                 | GROUP BY src_station_id, dst_station_id
			                 | ;
										""".stripMargin

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val preparedStmt = conn.prepareStatement(selectQry)
			val rs = preparedStmt.executeQuery

			if (rs.next) {
				logger.info("MonthlyJob::selectFlowsStatsMonthly() - TRUE")
				true
			} else {
				logger.info("MonthlyJob::selectFlowsStatsMonthly() - FALSE")
				false
			}
		} catch {
			case sqlEx: SQLException =>
				logger.error(s"ActorId=[-1]  Exception: selectFlowsStatsMonthly() - ${sqlEx.getMessage}")
				false
			case ex: Throwable =>
				logger.error(s"ActorId=[-1]  Exception: selectFlowsStatsMonthly() - ${ex.getMessage}")
				false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}
}
