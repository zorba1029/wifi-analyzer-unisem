package com.unisem.metrobus.analyzer

import java.io.IOException
import java.sql._
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Arrays, Properties}
import akka.actor.{Actor, ActorLogging}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.unisem.metrobus.analyzer.db.DBDataSource
import com.unisem.metrobus.analyzer.messages.Messages
import com.unisem.metrobus.analyzer.messages.Messages._
import io.circe.Error
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._


object ConsumerActor {
	val DATE_ONLY_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
	val DATE_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
	val TIME_DELTA_MINUTES: Int = 30 //-- 30 minutes

	var consumerActorIdCounter: Int = 0

	def getConsumerActorId() = {
		consumerActorIdCounter = consumerActorIdCounter + 1
		consumerActorIdCounter
	}

	private def getPartitionNameForDate(dt: LocalDate) = {
		"p%04d_%02d_%02d".format(dt.getYear, dt.getMonth.getValue, dt.getDayOfMonth)
	}
}


class ConsumerActor() extends Actor with ActorLogging {
	import ConsumerActor._

	implicit val system = context.system
	implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
	implicit val executor = context.dispatcher
	implicit val timeout = Timeout(30 seconds)

	//------------------------------------------------------
	//--- Kafka Consumer Setup
	//------------------------------------------------------
	val appConfig = ConfigFactory.load()

	val consumerConf: Config = appConfig.getConfig("analyzer.consumer")
	val consumerTopic: String = consumerConf.getString("analyzer_topic")
	val consumerGroupId: String = consumerConf.getString("analyzer_group")
	val consumerBrokerInfo: String = consumerConf.getString("analyzer_broker_info")
	val consumerClusterInfo: String = consumerConf.getString("analyzer_cluster_info")
	val consumerCount: Int = consumerConf.getInt("analyzer_count")

	val consumerProps: Properties = {
		val props = new Properties
		props.put("bootstrap.servers", consumerBrokerInfo)
		props.put("group.id", consumerGroupId)
		//--		props.put("zookeeper.connect", consumerClusterInfo)
		props.put("auto.commit.interval.ms", "1000")
		props.put("key.deserializer", classOf[StringDeserializer])
		props.put("value.deserializer", classOf[StringDeserializer])
		props
	}

	val consumer = new KafkaConsumer[String, String](consumerProps)
	val topicList: List[String] = List[String](consumerTopic)
	val consumerActorId: Int = getConsumerActorId()

	val columnMap = Map[Int,String](
		0 -> "hour_00",
		1 -> "hour_01",
		2 -> "hour_02",
		3 -> "hour_03",
		4 -> "hour_04",
		5 -> "hour_05",
		6 -> "hour_06",
		7 -> "hour_07",
		8 -> "hour_08",
		9 -> "hour_09",
		10 -> "hour_10",
		11 -> "hour_11",
		12 -> "hour_12",
		13 -> "hour_13",
		14 -> "hour_14",
		15 -> "hour_15",
		16 -> "hour_16",
		17 -> "hour_17",
		18 -> "hour_18",
		19 -> "hour_19",
		20 -> "hour_20",
		21 -> "hour_21",
		22 -> "hour_22",
		23 -> "hour_23",
	)

	log.info(s"ActorId[$consumerActorId], topicList=[$consumerTopic], consumerGroupId=[$consumerGroupId], clusterInfo=[$consumerClusterInfo]")


	override def preStart = {
		log.info(s"ActorId[$consumerActorId] - [*] ~~>> preStart() ")

		context.system.scheduler.scheduleOnce(5 seconds, self, Messages.StartConsumer)
	}


	override def postStop = {
		//log.info(s"ActorId=[$consumerActorId] - [*] ~~>> postStop() ")
		consumer.wakeup()
		consumer.unsubscribe()
		consumer.close()

		log.info(s"ActorId[$consumerActorId] - [*] ~~>> postStop() - DONE")
	}


	override def receive: Receive = {
		case Messages.StartConsumer =>
			log.info(s"ActorId=[$consumerActorId] - Messages.StartConsumer == ${topicList(0)}")
			consumer.subscribe(Arrays.asList(topicList(0)))
			self ! Messages.ConsumeRecord

		case Messages.ConsumeRecord =>
			//--log.info(s"ActorId=[$consumerActorId] - Messages.ConsumeRecord ==")
			try {
				val records: ConsumerRecords[String,String] = consumer.poll(3000)
				self ! Messages.ProcessRecords(records)
			} catch {
				case wakeupEx: WakeupException =>
					log.error(s"[$$] ActorId=[$consumerActorId] Kafka Wakeup Exception - consumer closed...$wakeupEx")
					consumer.close()
				case _: Throwable =>
					log.error(s"[$$] ActorId=[$consumerActorId] Kafka Exception")
			}

		case Messages.ProcessRecords(records: ConsumerRecords[String,String]) =>
			//--log.info(s"ActorId=[$consumerActorId] - Messages.ProcessRecords ==")
			if (!records.isEmpty()) {
				processRecords(records)
			}
			self ! Messages.ConsumeRecord

		case _ =>
			log.info(s"[$$][$consumerActorId] --- un-handled message ==")
	}


	protected def processRecords(records: ConsumerRecords[String, String]) = {
		//--records.forEach(record => log.info(s"[*] ActorId=[$consumerActorId] Incoming [Kafka] DevRecord = topic[${record.topic}], " +
		//--													s"partition[${record.partition}], offset[${record.offset}], value = {record.value}"))
		records.forEach(record => process(record))
	}


	protected def process(record: ConsumerRecord[String, String]) = {
		try {
			val msgBody = record.value
			//--log.info(s"[*] ActorId=[$consumerActorId] --------------- process(): msgBody = $msgBody")
			//	msgBody = {
			//			 "sensorID":"00027",
			//			 "sensorType":"platform",
			//			 "stationID":"station-3",
			//			 "deviceID":"02:1E:31:79:E6:E5",
			//			 "power":-86,"dsStatus":2,
			//			 "datetime":"2018-05-29 10:40:15"
			//	 }

			tokenizeDeviceRecord(consumerActorId, msgBody) match {
				case Right(devRecord) =>
					var conn: Connection = null
					try {
						//-- check if the new_record is on the global_dev table
						conn = DBDataSource.getConnection(consumerActorId)
						val dbRecord = getDBRecord(conn, devRecord)
						dbRecord match {
							case Some(dbRecord) =>
								updateStatsTables(conn, devRecord, dbRecord)
							case None =>
								insertNewRecordAndUpdateStatsTable(conn, devRecord)
						}
					} catch {
						case sqlEx: SQLException =>
							log.error(s"ActorId[$consumerActorId] SQLException: ${sqlEx.getMessage}")
						case ex: Exception =>
							log.error(s"ActorId[$consumerActorId] Exception: ${ex.getMessage}")
					} finally {
						if (conn != null) {
							conn.close()
						}
					}

				case Left(failure) =>
					log.warning(s"[*] ActorId[$consumerActorId] - Invalid Device Record!! - ${failure}")
			}
		} catch {
			case e1: IOException =>
				log.warning(s"[$$] ActorId[$consumerActorId] - IOException - ${e1.getMessage}")
			case th: Exception =>
				log.error(s"[$$] ActorId[$consumerActorId] - Exception is occurred - ${th.getMessage}")
		}
	}


	private def tokenizeDeviceRecord(myIndex: Int, messageBody: String): Either[Error,FilteredDeviceRecord] = {
		import io.circe.parser.decode
		import io.circe.generic.auto._

		//------------------------------
		//-- 2018/6/1, used circe Json to parse Json string instead of Gson
		//---- DeviceRecord(00012,gate,station-2,7C:7A:91:85:F2:91,-79,1,2018-06-01 07:31:25)
		//---- FilteredDeviceRecord(0007,station-1,7C:7A:91:85:F2:91,2018-05-30 03:36:03)
		decode[DeviceRecord](messageBody) match {
			case Right(devRec: DeviceRecord) =>
				log.info(s"[*] ActorId=[$consumerActorId] Incoming devRecord = $devRec")
				val recInstance = FilteredDeviceRecord(devRec.sensorID, devRec.stationID, devRec.deviceID, devRec.datetime)
				//--log.debug(s"[$$] ActorId=[$consumerActorId] * tokenize FilteredDeviceRecord(): " + recInstance)
				Right(recInstance)
			case Left(failure) =>
				log.warning(s"[*] ActorId=[$consumerActorId] ERROR: Incoming devRecord = $failure")
				Left(failure)
		}
	}


	private def getDBRecord(conn: Connection, newRecord: FilteredDeviceRecord): Option[DBDeviceRecord] = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		val partitionName = getPartitionNameForDate(newRecordDT.toLocalDate)
		val selectQry = s""" SELECT dev_id, DATE_FORMAT(start_date, '%Y-%m-%d') as start_date,
			                 |        DATE_FORMAT(last_datetime, '%Y-%m-%d %H:%i:%s') as last_datetime, station_id
			                 | FROM global_dev_tbl  PARTITION (${partitionName})
			                 | WHERE dev_id = ? ;
										""".stripMargin

		try {
			val prepareStmt = conn.prepareStatement(selectQry)
			prepareStmt.setString(1, newRecord.deviceID)
			val rs = prepareStmt.executeQuery

			if (rs.next) {
				val start_date = LocalDate.parse(rs.getString("start_date"), DATE_ONLY_FORMATTER)
				val last_datetime = LocalDateTime.parse(rs.getString("last_datetime"), DATE_TIME_FORMATTER)
				val dev_id = rs.getString("dev_id")
				val station_id = rs.getString("station_id")
				val dbRecord = DBDeviceRecord(dev_id, start_date, last_datetime, station_id)
				//log.debug(s"ActorId=[$consumerActorId] getDBRecord() - *** FOUND = ${dbRecord}")
				return Some(dbRecord)
			}
			else {
				//log.debug(s"ActorId=[$consumerActorId] getDBRecord() - NOT FOUNT = ${newRecord}")
				None
			}
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$consumerActorId] -- getDBRecord(): SQLException: ${sqlEx}")
			case ex: Throwable =>
				log.error(s"ActorId[$consumerActorId] -- getDBRecord(): Exception: ${ex.getMessage}")
		}

		None
	}


	private def insertNewRecordAndUpdateStatsTable(conn: Connection, newRecord: FilteredDeviceRecord): Boolean = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		val partitionName = getPartitionNameForDate(newRecordDT.toLocalDate)
		val newRecDateTime = newRecordDT.format(DATE_TIME_FORMATTER)
		val insertDevTblQry = s""" INSERT INTO global_dev_tbl PARTITION (${partitionName})
			                       |       (dev_id, start_date, last_datetime, station_id)
			                       | VALUES (?, ?, ?, ?) ;
													""".stripMargin

		log.info(s"ActorId=[$consumerActorId] INSERT New Record & UPDATE Stats(Visits) Table -- " +
						s"(devId = ${newRecord.deviceID}, pName = ${partitionName}, newRecord = ${newRecord})")

		//----------------------------------------------------------------
		//-- 1) INSERT into global_dev_tbl
		try {
			val preparedStmt = conn.prepareStatement(insertDevTblQry)
			preparedStmt.setString(1, newRecord.deviceID)
			preparedStmt.setString(2, newRecDateTime)
			preparedStmt.setString(3, newRecDateTime)
			preparedStmt.setString(4, newRecord.stationID)
			preparedStmt.execute

			log.debug(s"ActorId=[$consumerActorId] insertDevTblQry: ${preparedStmt}")
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$consumerActorId] SQLException: ${sqlEx}")
				log.error(s"ActorId=[$consumerActorId] insertDevTblQry: ${insertDevTblQry}")
				return false
			case ex: Throwable =>
				log.error(s"ActorId[$consumerActorId] Exception: ${ex.getMessage}")
				return false
		}

		// 2) (UPDATE 1st time today) STATION_STATS_HOURLY_TBL, count = count + 1
		//-- 2017/12/13, MODIFIED - insert/update Station-Stats Table when a new station-id added after table init
		//   from Scheduler.
		if (!updateStationStatsHourlyTbl(conn, newRecord)) {
			val successInsertInit = selectInsertStationStatsRecordForDate(newRecordDT.toLocalDate, newRecord.stationID)
			if (successInsertInit) {
				val successUpdate = updateStationStatsHourlyTbl(conn, newRecord)
				if (successUpdate != true) {
					log.error(s"ActorId=[$consumerActorId] ** INSERT-UPDATE ERROR (1): ")
					return false
				} else {
					log.error(s"ActorId=[$consumerActorId] INSERT-UPDATE SUCCESS: ")
					return true
				}
			} else {
				log.error(s"ActorId=[$consumerActorId] ** INSERT ERROR (2): ")
				return false
			}
		}

		true
	}


	private def updateStatsTables(conn: Connection, newRecord: FilteredDeviceRecord, dbRec: DBDeviceRecord): Unit = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		log.debug(s"ActorId=[$consumerActorId] -- UPDATE Stats(Visits) Table -- (devId=${newRecord.deviceID}), " +
							s"(dbRec, newRecord) = (${dbRec.station_id} --> ${newRecord.stationID})," +
							s"(DB: ${dbRec.last_dt}, Dev: ${newRecordDT})")

		//========================================================================
		//-- IF dev IS in the SAME station
		if (newRecord.stationID == dbRec.station_id) {
			if (newRecordDT.isAfter(dbRec.last_dt.plusMinutes(TIME_DELTA_MINUTES))) {
				//----------------------------------------------------------------
				//-- 1-1) UPDATE STATION_STATS_HOURLY_TBL, count + 1
				log.debug(s"ActorId=[$consumerActorId] A-1/2) UPDATE Visitors, count + 1 IN SAME station (" + dbRec.station_id + " --> " + newRecord.stationID + "), ")

				//-- 2017/12/13, MODIFIED - insert/update Station-Stats Table when a new station-id added
				//  after table init from Scheduler.
				if (updateStationStatsHourlyTbl(conn, newRecord) != true) {
					val successInsertInit = selectInsertStationStatsRecordForDate(newRecordDT.toLocalDate, newRecord.stationID)

					if (successInsertInit) {
						val successUpdate = updateStationStatsHourlyTbl(conn, newRecord)

						if (successUpdate != true) {
							log.error(s"ActorId=[$consumerActorId] * UPDATE ERROR: ")
						} else {
							//--log.info(s"ActorId=[$consumerActorId] + UPDATE SUCCESS: ")
						}
					} else {
						log.error(s"ActorId=[$consumerActorId] * INSERT ERROR: ")
					}
				}

				//-- 1-2) UPDATE global_dev_tbl, UPDATE last_ts
				log.debug(s"ActorId=[$consumerActorId] A-2/2) UPDATE Device Table (last_ts) IN SAME station (${dbRec.station_id} --> ${newRecord.stationID})")
				updateGlobalDevTblWithLastDT(conn, newRecord)
			} else {
				//-- NO UPDATE
				//--log.info(s"ActorId=[$consumerActorId] updateStatsTables(), new-Record < db-rec + 30  <<--------");
			}
		}
		else {
			//-- IF dev is in DIFFERENT station NOW
			//-- 2-1) UPDATE STATION_STATS_HOURLY_TBL, count + 1
			log.debug(s"ActorId=[$consumerActorId] B-1/3) UPDATE Visitors, count + 1 IN DIFFERENT st (${dbRec.station_id} --> ${newRecord.stationID})")

			if (updateStationStatsHourlyTbl(conn, newRecord) != true) {
				val successInsertInit = selectInsertStationStatsRecordForDate(newRecordDT.toLocalDate, newRecord.stationID)

				if (successInsertInit) {
					val successUpdate = updateStationStatsHourlyTbl(conn, newRecord)

					if (successUpdate != true) {
						log.error(s"ActorId=[$consumerActorId] * UPDATE ERROR: ")
					} else {
						log.info(s"ActorId=[$consumerActorId] + UPDATE SUCCESS: ")
					}
				} else {
					log.error(s"ActorId=[$consumerActorId] * INSERT ERROR:")
				}
			}

			//-- 2-2) UPDATE flows_stats_tbl, count + 1
			log.debug(s"ActorId=[$consumerActorId] B-2/3) UPDATE Flows, count + 1 (${dbRec.station_id} --> ${newRecord.stationID})")

			//-- 2017/12/13, MODIFIED - insert/update Flows-Stats Table when a new station-id added
			if (updateFlowsStatsHourlyTbl(conn, newRecord, dbRec) != true) {
				//-- SRC: dbRec.station_id, DST: newRecord.stationID
				val successInsertInit = selectInsertFlowsStatsRecordForDate(newRecordDT.toLocalDate, dbRec.station_id, newRecord.stationID)

				if (successInsertInit) {
					val successUpdate = updateFlowsStatsHourlyTbl(conn, newRecord, dbRec)
					if (successUpdate != true) {
						log.error(s"ActorId=[$consumerActorId] * UPDATE ERROR: ")
					}
					else {
						//--log.info(s"ActorId=[$consumerActorId] + UPDATE SUCCESS: ")
					}
				}
				else {
					log.error(s"ActorId=[$consumerActorId] * INSERT ERROR: ")
				}
			}

			//-- 2-3) UPDATE global_dev_tbl, UPDATE last_ts, station_id
			log.debug(s"ActorId=[$consumerActorId] B-3/3) UPDATE Device Table (last_ts, station_id) IN DIFFERENT st (${dbRec.station_id} --> ${newRecord.stationID}) ")
			updateGlobalDevTblWithStationId(conn, newRecord)
		}
	}


	//-- 2017/12/12, ADDED
	//-- UPDATE station_stats_hourly
	private def updateStationStatsHourlyTbl(conn: Connection, newRecord: FilteredDeviceRecord) = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		val newRecDate = newRecordDT.format(DATE_ONLY_FORMATTER)
		val hour = newRecordDT.getHour
		var ret = false

		//----------------------------------------------------------------
		// UPDATE station_stats_hourly, count = count + 1
		val columnHour = columnMap.get(hour).getOrElse("hour_00")
		val updateStationStatsTblQry = s""" UPDATE station_stats_hourly
			                                | SET  ${columnHour} = ${columnHour} + 1
			                                | WHERE station_id = ? AND stat_date = ? ;
																	""".stripMargin

		try {
			val preparedStmt = conn.prepareStatement(updateStationStatsTblQry)
			preparedStmt.setString(1, newRecord.stationID)
			preparedStmt.setString(2, newRecDate)
			val rowCount = preparedStmt.executeUpdate
			//--log.debug(s"ActorId=[$consumerActorId]  Station Stats sql = " + preparedStmt.toString)

			if (rowCount >= 1) {
				ret = true
				//--log.debug(s"ActorId=[$consumerActorId]  updateStationStatsHourlyTbl(),  ret = true")
			} else {
				ret = false
				//--log.debug(s"ActorId=[$consumerActorId]  updateStationStatsHourlyTbl(),  ret = false")
			}
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$consumerActorId]  SQLException - updateStationStatsHourlyTbl() : " + sqlEx)
				ret = false
			case ex: Throwable =>
				log.error(s"ActorId=[$consumerActorId]  Exception: ${ex.getMessage}")
				ret = false
		}

		ret
	}


	//-- 2017/12/12, ADDED
	//-- UPDATE flows_stats_hourly
	private def updateFlowsStatsHourlyTbl(conn: Connection, newRecord: FilteredDeviceRecord, dbRec: DBDeviceRecord): Boolean = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		val newRecDate = newRecordDT.format(DATE_ONLY_FORMATTER)
		val hour = newRecordDT.getHour
		var ret = false

		//----------------------------------------------------------------
		//-- UPDATE flows_stats_tbl, count + 1
		val columnHour = columnMap.get(hour).getOrElse("hour_00")
		val updateFlowsStatsTblQry = s""" UPDATE flows_stats_hourly
			                              | SET  ${columnHour}  = ${columnHour} + 1
			                              | WHERE src_station_id = ? AND dst_station_id = ? AND stat_date = ? ;
																	""".stripMargin

		try {
			val preparedStmt = conn.prepareStatement(updateFlowsStatsTblQry)
			preparedStmt.setString(1, dbRec.station_id)
			preparedStmt.setString(2, newRecord.stationID)
			preparedStmt.setString(3, newRecDate)
			val rowCount = preparedStmt.executeUpdate
			//--log.debug(s"ActorId=[$consumerActorId]  Flows Stats sql = ${preparedStmt.toString}")

			if (rowCount >= 1) {
				ret = true
				//--log.debug(s"ActorId=[$consumerActorId] updateFlowsStatsHourlyTbl(), ret = true")
			} else {
				ret = false
				//-log.debug(s"ActorId=[$consumerActorId] updateFlowsStatsHourlyTbl(), ret = false")
			}
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$consumerActorId] SQLException - updateFlowsStatsHourlyTbl() : $sqlEx")
				ret = false
			case ex: Throwable =>
				log.error(s"ActorId=[$consumerActorId]  Exception: ${ex.getMessage}")
				ret = false
		}

		ret
	}


	//--------------------------------------------
	//-- UPDATE global_dev_tbl
	private def updateGlobalDevTblWithLastDT(conn: Connection, newRecord: FilteredDeviceRecord): Boolean = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		val partitionName = getPartitionNameForDate(newRecordDT.toLocalDate)
		var ret = false

		//----------------------------------------------------------------
		//-- UPDATE last_ts only in global_dev_tbl,
		val updateDevTblQry = s""" UPDATE global_dev_tbl PARTITION (${partitionName})
			                       | SET   last_datetime = ?
			                       | WHERE dev_id = ? ;
													""".stripMargin

		try {
			val preparedStmt = conn.prepareStatement(updateDevTblQry)
			preparedStmt.setString(1, newRecord.datetime)
			preparedStmt.setString(2, newRecord.deviceID)
			val rowCount = preparedStmt.executeUpdate

			if (rowCount >= 1) {
				ret = true
				//log.debug(s"ActorId=[$consumerActorId] updateGlobalDevTblWithLastDT(), ret = true");
			} else {
				ret = false
				//log.debug(s"ActorId=[$consumerActorId] updateGlobalDevTblWithLastDT(), ret = false");
			}
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$consumerActorId]  SQLException: $sqlEx")
				ret = false
			case ex: Throwable =>
				log.error(s"ActorId=[$consumerActorId]  Exception: ${ex.getMessage}")
				ret = false
		}

		ret
	}


	private def updateGlobalDevTblWithStationId(conn: Connection, newRecord: FilteredDeviceRecord): Boolean = {
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val newRecordDT = LocalDateTime.parse(newRecord.datetime, DATE_TIME_FORMATTER)
		val partitionName = getPartitionNameForDate(newRecordDT.toLocalDate)
		var ret = false

		//----------------------------------------------------------------
		//-- UPDATE last_ts, station_id in global_dev_tbl
		val updateDevTblQry = s""" UPDATE global_dev_tbl PARTITION (${partitionName})
			                       | SET   last_datetime = ?, station_id = ?
			                       | WHERE dev_id = ? ;
													""".stripMargin

		try {
			val preparedStmt = conn.prepareStatement(updateDevTblQry)
			preparedStmt.setString(1, newRecord.datetime)
			preparedStmt.setString(2, newRecord.stationID)
			preparedStmt.setString(3, newRecord.deviceID)
			val rowCount = preparedStmt.executeUpdate

			if (rowCount >= 1) {
				ret = true
				//log.debug(s"ActorId=[$consumerActorId]  updateGlobalDevTblWithStationId(), ret = true");
			} else {
				ret = false
				//log.debug(s"ActorId=[$consumerActorId]  updateGlobalDevTblWithStationId(), ret = false");
			}
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$consumerActorId]  SQLException: $sqlEx")
				ret = false
			case ex: Throwable =>
				log.error(s"ActorId=[$consumerActorId]  Exception: ${ex.getMessage}")
				ret = false
		}

		ret
	}


	private def selectInsertStationStatsRecordForDate(day: LocalDate, stationId: String): Boolean = {
		val pName = getPartitionNameForDate(day)

		//-- SELECT --------
		val selectQuery = s""" SELECT *
			                   | FROM  station_stats_hourly PARTITION (${pName})
			                   | WHERE station_id = ? AND stat_date = ? ;
											""".stripMargin

		//-- INSERT --------
		val insertQry = s""" INSERT INTO station_stats_hourly PARTITION (${pName})
			                 |   (station_id, stat_date,
			                 |    hour_00, hour_01, hour_02, hour_03, hour_04,
			                 |    hour_05, hour_06, hour_07, hour_08, hour_09,
			                 |    hour_10, hour_11, hour_12, hour_13, hour_14,
			                 |    hour_15, hour_16, hour_17, hour_18, hour_19,
			                 |    hour_20, hour_21, hour_22, hour_23)
			                 | VALUES (?, ?,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0 ) ;
										""".stripMargin

		var ret = false
		val localDateStr = day.format(DATE_ONLY_FORMATTER)
		log.debug(s"ActorId=[$consumerActorId] INSERT INTO station_stats_hourly, pName = $pName")

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(consumerActorId)
			val selectStmt = conn.prepareStatement(selectQuery)
			val insertStmt = conn.prepareStatement(insertQry.toString)
			//--log.debug(s"ActorId=[$consumerActorId] station_stats_hourly, stationId = " + stationId);
			//-- SELECT
			selectStmt.setString(1, stationId)
			selectStmt.setString(2, localDateStr)
			val rs = selectStmt.executeQuery

			if (rs.next != true) {
				//-- NOT EXIST -> INSERT
				log.debug(s"ActorId=[$consumerActorId] station_stats_hourly - INSERT RECORD($stationId, $localDateStr) - $day, pName = $pName")
				insertStmt.setString(1, stationId)
				insertStmt.setString(2, localDateStr)
				val rowCount = insertStmt.executeUpdate

				if (rowCount >= 1) {
					ret = true
					//log.debug(s"ActorId=[$consumerActorId] selectInsertStationStatsRecordForDate(), ret = true");
				} else {
					ret = false
					//log.debug(s"ActorId=[$consumerActorId] selectInsertStationStatsRecordForDate(), ret = false");
				}
			}
			else {
				//-- ALREADY EXIST -> NO OP
				ret = true
				log.debug(s"ActorId=[$consumerActorId] station_stats_hourly - NO_OP RECORD($stationId,$localDateStr) - $day, pName = $pName")
			}
			log.debug(s"ActorId=[$consumerActorId] station_stats_hourly - initialized for $day, pName = $pName")
		} catch {
			case sqlEx: SQLException =>
				ret = false
				log.warning(s"ActorId=[$consumerActorId] *** SQLException - selectInsertStationStatsRecordForDate() *** : INSERT STATION_STATS_HOURLY_TBL - ${sqlEx.getMessage}")
			case ex: Throwable =>
				log.error(s"ActorId=[$consumerActorId]  Exception: ${ex.getMessage}")
				ret = false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		ret
	}


	private def selectInsertFlowsStatsRecordForDate(day: LocalDate, srcStationId: String, dstStationId: String): Boolean = {
		val pName = getPartitionNameForDate(day)
		//-- SELECT --------
		val selectQuery = s""" SELECT *
			                   | FROM  flows_stats_hourly PARTITION (${pName})
			                   | WHERE src_station_id = ? AND dst_station_id = ? AND stat_date = ? ;
												""".stripMargin

		//-- INSERT --------
		val insertQry = s""" INSERT INTO flows_stats_hourly PARTITION ($pName)
			                 |   (src_station_id, dst_station_id, stat_date,
			                 |    hour_00, hour_01, hour_02, hour_03, hour_04,
			                 |    hour_05, hour_06, hour_07, hour_08, hour_09,
			                 |    hour_10, hour_11, hour_12, hour_13, hour_14,
			                 |    hour_15, hour_16, hour_17, hour_18, hour_19,
			                 |    hour_20, hour_21, hour_22, hour_23)
			                 | VALUES (?, ?, ?,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0, 0,
			                 |         0, 0, 0, 0 ) ;
			                 |""".stripMargin
		var ret = false
		val localDateStr = day.format(DATE_ONLY_FORMATTER)
		log.debug(s"ActorId=[$consumerActorId] INSERT INTO flows_stats_hourly ---, pName = $pName")

		var conn: Connection = null
		try {
			conn = DBDataSource.getConnection(-1)
			val selectStmt = conn.prepareStatement(selectQuery)
			val insertStmt = conn.prepareStatement(insertQry.toString)

			if (srcStationId != dstStationId) {
				//-- SELECT
				selectStmt.setString(1, srcStationId)
				selectStmt.setString(2, dstStationId)
				selectStmt.setString(3, localDateStr)
				val rs = selectStmt.executeQuery

				if (rs.next != true) {
					//-- NOT EXIST -> INSERT
					log.debug(s"ActorId=[$consumerActorId] flows_stats_hourly - INSERT RECORD($srcStationId,$dstStationId,$localDateStr) - $day , pName = $pName")
					insertStmt.setString(1, srcStationId)
					insertStmt.setString(2, dstStationId)
					insertStmt.setString(3, localDateStr)
					val rowCount = insertStmt.executeUpdate

					if (rowCount >= 1) {
						ret = true
						//--log.debug(s"ActorId=[$consumerActorId] selectInsertStationStatsRecordForDate(), ret = true")
					} else {
						ret = false
						//--log.debug(s"ActorId=[$consumerActorId] selectInsertStationStatsRecordForDate(), ret = false")
					}
				} else {
					//-- ALREADY EXIST -> NO OP
					ret = true
					log.debug(s"ActorId=[$consumerActorId] flows_stats_hourly - NO-OP RECORD($srcStationId,$dstStationId,$localDateStr) - $day, pName = $pName")
				}
			} else {
				ret = true
			}
			log.debug(s"ActorId=[$consumerActorId] flows_stats_hourly - initialized for $day, pName = $pName")
		} catch {
			case sqlEx: SQLException =>
				ret = false
				log.error(s"ActorId=[$consumerActorId] *** SQLException - selectInsertFlowsStatsRecordForDate(): INSERT INTO flows_stats_hourly - ${sqlEx.getMessage}")
			case ex: Throwable =>
				log.error(s"ActorId=[$consumerActorId]  Exception: ${ex.getMessage}")
				ret = false
		} finally {
			if (conn != null) {
				conn.close()
			}
		}

		ret
	}
}

