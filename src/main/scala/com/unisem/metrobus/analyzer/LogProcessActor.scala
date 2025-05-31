package com.unisem.metrobus.analyzer

import java.io.IOException
import java.sql.{Connection, SQLException}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Arrays, Properties}

import akka.actor.{Actor, ActorLogging}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import com.unisem.metrobus.analyzer.db.DeviceLogDBSource
import com.unisem.metrobus.analyzer.messages.Messages
import com.unisem.metrobus.analyzer.messages.Messages._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.duration._


object LogProcessActor {
	case object InsertLogMessages

	val MAX_MESSAGE_COUNT = 1000
	val INSERT_DURATION = 5
	val DATE_ONLY_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
	val DATE_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
	var deviceLoggingActorIdCounter: Int = 0

	def getLogProcessActorId() = {
		deviceLoggingActorIdCounter = deviceLoggingActorIdCounter + 1
		deviceLoggingActorIdCounter
	}

	private def getPartitionNameForDate(dt: LocalDate) = {
		"p%02d%02d%02d".format(dt.getYear%100, dt.getMonth.getValue, dt.getDayOfMonth)
	}
}


class LogProcessActor extends Actor with ActorLogging {
	import LogProcessActor._

	implicit val system = context.system
	implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
	implicit val executor = context.dispatcher
	implicit val timeout = Timeout(30 seconds)

	//------------------------------------------------------
	//--- Kafka DeviceLogger Consumer Setup
	//------------------------------------------------------
	val appConfig = ConfigFactory.load()

	val consumerConf = appConfig.getConfig("analyzer.consumer")
	val deviceLoggerTopic = consumerConf.getString("device_log_topic")
	val deviceLoggerGroupId = consumerConf.getString("device_log_group")

	val deviceLoggerBrokerInfo = consumerConf.getString("analyzer_broker_info")
	val deviceLoggerClusterInfo = consumerConf.getString("analyzer_cluster_info")

	val deviceLoggerCount = consumerConf.getInt("device_logger_count")

	val deviceLoggerProps = {
		val props = new Properties
		props.put("bootstrap.servers", deviceLoggerBrokerInfo)
		props.put("group.id", deviceLoggerGroupId)
		//--		props.put("zookeeper.connect", consumerClusterInfo)
		props.put("auto.commit.interval.ms", "1000")
		props.put("key.deserializer", classOf[StringDeserializer])
		props.put("value.deserializer", classOf[StringDeserializer])
		props
	}

	val deviceLogConsumer = new KafkaConsumer[String, String](deviceLoggerProps)
	val topicList: List[String] = List[String](deviceLoggerTopic)
	val logProcessActorId: Int = getLogProcessActorId()

	var logMessages: Seq[DeviceLogRecord] = Nil
	var messageCount = 0
	var flushMessages = true

	log.info(s"ActorId[$logProcessActorId], topicList=[$deviceLoggerTopic], deviceLoggerGroupId=[$deviceLoggerGroupId], clusterInfo=[$deviceLoggerClusterInfo]")

	override def preStart = {
		log.info(s"ActorId[$logProcessActorId] - [*] ~~>> preStart() ")

		context.system.scheduler.scheduleOnce(5 seconds, self, Messages.StartLogProcessConsumer)
		context.system.scheduler.schedule(5 seconds, INSERT_DURATION seconds, self, InsertLogMessages)
	}

	override def postStop = {
		deviceLogConsumer.wakeup()
		deviceLogConsumer.unsubscribe()
		deviceLogConsumer.close()

		log.info(s"ActorId[$logProcessActorId] - [*] ~~>> postStop() - DONE")
	}

	override def receive: Receive = {
		case Messages.StartLogProcessConsumer =>
			log.info(s"ActorId=[$logProcessActorId] - Messages.StartLogProcessConsumer == ${topicList(0)}")
			deviceLogConsumer.subscribe(Arrays.asList(topicList(0)))
			self ! Messages.ConsumeDeviceLogRecord

		case Messages.ConsumeDeviceLogRecord =>
			try {
				val records: ConsumerRecords[String,String] = deviceLogConsumer.poll(3000)
				self ! Messages.ProcessDeviceLogRecords(records)
			} catch {
				case wakeupEx: WakeupException =>
					log.error(s"[$$] ActorId=[$logProcessActorId] Kafka Wakeup Exception - deviceLogConsumer closed...$wakeupEx")
					deviceLogConsumer.close()
				case _: Throwable =>
					log.error(s"[$$] ActorId=[$logProcessActorId] Kafka Throwable")
			}

		case Messages.ProcessDeviceLogRecords(records: ConsumerRecords[String,String]) =>
			if (!records.isEmpty()) {
				processRecords(records)
			}
			self ! Messages.ConsumeDeviceLogRecord

		case InsertLogMessages =>
			log.debug(s"[$$][$logProcessActorId] :: case InsertLogMessages =>")
			if (flushMessages) insertLogMessages() else flushMessages = true

		case _ =>
			log.info(s"[$$][$logProcessActorId] --- un-handled message ==")
	}


	protected def processRecords(records: ConsumerRecords[String, String]) = {
		//--records.forEach(record => log.info(s"[*] ActorId=[$logProcessActorId] Incoming [Kafka] DeviceLogRecord = topic[${record.topic}], " +
		//--		s"partition[${record.partition}], offset[${record.offset}], value = ${record.value}"))
		records.forEach(record => process(record))
	}

	protected def process(record: ConsumerRecord[String, String]) = {
		import io.circe.parser.decode
		import io.circe.generic.auto._

		try {
			val msgBody = record.value
			//--log.debug(s"[*] ActorId=[$logProcessActorId] LogProcessActor // process(): msgBody = $msgBody")
			//			msgBody = {
			//				"sensorID" : "2804",
			//				"sensorType" : "P",
			//				"stationID" : "28",
			//				"stationName" : "Florya",
			//				"deviceID" : "fb4032c5962cea1e0365d6e49fe381c4",
			//				"power" : -20,
			//				"dsStatus" : 1,
			//				"datetime" : "2018-09-03 11:43:45"
			//			}
			decode[DeviceLogRecord](msgBody) match {
				case Right(devRec: DeviceLogRecord) =>
					//--log.debug(s"[$$] ActorId=[$logProcessActorId] * tokenize DeviceLogRecord: " + devRec)
					logMessages = devRec +: logMessages
					messageCount += 1
					if (messageCount >= MAX_MESSAGE_COUNT) {
						insertLogMessages()
						flushMessages = false
					}

				case Left(failure) =>
					log.warning(s"[*] ActorId=[$logProcessActorId] ERROR: DeviceLogRecord = $failure")
			}
		} catch {
			case e1: IOException =>
				log.warning(s"[$$] ActorId[$logProcessActorId] - IOException - ${e1.getMessage}")
			case th: Throwable =>
				log.error(s"[$$] ActorId[$logProcessActorId] - Exception is occurred - ${th.getMessage}")
		}
	}


	private def insertLogMessages(): Unit = {
		if (messageCount > 0) {
			insertBulkMessages(logMessages)
			logMessages = Nil
			messageCount = 0
		}
	}

	// Database: ISTB1;
	//	mysql> desc ISTB2;
	//	+----------+-------------+------+-----+---------+-------+
	//	| Field    | Type        | Null | Key | Default | Extra |
	//	+----------+-------------+------+-----+---------+-------+
	//	| SensorId | int(3)      | NO   | PRI | NULL    |       |
	//	| Mac      | varchar(32) | NO   | PRI | NULL    |       |
	//	| Rssi     | tinyint(3)  | YES  |     | NULL    |       |
	//	| TimeNow  | datetime    | NO   | PRI | NULL    |       |
	//	+----------+-------------+------+-----+---------+-------+
	//	4 rows in set (0.00 sec)
	private def insertBulkMessages(messages: Seq[DeviceLogRecord]): AnyVal = {
		//-- INSERT INTO DB Tables
		log.debug(s"[*] ActorId=[$logProcessActorId] LogProcessActor:: insertBulkMessages(): count = ${messages.length} -----------")
		//-----------------------------
		//-- 2018/4/25, MODIFIED: newRecord.datetime is already adjusted by timezone_delta
		val queryTemplate = s""" INSERT INTO ISTB2
			                     |       (SensorId, Mac, Rssi, TimeNow)
			                     | VALUES
													""".stripMargin

		val stringBuffer = new StringBuilder(queryTemplate)
		messages.zipWithIndex.foreach { case (_, i) => {
			if (i < messages.length-1) stringBuffer.append(""" (?, ?, ?, ?),""")
			else stringBuffer.append(""" (?, ?, ?, ?) ; """)
		}}
		val queryString = stringBuffer.toString()

		//--log.debug(s"ActorId=[$logProcessActorId] INSERT DeviceLog INTO ISTB2 : logMessage count = ${messages.length}")

		//----------------------------------------------------------------
		//-- 1) INSERT into ISTB2 table
		var conn: Connection = null

		try {
			conn = DeviceLogDBSource.getConnection(logProcessActorId)
			val preparedStmt = conn.prepareStatement(queryString)

			messages.zipWithIndex.foreach { case (rec, i) =>
				val newRecordDT = LocalDateTime.parse(rec.datetime, DATE_TIME_FORMATTER)
				val newRecDateTime = newRecordDT.format(DATE_TIME_FORMATTER)
				preparedStmt.setInt((4 * i) + 1, rec.sensorID.toInt)
				preparedStmt.setString((4 * i) + 2, rec.deviceID)
				preparedStmt.setInt((4 * i) + 3, rec.power)
				preparedStmt.setString((4 * i) + 4, newRecDateTime)
			}
			preparedStmt.execute
			//--log.debug(s"ActorId=[$logProcessActorId] insertDevTblQry: ${preparedStmt}")
		} catch {
			case sqlEx: SQLException =>
				log.error(s"ActorId=[$logProcessActorId] SQLException: insertNewDeviceLog() - ${sqlEx}")
				log.error(s"ActorId=[$logProcessActorId] insertDevLogTblQry(): ${queryString}")
			case ex: Throwable =>
				log.error(s"ActorId[$logProcessActorId] Throwable: insertNewDeviceLog() - ${ex.getMessage}")
		} finally {
			if (conn != null) {
				conn.close()
			}
		}
	}

}
