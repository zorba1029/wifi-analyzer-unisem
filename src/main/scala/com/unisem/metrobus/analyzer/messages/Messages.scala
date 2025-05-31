package com.unisem.metrobus.analyzer.messages

import java.time.{LocalDate, LocalDateTime}

import org.apache.kafka.clients.consumer.ConsumerRecords

object Messages {
	case object SubscribeConsumer
	case object StartConsumer
	case object ConsumeRecord
	case object ShutDownConsumer
	case object MakeTablePartition



	case class ProcessRecords(records: ConsumerRecords[String,String])

	case class FilteredDeviceRecord(sensorID: String, stationID: String, deviceID: String, datetime: String)

	case class DBDeviceRecord(dev_id: String, start_dt: LocalDate, last_dt: LocalDateTime, station_id: String)

	//--------------------------------
	// 2018/5/25, modified DeviceRecord class to case class
	//----------
	case class DeviceRecord(sensorID: String, sensorType: String, stationID: String, deviceID: String, power: Int,
	                        dsStatus: Int, datetime: String)

	//-- 2018/9/3, ADDED
	case object StartLogProcessConsumer
	case object ConsumeDeviceLogRecord
	case class ProcessDeviceLogRecords(records: ConsumerRecords[String,String])

	case class DeviceLogRecord(sensorID: String, sensorType: String, stationID: String, stationName: Option[String] = None,
	                        deviceID: String, power: Int, dsStatus: Int, datetime: String)
}
