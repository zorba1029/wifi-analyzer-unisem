package com.unisem.metrobus.analyzer.common

object DBRecords {
	case class StationStatsMonthlyRecord(station_id: String, stat_date: String, total_count: Long)

	case class FlowsStatsMonthlyRecord(src_station_id: String, dst_station_id: String, stat_date: String,
	                                   total_count: Long)
}
