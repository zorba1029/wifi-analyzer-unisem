package com.unisem.metrobus.analyzer.scheduler

import org.slf4j.LoggerFactory


object PartitionManagerContainer {
	def apply() = {
		new PartitionManagerContainer()
	}
}


class PartitionManagerContainer {
	private val logger = LoggerFactory.getLogger(classOf[PartitionManagerContainer])
	private val deviceTblMgr = DeviceTablePartitionManager()
	private val stationsTblMgr = StationStatsTablesPartitionManager()
	private val flowsTblMgr = FlowsStatsTablesPartitionManager()
	private val managerList = List(deviceTblMgr, stationsTblMgr, flowsTblMgr)

	def run() = {
		logger.info("^^^^^^^^^^^^^^^ TopPartitionManager: run() is executed. ^^^^^^^^^^^^^^")

		for (mgr <- managerList) {
			mgr.process()
		}
		//		//---------------------------------------------------------------
		//		// 1. global_dev_tbl
		//		deviceTblMgr.process()
		//
		//		// 2. STATION_STATS_HOURLY_TBL,
		//		stationsTblMgr.process()
		//
		//		// 3. FLOWS_STATS_HOURLY_TBL
		//		flowsTblMgr.process()
	}
}