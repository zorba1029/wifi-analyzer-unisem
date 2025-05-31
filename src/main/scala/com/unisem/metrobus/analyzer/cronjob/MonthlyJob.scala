/**
	* Copyright (C) 2017 Unisem Inc. <http://www.unisem.co.kr>
	*/
package com.unisem.metrobus.analyzer.cronjob

import com.typesafe.config.ConfigFactory
import org.quartz.Job
import org.quartz.JobExecutionContext
import org.quartz.JobExecutionException
import org.quartz.JobKey
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

import org.slf4j.LoggerFactory


object MonthlyJob {
	private val logger = LoggerFactory.getLogger(classOf[MonthlyJob])
}


class MonthlyJob() extends Job {
	val appConfig = ConfigFactory.load()

	@throws[JobExecutionException]
	override def execute(context: JobExecutionContext): Unit = {
		processLastMonthStats(context)
	}

	@throws[JobExecutionException]
	private def processLastMonthStats(context: JobExecutionContext): Unit = {
		try {
			val jobKey = context.getJobDetail.getKey
			val currentDateTime = LocalDateTime.now
			//-----------------------------------------------
			// 2018/4/25, ADDED timezone_delta hour
			//-- adjust current DateTime: Turkey Metro Bus - utc+3
			val adjDateTime = currentDateTime.plusHours(appConfig.getInt("analyzer.timezone_delta"))
			val adjDate = currentDateTime.plusHours(appConfig.getInt("analyzer.timezone_delta")).toLocalDate
			val lastMonthYear = adjDate.minusMonths(1) //-- last month

			MonthlyJob.logger.info(s"[*] ------->> MonthlyJob (Last Month): ${jobKey} executing at ${adjDateTime}")

			val year = lastMonthYear.getYear
			val month = lastMonthYear.getMonthValue   //-- 1 ~ 12

			//-- get monthly stats from <station stats> table for the last month
			val stationStatsList = MonthlyStatsDBI.getStationStatsMonthly(year, month)
			if (stationStatsList != null) {
				MonthlyStatsDBI.saveStationStatsMonthly(stationStatsList, year, month)
			}

			TimeUnit.SECONDS.sleep(2)

			//-- get monthly stats from <flow stats> table for the last month
			val flowsStatsList = MonthlyStatsDBI.getFlowsStatsMonthly(year, month)
			if (flowsStatsList != null) {
				MonthlyStatsDBI.saveFlowsStatsMonthly(flowsStatsList, year, month)
			}
		} catch {
			case e: Exception =>
				MonthlyJob.logger.info("MonthlyJob: Exception - " + e)
				throw new JobExecutionException(e)
		}
	}
}

