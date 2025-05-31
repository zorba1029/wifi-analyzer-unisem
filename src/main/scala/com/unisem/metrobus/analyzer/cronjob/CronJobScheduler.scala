/**
	* Copyright (C) 2017 Unisem Inc. <http://www.unisem.co.kr>
	*
	* Created by Heung-Mook CHOI (zorba)
	*/
package com.unisem.metrobus.analyzer.cronjob

import org.quartz.CronScheduleBuilder
import org.quartz.JobBuilder
import org.quartz.TriggerBuilder
import org.quartz._
import org.quartz.impl.StdSchedulerFactory
import org.slf4j.LoggerFactory


object CronJobScheduler {
	private val logger = LoggerFactory.getLogger(CronJobScheduler.getClass)

	def apply() = {
		new CronJobScheduler()
	}
}


class CronJobScheduler() {
	import CronJobScheduler._

	logger.info("[[CronJobScheduler]] : ^^^^^^^^^^^^ START ^^^^^^^^^^^^^^^^")
	MonthlyStatsDBI.createStationStatsMonthlyTable()
	MonthlyStatsDBI.createFlowsStatsMonthlyTable()
	private val factory = new StdSchedulerFactory()
	private val scheduler: Scheduler = factory.getScheduler

	//-- Cron Expression
	//	# *    *    *    *    *  command to execute
	//	# ┬    ┬    ┬    ┬    ┬
	//	# │    │    │    │    │
	//	# │    │    │    │    │
	//	# │    │    │    │    └───── day of week (0 - 6) (0 to 6 are Sunday to Saturday)
	//	# │    │    │    └────────── month (1 - 12)
	//	# │    │    └─────────────── day of month (1 - 31)
	//	# │    └──────────────────── hour (0 - 23)
	//	# └───────────────────────── min (0 - 59)
	def execute(): Unit = {
		try {
			//-- First we must get a reference to a scheduler
			//-- add Listener
			scheduler.getListenerManager.addSchedulerListener(new CronSchedulerListener(scheduler))
			//-- job 1 will run every 20 seconds
			val job = JobBuilder.newJob(classOf[MonthlyJob]).withIdentity("MonthlyJob-1", "group-1").build
			//-----------------------------------------------------
			//-- Monthly Job
			//-- "0 5 3 1 * ?" -- 1st day every month, am 03:05
			//			CronTrigger triggerMonthly = TriggerBuilder.newTrigger()
			//							.withIdentity("trigger-Month", "group-1")
			//							.withSchedule(CronScheduleBuilder.cronSchedule("0 5 3 1 * ?"))
			//							//--.withSchedule(CronScheduleBuilder.cronSchedule("0 34 13 4 * ?"))
			//							.build();
			//			Date timeMonth = scheduler.scheduleJob(job, triggerMonthly);
			//
			//			logger.info("[^^-^^] ------->> " + job.getKey() + " has been scheduled (" + timeMonth + ") with : ["
			//							+ triggerMonthly.getCronExpression() + "]");
			//-----------------------------------------------------
			//-- Yearly Job
			//-- "0 5 4 1 1 *" -- 1st day January, every year, am 04:05
			//	CronTrigger triggerYearly = TriggerBuilder.newTrigger()
			//					.withIdentity("trigger-Year", "group-1")
			//					.withSchedule(CronScheduleBuilder.cronSchedule("0 5 4 1 1 ?"))
			//					.build();
			//-----------------------------------------------------
			//-- TEST ONLY
			//-- "0/20 * * * * ?" -- every 20 seconds,
			val testTrigger = TriggerBuilder.newTrigger
					.withIdentity("testTrigger-1", "group-1")
					.withSchedule(CronScheduleBuilder.cronSchedule("0 0/5 * * * ?"))  //-- every 5 minutes
//					.withSchedule(CronScheduleBuilder.cronSchedule("0 0/60 * * * ?")) //-- every 1 hour
					.build()

			val timeTest = scheduler.scheduleJob(job, testTrigger)

			logger.info(s"[CronJobScheduler] [^^-^^] ------->> ${job.getKey} has been scheduled (${timeTest}) with : [${testTrigger.getCronExpression}]")

			scheduler.start()
		} catch {
			case e: SchedulerException =>
				logger.info("[[CronJobScheduler]] ------->>  SchedulerException - " + e)
			case e: Exception =>
				logger.info("[*[CronJobScheduler]] ------->> Exception - " + e)
		}
	}

	@throws[SchedulerException]
	def shutdown(): Unit = {
		try {
			scheduler.shutdown()
			logger.info("[[CronJobScheduler]] : ^^^^^^^^^^^^ STOP ^^^^^^^^^^^^^^^^")
		} catch {
			case e: SchedulerException =>
				logger.error("[[CronJobScheduler]]: SchedulerException - " + e)
				throw new SchedulerException(e)
		}
	}
}
