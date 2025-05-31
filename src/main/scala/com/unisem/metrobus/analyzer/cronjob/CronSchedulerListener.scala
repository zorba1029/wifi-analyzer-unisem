package com.unisem.metrobus.analyzer.cronjob

import org.quartz._
import org.slf4j.LoggerFactory


object CronSchedulerListener {
	private val logger = LoggerFactory.getLogger(classOf[CronSchedulerListener])
}

class CronSchedulerListener(val scheduler: Scheduler) extends SchedulerListener {
	override def jobScheduled(trigger: Trigger): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> job Scheduled : " + trigger.getKey.getName)
	}

	override def jobUnscheduled(triggerKey: TriggerKey): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> job Unscheduled : " + triggerKey.getName)
	}

	override def triggerFinalized(trigger: Trigger): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> trigger Finalized : " + trigger.getKey.getName)
	}

	override def triggerPaused(triggerKey: TriggerKey): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> trigger Paused : " + triggerKey.getName)
	}

	override def triggersPaused(triggerGroup: String): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> triggers Paused - Group : " + triggerGroup)
	}

	override def triggerResumed(triggerKey: TriggerKey): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> trigger Resumed : " + triggerKey)
	}

	override def triggersResumed(triggerGroup: String): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> triggers Resumed - Group : " + triggerGroup)
	}

	override def jobAdded(jobDetail: JobDetail): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> job Added : " + jobDetail.getKey.getName)
	}

	override def jobDeleted(jobKey: JobKey): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> job Deleted : " + jobKey.getName)
	}

	override def jobPaused(jobKey: JobKey): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> job Paused : " + jobKey)
	}

	override def jobsPaused(jobGroup: String): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> jobs Paused - Group : " + jobGroup)
	}

	override def jobResumed(jobKey: JobKey): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> job Resumed : " + jobKey.getName)
	}

	override def jobsResumed(jobGroup: String): Unit = {
		CronSchedulerListener.logger.info("[*] ------->> jobs Resumed - Group : " + jobGroup)
	}

	override def schedulerError(msg: String, cause: SchedulerException): Unit = {
		CronSchedulerListener.logger.error("[*] ------->> scheduler Error: cause - " + cause)
	}

	override def schedulerInStandbyMode(): Unit = {
		try
			CronSchedulerListener.logger.info("[*] ------->> scheduler In Standby Mode : " + scheduler.getSchedulerName)
		catch {
			case e: SchedulerException =>
				CronSchedulerListener.logger.error("[*] ------->> scheduler In Standby Mode - error : " + e)
		}
	}

	override def schedulerStarted(): Unit = {
		try
			CronSchedulerListener.logger.info("[*] ------->> scheduler Started : " + scheduler.getSchedulerName)
		catch {
			case e: SchedulerException =>
				CronSchedulerListener.logger.error("[*] ------->> scheduler Started - error : " + e)
		}
	}

	override def schedulerShutdown(): Unit = {
		try
			CronSchedulerListener.logger.info("[*] ------->> scheduler Shutdown : " + scheduler.getSchedulerName)
		catch {
			case e: SchedulerException =>
				CronSchedulerListener.logger.error("[*] ------->> scheduler Shutdown - error : " + e)
		}
	}

	override def schedulerShuttingdown(): Unit = {
		try
			CronSchedulerListener.logger.info("[*] ------->> scheduler Shuttingdown : " + scheduler.getSchedulerName)
		catch {
			case e: SchedulerException =>
				CronSchedulerListener.logger.error("[*] ------->> scheduler Shuttingdown - error : " + e)
		}
	}

	override def schedulingDataCleared(): Unit = {
		try
			CronSchedulerListener.logger.info("[*] ------->> scheduling Data Cleared : " + scheduler.getSchedulerName)
		catch {
			case e: SchedulerException =>
				CronSchedulerListener.logger.error("[*] ------->> scheduling Data Cleared - error : " + e)
		}
	}

	override def schedulerStarting(): Unit = {
		try
			CronSchedulerListener.logger.info("[*] ------->> scheduler Starting : " + scheduler.getSchedulerName)
		catch {
			case e: SchedulerException =>
				CronSchedulerListener.logger.error("[*] ------->> scheduler Starting - error : " + e)
		}
	}
}

