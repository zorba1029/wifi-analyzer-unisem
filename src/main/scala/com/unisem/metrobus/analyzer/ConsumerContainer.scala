package com.unisem.metrobus.analyzer

import java.util.concurrent.TimeUnit
import akka.actor.Props
import akka.routing.RoundRobinPool
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.slf4j.{Logger, LoggerFactory}
import com.unisem.metrobus.analyzer.MetrobusAnalyzerMain.actorSystem
import com.unisem.metrobus.analyzer.db.{DBDataSource, DeviceLogDBSource}

import scala.concurrent.duration._
import akka.pattern.gracefulStop
import com.unisem.metrobus.analyzer.cronjob.CronJobScheduler
import com.unisem.metrobus.analyzer.scheduler.DailyScheduleActor
import org.quartz.SchedulerException

import scala.concurrent.Await
import scala.language.postfixOps


object ConsumerContainer {
	def apply() = {
		new ConsumerContainer()
	}
}


class ConsumerContainer() {
	val log: Logger = LoggerFactory.getLogger(classOf[ConsumerContainer])

	//------------------------------------------------------
	//--- Kafka Consumer Setup
	val appConfig = ConfigFactory.load()
	val consumerCount = appConfig.getInt("analyzer.consumer.consumer_count")
	val deviceLoggerCount = appConfig.getInt("analyzer.consumer.device_logger_count")

	val cronScheduler = CronJobScheduler()
	cronScheduler.execute()

	val dailyScheduleActor = actorSystem.actorOf(Props[DailyScheduleActor], "DailyScheduleActor")

	val kafkaConsumers = actorSystem.actorOf(RoundRobinPool(consumerCount)
													.props(Props(classOf[ConsumerActor])), "consumerActor")

	//-- 2018/9/3, ADDED
	val deviceLogConsumers = actorSystem.actorOf(RoundRobinPool(deviceLoggerCount)
													.props(Props(classOf[LogProcessActor])), "logProcessActor")

	//===================================================================
	// shutdown handling
	//-------------------------------------------------------------------
	Runtime.getRuntime().addShutdownHook(new Thread(() => {
		log.info("=============== Start Shutdown ===============")
		implicit val timeout = Timeout(30 seconds)

		//---------------------------------------------------
		//-- 1) stop scheduler actor
		log.info("  -- 1) stop scheduler actor")

		val stopScheduler = gracefulStop(dailyScheduleActor, 15 seconds)
		Await.result(stopScheduler, 20 seconds)

		//-- 2) stop kafka consumers
		log.info("  -- 2-1) stop kafka consumers")
		val stopConsumers = gracefulStop(kafkaConsumers, 15 seconds)
		Await.result(stopConsumers, 20 seconds)

		log.info("  -- 2-2) stop kafka device-Logging-Consumers")
		val stopLogConsumers = gracefulStop(deviceLogConsumers, 15 seconds)
		Await.result(stopLogConsumers, 20 seconds)

		//-- 3) shutdown Quartz scheduler
		log.info("  -- 3) shutdown Quartz scheduler")
		try {
			cronScheduler.shutdown()
		} catch {
			case e: SchedulerException =>
				log.info("ConsumerContainer: SchedulerException - " + e)
		}

		//---------------------------------------------------
		//-- 4) close DB Pool
		log.info("  -- 4-1) close DB Pool")
		DBDataSource.close()

		log.info("  -- 4-2) close DB Pool")
		//-- 2018/9/3, ADDED a new DB
		DeviceLogDBSource.close()

		//---------------------------------------------------
		//-- 5) Terminate ActorSystem
		log.info("  -- 5) Terminate ActorSystem")

		try {
			val terminateFuture = actorSystem.terminate()
			Await.result(terminateFuture, 60 seconds)
			TimeUnit.SECONDS.sleep(1)
		} catch {
			case e: InterruptedException =>
				log.warn(" * -- ConsumerContainer: InterruptedException")
		}
		log.info("=============== Shutdown: Finished =============== ")
	}))
}
