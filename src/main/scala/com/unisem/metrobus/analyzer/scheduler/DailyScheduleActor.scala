package com.unisem.metrobus.analyzer.scheduler

import akka.actor.{Actor, ActorLogging, Cancellable}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings}
import akka.util.Timeout

import com.unisem.metrobus.analyzer.messages.Messages

import scala.concurrent.duration._


class DailyScheduleActor extends Actor with ActorLogging {
	implicit val system = context.system
	implicit val materializer: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(context.system))
	implicit val executor = context.dispatcher
	implicit val timeout = Timeout(10 seconds)

	private val initialDelay = 0 second     //-- hour
	private val schedulePeriod = 1 hour     //-- hour
	private val partitionManager = PartitionManagerContainer()

	var scheduler: Cancellable = _

	override def preStart = {
		log.info(s"[*] ~~>> preStart() ")

		scheduler = context.system.scheduler.schedule(initialDelay, schedulePeriod, self, Messages.MakeTablePartition)
	}

	override def postStop = {
		scheduler.cancel()
		log.info(s"*] ~~>> postStop() - DONE")
	}

	override def receive: Receive = {
		case Messages.MakeTablePartition =>
			partitionManager.run()
	}
}
