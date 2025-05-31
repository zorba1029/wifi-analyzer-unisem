/**
	* Copyright (C) 2017 Unisem Inc. <http://www.unisem.co.kr>
	*
	* Created by Heung-Mook CHOI (zorba)
	*/
//
// 1) create topic (only one time)
// 	$ ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 5 --topic "test-topic-analyzer"
//-----------------------------
// 2) start zookeeper
// 	$ bin/zookeeper-server-start.sh config/zookeeper.properties
//-----------------------------
// 3) start kafka server
// 	$ bin/kafkas-server-start.sh config/server.properties
//-----------------------------
// 4) verify that the topic was created in success
// 	$ bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic "test-topic-analyzer"
//  $ ./bin/kafka-topics.sh --list --zookeeper "localhost:2181"
//-----------------------------
// 5) VM Options
//   -Dlog4j.configuration=file:"./log4j.properties" -Dfile.encoding=UTF8
// 6) Program arguments
//   sconf.cfg
// #-- 2018/5/17
// #-- FOR Scala
// java -Dconfig.file=application.conf -Xmx1g -Dlogback.configurationFile=./logback.xml -cp ./metrobus-analyzer-actor.jar com.unisem.metrobus.analyzer.MetrobusAnalyzerMain
//-----------------------------------------------------------------------------
package com.unisem.metrobus.analyzer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.unisem.metrobus.analyzer.db.{DBDataSource, DeviceLogDBSource}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContextExecutor


object MetrobusAnalyzerMain extends App {
	implicit val actorSystem: ActorSystem = ActorSystem("metrobus-analyzer")
	implicit val materializer: ActorMaterializer = ActorMaterializer()
	implicit val executionContext: ExecutionContextExecutor = actorSystem.dispatcher
	private val logger = LoggerFactory.getLogger(MetrobusAnalyzerMain.getClass)

	if (!InstanceChecker.lockInstance("/tmp/lock_analyzer")) {
		logger.error(s"**** Failed to lock Instance. ****")
		System.exit(1)
	}
	InstanceChecker.printConfigs()

	DBDataSource()
	//-- 2018/9/3, ADDED a new DB (2nd DB) source
	DeviceLogDBSource()

	ConsumerContainer()
}

