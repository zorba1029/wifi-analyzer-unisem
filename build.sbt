
name := "metrobus-analyzer"

version := "1.0"

scalaVersion := "2.12.6"

val akkaVersion = "10.1.0"
val circeVersion = "0.9.3"

libraryDependencies ++= Seq(
	"com.typesafe.akka" %% "akka-stream" % "2.5.11",
	"com.typesafe.akka" %% "akka-http-core" % akkaVersion,
	"com.typesafe.akka" %% "akka-http" % akkaVersion,
	"com.typesafe.akka" %% "akka-http-spray-json" % akkaVersion,

	//	"com.typesafe.akka" %% "akka-stream-kafka" % "0.18",
	"org.apache.kafka"  % "kafka-clients" % "1.0.0",

	//	"org.slf4j" % "log4j-over-slf4j" % "1.7.12",
	//	"org.slf4j" % "slf4j-nop" % "1.7.12",

//	"com.google.code.gson" % "gson" % "2.6.2",

	"ch.qos.logback" % "logback-classic" % "1.2.3",
	"com.typesafe.akka" % "akka-slf4j_2.12" % "2.4.17",

	"redis.clients" % "jedis" % "2.9.0",

	"com.zaxxer" % "HikariCP" % "2.7.5",

	"org.quartz-scheduler" % "quartz" % "2.2.1",
	"org.quartz-scheduler" % "quartz-jobs" % "2.2.1",

	//	"com.typesafe.slick" %% "slick" % "3.2.0",
	//	"com.typesafe.slick" %% "slick-hikaricp" % "3.2.0",

//	"org.postgresql" % "postgresql" % "42.1.1",
//
	"org.mariadb.jdbc" % "mariadb-java-client" % "2.2.0",

	"mysql" % "mysql-connector-java" % "5.1.36"
)


libraryDependencies ++= Seq(
	"io.circe" %% "circe-core",
	"io.circe" %% "circe-generic",
	"io.circe" %% "circe-parser"
).map(_ % circeVersion)

mainClass in Compile := Some("MetrobusAnalyzerMain")
mainClass in assembly := Some("MetrobusAnalyzerMain")
assemblyJarName in assembly := "metrobus-analyzer.jar"

