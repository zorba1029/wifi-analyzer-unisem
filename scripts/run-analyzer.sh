#!/usr/bin/env bash
#--nohup java  -cp  ./analyzer-consumer_main.jar -Dlog4j.configuration=file:"./log4j.properties" -Dfile.encoding=UTF8  com.unisem.analyzer.AnalyzerMain ./sconf.cfg > /dev/null 2>&1 &
#-- FOR JAVA
#--nohup java -Xmx1g  -Dlogback.configurationFile=./logback.xml -Dfile.encoding=UTF8 -cp ./metrobus-analyzer_main.jar  com.unisem.metrobus.analyzer.MetroBusAnalyzerMain ./sconf.cfg > /dev/null 2>&1 &

#-- 2018/5/17
#-- FOR Scala
nohup Dconfig.file=application.conf -cp ./metrobus-analyzer-actor.jar com.unisem.metrobus.analyzer.MetrobusAnalyzerMain > /dev/null 2>&1 &

