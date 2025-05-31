#!/usr/bin/env bash
nohup java -Dconfig.file=/opt/istanbul_metrobus/collector/collector.conf  -cp  /opt/istanbul_metrobus/collector/metrobus-collector-actor.jar  com.unisem.metrobus.collector.MetroBusCollectorMain > /dev/null 2>&1 &
#nohup java  -cp  ./collector-consumer_main.jar -Dlog4j.configuration=file:"./log4j.properties" -Dfile.encoding=UTF8  com.unisem.collector.CollectorMain ./sconf.cfg > /dev/null 2>&1 &
