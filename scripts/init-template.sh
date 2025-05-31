#!/usr/bin/env bash
### BEGIN INIT INFO
# Provides: packet_cyber
# Required-Start: $remote_fs $syslog
# Required-Stop: $remote_fs $syslog
# Default-Start: 2 3 4 5
# Default-Stop: 0 1 6
# Short-Description: Start daemon at boot time
# Description: Enable service provided by daemon.
### END INIT INFO

#--------------------------------------------------------------------------
# 2016/12/12, zorba
#---------------------------------------------------------------------------
# #update-rc.d packet_cyber defaults 98 02
# update-rc.d: warning: /etc/init.d/packet_cyber missing LSB information
# update-rc.d: see <http://wiki.debian.org/LSBInitScripts>
#  Adding system startup for /etc/init.d/packet_cyber ...
#   /etc/rc0.d/K02packet_cyber -> ../init.d/packet_cyber
#   /etc/rc1.d/K02packet_cyber -> ../init.d/packet_cyber
#   /etc/rc6.d/K02packet_cyber -> ../init.d/packet_cyber
#   /etc/rc2.d/S98packet_cyber -> ../init.d/packet_cyber
#   /etc/rc3.d/S98packet_cyber -> ../init.d/packet_cyber
#   /etc/rc4.d/S98packet_cyber -> ../init.d/packet_cyber
#   /etc/rc5.d/S98packet_cyber -> ../init.d/packet_cyber
#-----------------
# remove
# update-rc.d -f packet_cyber remove
#---------------------------------------------------------------------------
DAEMON_PATH=/usr/local/bro/bin
DAEMON_NAME=broctl

# Check that networking is up.
#[ ${NETWORKING} = "no" ] && exit 0

PATH=$PATH:$DAEMON_PATH

awk_check_status='
BEGIN { mgr_run = 0; proxy_run=0; worker_run=0; worker_stop=0; }
{
        if ($2 == "manager") {
                if ($4 == "running") mgr_run += 1;
        }
        if ($2 == "proxy") {
                if ($4 == "running") proxy_run += 1;
        }
        if ($2 == "worker") {
                if ($4 == "running") worker_run += 1;
                else  worker_stop += 1;
        }
}
END {
        if (worker_run > 0 && mgr_run > 0 && proxy_run > 0) {
                print mgr_run,proxy_run,worker_run;

        } else {
                print mgr_run,proxy_run,worker_run;
        }
}'

awk_deploy_result='
BEGIN { mgr_run = 0; proxy_run=0; worker_run=0; worker_stop=0; }
{
        if ($1 == "starting") {
                if ($2 == "manager") mgr_run += 1;
                if (match($2, /proxy.*/,m)) proxy_run += 1;
                if (match($2, /worker.*/,m))  worker_run += 1;
        }
}
END {
        if (worker_run > 0 && mgr_run > 0 && proxy_run > 0) {
                print mgr_run,proxy_run,worker_run;

        } else {
                print mgr_run,proxy_run,worker_run;
        }
}'

awk_start_result='
BEGIN { mgr_run = 0; proxy_run=0; worker_run=0; worker_stop=0; }
{
        if ($1 == "starting") {
                if ($2 == "manager") mgr_run += 1;
                if (match($2, /proxy.*/,m)) proxy_run += 1;
                if (match($2, /worker.*/,m))  worker_run += 1;
        }
}
END {
        if (worker_run > 0 && mgr_run > 0 && proxy_run > 0) {
                print mgr_run,proxy_run,worker_run;

        } else {
                print mgr_run,proxy_run,worker_run;
        }
}'

awk_stop_result='
BEGIN { mgr_run = 0; proxy_run=0; worker_run=0; worker_stop=0; }
{
        if ($1 == "stopping") {
                if ($2 == "manager") mgr_run += 1;

                if (match($2, /proxy.*/,m)) proxy_run += 1;

                if (match($2, /worker.*/,m))  worker_run += 1;
        }

        else if ($2 == "not") {
                if ($2 == "manager") mgr_run = 0;

                if (match($1, /proxy.*/,m)) proxy_run = 0;

                if (match($1, /worker.*/,m))  worker_run = 0;
        }
}
END {
        if (worker_run > 0 && mgr_run > 0 && proxy_run > 0) {
                print mgr_run,proxy_run,worker_run;

        } else {
                print mgr_run,proxy_run,worker_run;
        }
}'



#------------------------------------------------------------------------
CHECK_STATUS=""

check_status()
{
        local tmp_status=`$DAEMON_PATH/$DAEMON_NAME status | awk "$awk_check_status"`
    #echo [$tmp_status]
    local regex="^([0-9]+) ([0-9]+) ([0-9]+)"
    if [[ "$tmp_status" =~ $regex ]]
    then
        local mgr_cnt=${BASH_REMATCH[1]}
        local proxy_cnt=${BASH_REMATCH[2]}
        local worker_cnt=${BASH_REMATCH[3]}
        if [ "$mgr_cnt" -gt 0 ] && [ "$proxy_cnt" -gt 0 ] && [ "$worker_cnt" -gt 0 ]; then
                    #echo "PacketCyber is Running" "(Mgr=$mgr_cnt, Proxy=$proxy_cnt, Worker=$worker_cnt)";
                        CHECK_STATUS="RUNNING"
        else
            #echo "PacketCyber is not Running"
                        CHECK_STATUS="STOP"
        fi
    else
        #echo "PacketCyber is not Running"
                CHECK_STATUS="STOP"
    fi
}

deploy_engine()
{
    local status_result=`$DAEMON_PATH/$DAEMON_NAME deploy | awk "$awk_deploy_result"`
    #echo [$status_result]
    local regex="^([0-9]+) ([0-9]+) ([0-9]+)"
    if [[ "$status_result" =~ $regex ]]
    then
        local mgr_cnt=${BASH_REMATCH[1]}
        local proxy_cnt=${BASH_REMATCH[2]}
        local worker_cnt=${BASH_REMATCH[3]}
        if [ "$mgr_cnt" -gt 0 ] && [ "$proxy_cnt" -gt 0 ] && [ "$worker_cnt" -gt 0 ]; then
                    echo "PacketCyber is Deployed" "(Mgr=$mgr_cnt, Proxy=$proxy_cnt, Worker=$worker_cnt)";
                        CHECK_STATUS="DEPLOYED"
        else
            #echo "PacketCyber is not Stated"
                        CHECK_STATUS="NOT_DEPLOYED"
        fi
    else
        #echo "PacketCyber is not Stated"
                CHECK_STATUS="NOT_DEPLOYED"
    fi
}

start_engine()
{
    local status_result=`$DAEMON_PATH/$DAEMON_NAME start | awk "$awk_start_result"`
    #echo [$status_result]
    local regex="^([0-9]+) ([0-9]+) ([0-9]+)"
    if [[ "$status_result" =~ $regex ]]
    then
        local mgr_cnt=${BASH_REMATCH[1]}
        local proxy_cnt=${BASH_REMATCH[2]}
        local worker_cnt=${BASH_REMATCH[3]}
        if [ "$mgr_cnt" -gt 0 ] && [ "$proxy_cnt" -gt 0 ] && [ "$worker_cnt" -gt 0 ]; then
                    echo "PacketCyber is Started" "(Mgr=$mgr_cnt, Proxy=$proxy_cnt, Worker=$worker_cnt)";
                        CHECK_STATUS="RUNNING"
        else
            #echo "PacketCyber is not Stated"
                        CHECK_STATUS="STOP"
        fi
    else
        #echo "PacketCyber is not Stated"
                CHECK_STATUS="STOP"
    fi
}



stop_engine()
{
    local status_result=`$DAEMON_PATH/$DAEMON_NAME stop | awk "$awk_stop_result"`
    #echo [$status_result]
    local regex="^([0-9]+) ([0-9]+) ([0-9]+)"
    if [[ "$status_result" =~ $regex ]]
    then
        local mgr_cnt=${BASH_REMATCH[1]}
        local proxy_cnt=${BASH_REMATCH[2]}
        local worker_cnt=${BASH_REMATCH[3]}
        if [ "$mgr_cnt" -gt 0 ] && [ "$proxy_cnt" -gt 0 ] && [ "$worker_cnt" -gt 0 ]; then
                    #echo "PacketCyber is Stopped" "(Mgr=$mgr_cnt, Proxy=$proxy_cnt, Worker=$worker_cnt)";
                        CHECK_STATUS="STOPPED"
        else
            #echo "PacketCyber is not Running"
                        CHECK_STATUS="NOT_RUNNING"
        fi
    else
        #echo "PacketCyber is not Running"
                CHECK_STATUS="NOT_RUNNING"
    fi
}

# See how we were called.
case "$1" in
  deploy)
        echo "Deploy PacketCyber";
                deploy_engine

                if [ $CHECK_STATUS == "DEPLOYED" ]; then
                        echo "PacketCyber is Started/Running"
                else
                        echo "Re-tryied to start PacketCyber. "
                        start_engine
                fi
        ;;

  start)
        # Start daemon.
        echo "Starting PacketCyber";
                check_status

                if [ $CHECK_STATUS == "RUNNING" ]; then
                        echo "PacketCyber is already Running"
                else
                        start_engine
                fi
        ;;
  stop)
        # Stop daemons.
        echo "Stop PacketCyber";
        stop_engine

                if [ $CHECK_STATUS == "STOPPED" ]; then
                        echo "PacketCyber is Stopped"
                else
                        echo "PacketCyber is NOT Running"
                fi
        ;;
  restart)
        echo "Restart PacketCyber";
        stop_engine
                if [ $CHECK_STATUS == "STOPPED" ]; then
                        echo "PacketCyber is Stopped"
                else
                        echo "PacketCyber is NOT Running"
                fi
                sleep 1
                if [ $CHECK_STATUS == "RUNNING" ]; then
                        echo "PacketCyber is already Running"
                else
                        start_engine
                fi
        ;;
  status)
        echo "Status PacketCyber";
                check_status

                if [ $CHECK_STATUS == "RUNNING" ]; then
                        echo "PacketCyber is Running"
                else
                        echo "PacketCyber is NOT Running"
                fi
        ;;
  *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
esac

exit 0

