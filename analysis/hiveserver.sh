#!/bin/bash

. /lib/lsb/init-functions


RETVAL_SUCCESS=0

STATUS_RUNNING=0
STATUS_DEAD=1
STATUS_DEAD_AND_LOCK=2
STATUS_NOT_RUNNING=3
STATUS_OTHER_ERROR=102


ERROR_PROGRAM_NOT_INSTALLED=5
ERROR_PROGRAM_NOT_CONFIGURED=6


RETVAL=0
SLEEP_TIME=5
PROC_NAME="java"

DAEMON="hive-server"
DESC="Hive Server"
EXEC_PATH="/usr/lib/hive/bin/hive"
SVC_USER="hive"
DAEMON_FLAGS=""
CONF_DIR="/etc/hive/conf"
PIDFILE="/var/run/hive/hive-server.pid"
LOCKDIR="/var/lock/subsys"
LOCKFILE="$LOCKDIR/hive-server"
WORKING_DIR="/var/lib/hive"

start() {
    log_success_msg "Starting $DESC (${DAEMON}): "

    checkstatusofproc
    status=$?
    if [ "$status" -eq "$STATUS_RUNNING" ]; then
        log_success_msg "${DESC} is running"
        exit 0
    fi

    LOG_FILE=/var/log/hive/${DAEMON}.out

    exec_env="HADOOP_OPTS=\"-Dhive.log.dir=`dirname $LOG_FILE` -Dhive.log.file=${DAEMON}.log -Dhive.log.threshold=INFO\""

    su -s /bin/sh $SVC_USER -c "$exec_env nohup nice -n 0 \
        $EXEC_PATH --service hiveserver \
            > $LOG_FILE 2>&1 < /dev/null & "'echo $! '"> $PIDFILE"
    sleep 3

    checkstatusofproc
    RETVAL=$?
    [ $RETVAL -eq $STATUS_RUNNING ] && touch $LOCKFILE
    return $RETVAL
}
stop() {
    log_success_msg "Stopping $DESC (${DAEMON}): "
    killproc -p $PIDFILE java
    RETVAL=$?

    [ $RETVAL -eq $RETVAL_SUCCESS ] && rm -f $LOCKFILE $PIDFILE
    return $RETVAL
}
restart() {
  stop
  start
}

checkstatusofproc(){
  pidofproc -p $PIDFILE $PROC_NAME > /dev/null
}

checkstatus(){
  checkstatusofproc
  status=$?

  case "$status" in
    $STATUS_RUNNING)
      log_success_msg "${DESC} is running"
      ;;
    $STATUS_DEAD)
      log_failure_msg "${DESC} is dead and pid file exists"
      ;;
    $STATUS_DEAD_AND_LOCK)
      log_failure_msg "${DESC} is dead and lock file exists"
      ;;
    $STATUS_NOT_RUNNING)
      log_failure_msg "${DESC} is not running"
      ;;
    *)
      log_failure_msg "${DESC} status is unknown"
      ;;
  esac
  return $status
}

condrestart(){
  [ -e $LOCKFILE ] && restart || :
}

check_for_root() {
  if [ $(id -ur) -ne 0 ]; then
    echo 'Error: root user required'
    echo
    exit 1
  fi
}

service() {
  case "$1" in
    start)
      check_for_root
      start
      ;;
    stop)
      check_for_root
      stop
      ;;
    status)
      checkstatus
      RETVAL=$?
      ;;
    restart)
      check_for_root
      restart
      ;;
    condrestart|try-restart)
      check_for_root
      condrestart
      ;;
    *)
      echo $"Usage: $0 {start|stop|status|restart|try-restart|condrestart}"
      exit 1
  esac
}

service "$1"

exit $RETVAL
