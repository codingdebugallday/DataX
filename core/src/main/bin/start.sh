#!/bin/bash
# shellcheck disable=SC2046
source /etc/profile

DATAX_HOME=$(pwd)/..
PID_FILE=datax.pid

if [ -f "${PID_FILE}" ]; then
    if [ $(sed -n "$=" "${PID_FILE}") -gt 2 ]; then
        echo "本地datax集群最多启动三个！！！目前已启动三个本地节点！！！"
        exit 1
    fi
fi

nohup java -server -Xms1g -Xmx1g \
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${DATAX_HOME}/log \
-Ddatax.home=${DATAX_HOME} \
-Dfile.encoding=UTF-8 \
-Dlogback.configurationFile=${DATAX_HOME}/conf/logback.xml \
-Dlogback.statusListenerClass=ch.qos.logback.core.status.NopStatusListener \
-Djava.security.egd=file:///dev/urandom \
-classpath $DATAX_HOME/lib/*:. \
-Dloglevel=info \
-Dlog.file.name=datax-server \
com.alibaba.datax.app.DataxApplication >/dev/null 2>&1 & echo $! >> datax.pid

echo "datax app started."
