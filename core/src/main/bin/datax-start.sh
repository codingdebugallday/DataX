#!/bin/bash

source /etc/profile

DATAX_HOME=E:/myGitCode/MyDatax/target/datax/datax

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
com.alibaba.datax.app.DataxApplication & echo $! > datax.pid