#!/bin/bash

while read -r line
do
 echo "==> kill datax app, PID: ${line}"
 kill -9 "${line}" 2>/dev/null
done < datax.pid
# kill后需删除pid文件
rm -f datax.pid