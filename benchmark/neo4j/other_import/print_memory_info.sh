cat /proc/`ps -ef | grep neo4j-community-5.9.0|grep -v 'grep'|awk '{print $2}'`/status | grep "VmPeak\|VmHWM"
