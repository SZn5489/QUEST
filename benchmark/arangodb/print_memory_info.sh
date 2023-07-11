pid=`ps -ef | grep arangodb310|grep -v 'grep'|awk '{print $2}'`
cat /proc/${pid}/status | grep "VmPeak\|VmHWM"
