 pid=`ps -ef | grep arangodb310|grep -v 'grep'|awk '{print $2}'`
 kill -9 $pid 
 sleep 1s
 ./arangod --server.endpoint http+tcp://0.0.0.0:8532 --database.directory /home/liuhanyin/data/arangodb-data --query.memory-limit  67446538240 --query.global-memory-limit 67446538240  &
