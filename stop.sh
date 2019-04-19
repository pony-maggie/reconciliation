ps -ef|grep -v grep|grep $USER|grep "python TimeSrv"|awk '{print "kill -9 "$2}' |sh
