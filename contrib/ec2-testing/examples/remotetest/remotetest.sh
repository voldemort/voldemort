#!/bin/sh

LOG_SERVERS_FILE=/tmp/log-servers
LOG_CLIENTS_FILE=/tmp/log-clients
COMMANDS_FILE=/tmp/commands

if [ $# -lt 6 ]
	then
		echo "Usage: $0 <client hosts file> <server hosts file> <partitions per server> <config dir> <# requests> <# iterations> [<SSH private key>]"
		exit 1
fi

hostsClients=$1
hostsServers=$2
partitions=$3
configDir=$4
numRequests=$5
iterations=$6

if [ $# -ge 7 ]
    then
        sshPrivateKey="--sshprivatekey $7"
fi

rm -f $LOG_SERVERS_FILE $LOG_CLIENTS_FILE $COMMANDS_FILE

# Generate the cluster.xml file
./contrib/ec2-testing/bin/voldemort-clustergenerator.sh --hostnames $hostsServers --partitions $partitions > $configDir/config/cluster.xml
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-clustergenerator.sh: $exitCode, please see logs for more details"
		exit 1
fi

# Deploy our files to the remote host.
./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $hostsServers --source `pwd` $sshPrivateKey --parent "server" --logging debug 2>> $LOG_SERVERS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-deployer.sh for deploying servers: $exitCode, please see logs for more details"
		exit 1
fi

./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $hostsServers --source $configDir/* $sshPrivateKey --parent "testconfig" --logging debug 2>> $LOG_SERVERS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-deployer.sh for deploying server configuration: $exitCode, please see logs for more details"
		exit 1
fi

./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $hostsClients --source `pwd` $sshPrivateKey --parent "client" --logging debug 2>> $LOG_SERVERS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-deployer.sh for deploying clients: $exitCode, please see logs for more details"
		exit 1
fi

# We can't easily check the exit code as we run this in the background...
./contrib/ec2-testing/bin/voldemort-clusterstarter.sh --hostnames $hostsServers $sshPrivateKey --voldemortroot "server/$(basename `pwd`)" --voldemorthome "testconfig" --clusterxml $configDir/config/cluster.xml --logging debug 2>> $LOG_SERVERS_FILE &

sleep 10

bootstrapHost=`cat $hostsServers | grep "." | tail -n 1 | cut -d'=' -f2`

if [ "$bootstrapHost" = "" ]
	then
		echo "Couldn't determine bootstrap host"
		exit 1
fi

counter=0
sleep=30

cat $hostsClients |
while read line
do
	rampSeconds=$(($sleep * $counter))
	startKeyIndex=$(($numRequests * $counter))
	counter=$(($counter + 1))
	externalHost=`echo $line | cut -d'=' -f1`
	echo "$externalHost=cd "client/$(basename `pwd`)" ; sleep $rampSeconds ; ./bin/voldemort-remote-test.sh -r -w -d --iterations $iterations --start-key-index $startKeyIndex tcp://${bootstrapHost}:6666 test $numRequests" >> $COMMANDS_FILE
done

./contrib/ec2-testing/bin/voldemort-clusterremotetest.sh --hostnames $hostsClients $sshPrivateKey --commands $COMMANDS_FILE --logging debug 2>> $LOG_SERVERS_FILE > $LOG_CLIENTS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-clusterremotetest.sh: $exitCode, please see logs for more details"
		exit 1
fi

# Send SIGTERM
pids=`ps xwww | grep voldemort.utils.app.VoldemortClusterStarterApp | grep -v "run-class.sh" | grep -v "grep" | awk '{print $1}'`

if [ "$pids" != "" ]
	then
		kill -15 $pids
fi

./contrib/ec2-testing/examples/remotetest/remotetestparser.scala $LOG_CLIENTS_FILE

rm -f $LOG_SERVERS_FILE $LOG_CLIENTS_FILE $COMMANDS_FILE
