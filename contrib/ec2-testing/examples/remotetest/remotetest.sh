#!/bin/sh

HOSTS_SERVERS=/tmp/hosts-servers
HOSTS_CLIENTS=/tmp/hosts-clients
LOG_SERVERS_FILE=/tmp/log-servers
LOG_CLIENTS_FILE=/tmp/log-clients
COMMANDS_FILE=/tmp/commands

if [ $# -ne 10 ]
	then
		echo "Usage: $0 <access key ID> <secret key> <ami> <key pair ID> <# clients> <# servers> <partitions per server> <config dir> <# requests> <# iterations>"
		exit 1
fi

accessKeyId=$1
secretKey=$2
ami=$3
keypairId=$4
numClients=$5
numServers=$6
partitions=$7
configDir=$8
numRequests=$9
iterations=${10}

rm -f $LOG_SERVERS_FILE $LOG_CLIENTS_FILE $COMMANDS_FILE

# Create instances in EC2.
if [ $numClients -gt 0 ]
	then
		./contrib/ec2-testing/bin/voldemort-ec2instancecreator.sh --accessid $accessKeyId --secretkey $secretKey --ami $ami --instances $numClients --keypairid $keypairId > $HOSTS_CLIENTS 2>> $LOG_SERVERS_FILE
		exitCode=$?

		if [ $exitCode -ne 0 ]
			then
				echo "Exit code from voldemort-ec2instancecreator.sh for clients: $exitCode, please see logs for more details"
				exit 1
		fi
fi

if [ $numServers -gt 0 ]
	then
		./contrib/ec2-testing/bin/voldemort-ec2instancecreator.sh --accessid $accessKeyId --secretkey $secretKey --ami $ami --instances $numServers --keypairid $keypairId > $HOSTS_SERVERS 2>> $LOG_SERVERS_FILE
		exitCode=$?

		if [ $exitCode -ne 0 ]
			then
				echo "Exit code from voldemort-ec2instancecreator.sh for servers: $exitCode, please see logs for more details"
				exit 1
		fi
fi

# Generate the cluster.xml file
./contrib/ec2-testing/bin/voldemort-clustergenerator.sh --hostnames $HOSTS_SERVERS --partitions $partitions > $configDir/config/cluster.xml
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-clustergenerator.sh: $exitCode, please see logs for more details"
		exit 1
fi

# Deploy our files to the remote host.
./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $HOSTS_SERVERS --source `pwd` --sshprivatekey $EC2_RSAKEYPAIR --parent "server" --logging debug 2>> $LOG_SERVERS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-deployer.sh for deploying servers: $exitCode, please see logs for more details"
		exit 1
fi

./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $HOSTS_SERVERS --source $configDir/* --sshprivatekey $EC2_RSAKEYPAIR --parent "testconfig" --logging debug 2>> $LOG_SERVERS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-deployer.sh for deploying server configuration: $exitCode, please see logs for more details"
		exit 1
fi

./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $HOSTS_CLIENTS --source `pwd` --sshprivatekey $EC2_RSAKEYPAIR --parent "client" --logging debug 2>> $LOG_SERVERS_FILE
exitCode=$?

if [ $exitCode -ne 0 ]
	then
		echo "Exit code from voldemort-deployer.sh for deploying clients: $exitCode, please see logs for more details"
		exit 1
fi

# We can't easily check the exit code as we run this in the background...
./contrib/ec2-testing/bin/voldemort-clusterstarter.sh --hostnames $HOSTS_SERVERS --sshprivatekey $EC2_RSAKEYPAIR --voldemortroot "server/voldemort" --voldemorthome "testconfig" --clusterxml $configDir/config/cluster.xml --logging debug 2>> $LOG_SERVERS_FILE &

sleep 10

bootstrapHost=`tail -n 1 $HOSTS_SERVERS | cut -d'=' -f2`

counter=0
sleep=30

cat $HOSTS_CLIENTS |
while read line
do
	rampSeconds=$(($sleep * $counter))
	startKeyIndex=$(($numRequests * $counter))
	counter=$(($counter + 1))
	externalHost=`echo $line | cut -d'=' -f1`
	echo "$externalHost=cd client/voldemort ; sleep $rampSeconds ; ./bin/voldemort-remote-test.sh -r -w -d --iterations $iterations --start-key-index $startKeyIndex tcp://${bootstrapHost}:6666 test $numRequests" >> $COMMANDS_FILE
done

./contrib/ec2-testing/bin/voldemort-clusterremotetest.sh --hostnames $HOSTS_CLIENTS --sshprivatekey $EC2_RSAKEYPAIR --commands $COMMANDS_FILE --logging debug 2>> $LOG_SERVERS_FILE > $LOG_CLIENTS_FILE
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
