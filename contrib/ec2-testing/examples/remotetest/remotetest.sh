#!/bin/sh

HOSTS_SERVERS=/tmp/hosts-servers
HOSTS_CLIENTS=/tmp/hosts-clients
LOG=/tmp/log
COMMAND_FILE=/tmp/commands

# Create multiple instances in EC2.
#./contrib/ec2-testing/bin/voldemort-ec2instancecreator.sh --accessid $(cat $AWS_DIR/AccessKeyID.txt) --secretkey $(cat $AWS_DIR/SecretAccessKey.txt) --ami ami-e332d18a --instances 1 --keypairid mustardgrain >> $HOSTS_SERVERS
#./contrib/ec2-testing/bin/voldemort-ec2instancecreator.sh --accessid $(cat $AWS_DIR/AccessKeyID.txt) --secretkey $(cat $AWS_DIR/SecretAccessKey.txt) --ami ami-e332d18a --instances 2 --keypairid mustardgrain >> $HOSTS_CLIENTS

./contrib/ec2-testing/bin/voldemort-clustergenerator.sh --hostnames $HOSTS_SERVERS --partitions 4 > ./contrib/ec2-testing/examples/remotetest/remotetestcluster/config/cluster.xml

# Deploy our files to the remote host.
./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $HOSTS_SERVERS --source `pwd` --sshprivatekey $EC2_RSAKEYPAIR --parent "server" --logging debug 2>> $LOG
./contrib/ec2-testing/bin/voldemort-deployer.sh --hostnames $HOSTS_CLIENTS --source `pwd` --sshprivatekey $EC2_RSAKEYPAIR --parent "client" --logging debug 2>> $LOG

./contrib/ec2-testing/bin/voldemort-clusterstarter.sh --hostnames $HOSTS_SERVERS --sshprivatekey $EC2_RSAKEYPAIR --voldemortroot "server/voldemort" --voldemorthome "server/voldemort/contrib/ec2-testing/examples/remotetest/remotetestcluster" --clusterxml contrib/ec2-testing/examples/remotetest/remotetestcluster/config/cluster.xml --logging debug 2>> $LOG &

sleep 10

bootstrapHost=`tail -n 1 $HOSTS_SERVERS | cut -d'=' -f2`

counter=0
numRequests=100
sleep=3
iterations=10

rm -rf $COMMAND_FILE

grep "$#" $HOSTS_CLIENTS |
while read line
do
	rampSeconds=$(($sleep * $counter))
	startKeyIndex=$(($numRequests * $counter))
	counter=$(($counter + 1))
	externalHost=`echo $line | cut -d'=' -f1`
	echo "$externalHost=cd client/voldemort ; sleep $rampSeconds ; ./bin/voldemort-remote-test.sh -r -w -d --iterations $iterations --start-key-index $startKeyIndex tcp://${bootstrapHost}:6666 test $numRequests" >> $COMMAND_FILE
done

./contrib/ec2-testing/bin/voldemort-clusterremotetest.sh --hostnames $HOSTS_CLIENTS --sshprivatekey $EC2_RSAKEYPAIR --commands $COMMAND_FILE --logging debug 2>> $LOG > /tmp/results

# Send SIGTERM
pids=`ps xwww | grep voldemort.utils.app.VoldemortClusterStarterApp | grep -v "run-class.sh" | grep -v "grep" | awk '{print $1}'`

if [ "$pids" != "" ]
	then
		kill -15 $pids
fi

./contrib/ec2-testing/examples/remotetest/remotetestparser.scala /tmp/results

rm /tmp/results
