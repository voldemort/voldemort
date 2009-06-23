#!/bin/bash

#if [ $# != 3 ];
#do
#  echo "USAGE: $0 "
#done

EC2_COMMANDS_PATH=

CLUSTER_SIZE=4
INSTANCE_SIZE=m1.large
GROUP_NAME=vold-bench

# create group, if it doesn't exist
ec2-add-group $GROUP_NAME -d "voldemort benchmark security group."
ec2-authorize $GROUP_NAME -P tcp -p 6666
ec2-authorize $GROUP_NAME -P tcp -p 8081

result=$(ec2-run-instances ami-2547a34c -n $CLUSTER_SIZE -g vold-bench -k gsg-keypair --instance-type $INSTANCE_SIZE --availability-zone us-east-1b)
instance_ids=$(echo $result | grep INSTANCE | awk '{print 2}')

function wait_instance_available {
  echo "Waiting for instance $1 to start"
  while true; do
    echo -n "."
    # get private dns
    host=$(ec2-describe-instances $1 | grep running | awk '{print $5}')
    if [ ! -z $host ]; then
      echo $host
      break;
    fi
    sleep 1
  done
}

## Wait until all the instances are listed as available 
public_ips_file=/tmp/vold-bench-public.txt
private_ips_file=/tmp/vold-bench-private.txt
for instance in $instance_ids;
do
  wait_instance_available $instance
  echo $(ec2-describe-instances $instance | awk '{print $3}') >> $public_ips_file
  echo $(ec2-describe-instances $instance | awk '{print $4}') >> $public_ips_file
done

public_ips=$(ec2-describe-instances $1 | grep running | awk '{print $5}')

function check_host_booted {
  while true; do
    REPLY=$(ssh $SSH_OPTS "root@$1" 'echo "hello"')
    if [ ! -z $REPLY ]; then
     break;
    fi
    sleep 5
  done
}