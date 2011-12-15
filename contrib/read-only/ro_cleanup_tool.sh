#! /bin/bash

###############################################################################
# Tool to cleanup obsolete files after rebalancing in read-only server
# Author: Chinmay Soman
#
# Input: Directory where read-only data is stored
# Modes: a) Identify all the files to be deleted
#        Eg: ./ro_cleanup_tool.sh /mnt/u001/voldemort/data/read-only
#
#        b) Actually delete all the files
#	 Eg: ./ro_cleanup_tool.sh /mnt/u001/voldemort/data/read-only --delete
###############################################################################

DATA_DIR=$1
CMD_FILE="/tmp/beans_cmd.log"
OUT="/tmp/outfile"
DELETE_FILES_LOG="/tmp/delete_these.log"
STATUS_FILE="/tmp/cleanup_status"
VERBOSE="no"
DELETE="no"

[ $# -lt 1 ] && echo "Missing argument: Path of the read-only stores directory" && exit -1;
[ $# -ge 2 ] && DELETE=$2; 
[ $# -ge 3 ] && VERBOSE=$3; 

function build_jmx_cmd {
  STORE_NAME=$1
  echo "open localhost:12000" > ${CMD_FILE} 
  echo "get -d data-platform-storage/i001/voldemort.store.readonly -b type=$STORE_NAME getChunkIdToNumChunks" >> ${CMD_FILE} 
  echo "close" >> ${CMD_FILE}
}

function process_store {
  STORE_NAME=$1 
  V=0
  [ $VERBOSE = "--verbose" ] && V=1 

  # Get the active chunks
  build_jmx_cmd $STORE_NAME 
  java -jar jmxterm-1.0-alpha-4-uber.jar -i ${CMD_FILE} > ${OUT} 2>/dev/null 
  BYTES=`cat ${OUT} | wc -c | tr -d " "`
  [ ${BYTES} -eq 0 ] && echo "Cannot retrieve active chunks. Skipping store: $STORE_NAME" && return; 

  # Build hashmap of active chunks
  local hashmap
  count=1
  for i in `cat ${OUT} | tr -d " " | tr -s "]" "\n"`
  do
    echo $i | grep "\[" > /dev/null 2>&1
    [ $? -ne 0 ] && continue;
    KEY=`echo $i | cut -d"[" -f2 | tr -d ","`
    VAL=${hashmap["${KEY}"]}
    if [ -n "$VAL" ]; then
        [ $V -eq 1 ] && echo "Duplicate !!!"
        [ $V -eq 1 ] && echo "Key = $KEY. Existing value = $VAL"
    fi

    hashmap["${KEY}"]=${count}
    (( count++ ))
  done

  [ ${#hashmap[@]} -eq 0 ] && echo "Could not retrieve the active chunks. Skipping store: $STORE_NAME." && return;

  echo "*************** STORE : $STORE_NAME ***************" >> ${DELETE_FILES_LOG}

  for file in `ls $DATA_DIR/$store/latest`
  do
	[ $file = "0.data" ] && continue;
	[ $file = "0.index" ] && continue;
	SEARCH_KEY=`echo $file | cut -d "_" -f 1,2 | tr -d "_"`
	FILE_PREFIX=`echo $file | cut -d "_" -f 1,2`
	[ $SEARCH_KEY = "00" ] && continue;
	[ $SEARCH_KEY = "01" ] && continue;

	EXISTS=${hashmap["${SEARCH_KEY}"]}
	if [ ! -n "$EXISTS" ]; then
	  [ $V -eq 1 ] && echo "FILE $FILE_PREFIX* not present in the hashmap. Should be deleted"
	  [ ! -n "$FILE_PREFIX" ] && continue;
	  echo "$DATA_DIR/$store/latest/$FILE_PREFIX*" >> ${DELETE_FILES_LOG}
	fi
  done

  echo >> ${DELETE_FILES_LOG}
  echo >> ${DELETE_FILES_LOG}

  unset hashmap
}

echo "RUNNING" > ${STATUS_FILE}
if [ $DELETE = "no" ]
then
	if [ -f ${DELETE_FILES_LOG} ] 
	then 
		echo "${DELETE_FILES_LOG} already exists. Do you want to delete it and rerun the tool (y/n) ?"
		read opt
		[ $opt = "n" ] && exit 0;
	fi
	rm -f ${DELETE_FILES_LOG}
	TOTAL_FILES=`ls -l $DATA_DIR | wc -l | tr -d "\ "`
	((TOTAL_FILES--)) 
	COUNT=1 
	for store in `ls $DATA_DIR`
	do
  		echo "Processing <$store> .............................................. (${COUNT}/${TOTAL_FILES})";
  		process_store $store
  		((COUNT++))
	done
elif [ $DELETE = "--delete" ]
then
	[ ! -f $DELETE_FILES_LOG ] && echo "The delete files log : $DELETE_FILES_LOG does not exist !" && exit -1;
        for line in `cat $DELETE_FILES_LOG`
        do
                echo $line | grep latest > /dev/null 2>&1
                [ $? -ne 0 ] && continue;
                sudo -u app rm -f ${line}
        done
        exit 0
fi
echo "DONE" > ${STATUS_FILE}
