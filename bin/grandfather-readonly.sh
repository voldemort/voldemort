#!/bin/bash

#
#   Copyright 2010 LinkedIn, Inc
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
if [ $# -ne 1 ];
then
        echo 'USAGE: bin/grandfather-readonly.sh [readonly-folder]'
        exit 1
fi
#  Read args
READ_ONLY_DIR=$1

for stores in $READ_ONLY_DIR/*
do
        if [ -d $stores ]; then

		# Convert all to .temp
                numVersions=`find $stores -name version-* | grep -v .bak | grep -v .temp | wc -l`
		if [ $numVersions -gt 1 ]; then
                        maxVersion=`find $stores -name version-* | grep -v .bak | grep -v .temp | awk -F'-' '{print $2}' | sort -n | tail -1`
                        for versionDirNo in `find $stores -name version-* | grep -v .bak | grep -v .temp | awk -F'-' '{print $2}' | sort -n`
                        do
                                echo $stores/version-$versionDirNo $stores/version-$maxVersion.temp
                                mv $stores/version-$versionDirNo $stores/version-$maxVersion.temp
				let maxVersion=maxVersion-1
                        done
			echo '-------'
                fi
	
		# Convert all .temp to normal
                numVersionsTmp=`find $stores -name version-*.temp | grep -v .bak | wc -l`
        	if [ $numVersionsTmp -gt 1 ]; then
                        for versionDir in `find $stores -name version-*.temp | grep -v .bak | awk -F'.' '{print $1}'`
                        do
                                mv $versionDir.temp $versionDir
                        done
                        echo '======='
                fi
	fi
done

