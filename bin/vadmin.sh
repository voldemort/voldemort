#!/bin/bash

#
#   Copyright 2008-2014 LinkedIn, Inc
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

#  This shell script provides Voldemort Admin Tool command-line interface.
#  To see help menu, please type 'vadmin.sh help'.

base_dir=$(cd $(dirname $0)/.. && pwd)

export LOG4JPROPERTIES="-Dlog4j.configuration=file://${base_dir}/src/java/log4j-admin.properties"
$base_dir/bin/run-class.sh voldemort.tools.admin.VAdminTool $@
