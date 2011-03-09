#
#   Copyright 2011 LinkedIn, Inc
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

# To generate the java -
# > thrift --gen java MockMessage.thrift
#
namespace java voldemort.serialization.thrift

struct MockMessage 
{ 
1: string name, 
2: map<i64, map<string, i32>> mappings,
3: list<i16> intList, 
4: set<string> strSet
}
