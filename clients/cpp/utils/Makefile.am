#
#   Voldemort C client Makefile.am
#   (c) 2009 Webroot Software, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# Process this file with automake to produce a Makefile.in

AM_CPPFLAGS = -I$(top_srcdir)/include 

bin_PROGRAMS = voldemortShell stress

voldemortShell_SOURCES = voldemortShell.cpp
voldemortShell_LDADD = ../src/libvoldemort.la

stress_SOURCES = stress.cpp
stress_LDADD = ../src/libvoldemort.la
