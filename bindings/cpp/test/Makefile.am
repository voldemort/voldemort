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

AM_CPPFLAGS = $(BOOST_CPPFLAGS) -DBOOST_TEST_DYN_LINK \
	-I$(top_srcdir)/src/include -I$(top_srcdir)/src \
	-I$(top_srcdir)/include 
AM_LDFLAGS = $(BOOST_LDFLAGS)

TESTS = voldemort_test
voldemort_test_SOURCES = \
	main.cpp \
	VectorClock_test.cpp \
	ProtocolBuffersRequestFormat_test.cpp 
voldemort_test_LDADD = ../src/libvoldemort.la $(BOOST_UNIT_TEST_FRAMEWORK_LIB)

# check_PROGRAMS = $(TESTS)
noinst_PROGRAMS = $(TESTS)