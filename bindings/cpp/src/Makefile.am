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

AM_CPPFLAGS = $(BOOST_CPPFLAGS) \
	-I$(srcdir)/include -I$(top_srcdir)/include 

lib_LTLIBRARIES = libvoldemort.la

libvoldemort_la_SOURCES = \
	include/VectorClock.h \
	include/SocketStore.h \
	include/DefaultStoreClient.h \
	include/RequestFormat.h \
	include/VoldemortNativeRequestFormat.h \
	include/ProtocolBuffersRequestFormat.h \
	include/Connection.h \
	include/ConnectionPool.h \
	include/Cluster.h \
	include/RoutedStore.h \
	include/InconsistencyResolvingStore.h \
	include/VectorClockInconsistencyResolver.h \
	include/TimeBasedInconsistencyResolver.h \
	DefaultStoreClient.cpp \
	VersionedValue.cpp \
	VectorClock.cpp \
	SocketStoreClientFactory.cpp \
	ClientConfig.cpp \
	SocketStore.cpp \
	RequestFormat.cpp \
	VoldemortNativeRequestFormat.cpp \
	ProtocolBuffersRequestFormat.cpp \
	Connection.cpp \
	ConnectionPool.cpp \
	Cluster.cpp \
	RoutedStore.cpp \
	InconsistencyResolvingStore.cpp \
	VectorClockInconsistencyResolver.cpp \
	TimeBasedInconsistencyResolver.cpp \
	voldemort-client.pb.cc \
	voldemort-client.pb.h 

libvoldemort_la_LDFLAGS = $(BOOST_LDFLAGS) \
	-version-info $(VERSION_INFO)
libvoldemort_la_LIBADD = $(BOOST_ASIO_LIB) $(BOOST_THREAD_LIB)
