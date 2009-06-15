#
#   Voldemort C client configure.ac
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
# Process this file with autoconf to produce a configure script
#
# If you just want to start a build from source control, run
# autogen.sh first.
#

# ---------------------------------------------------------------
# Version information

# Before doing a release, edit:
# 1. the version in AC_INIT
# 2. the libtool version in VERSION_INFO

m4_define([sdk_pversion], 0)
m4_define([sdk_sversion], 1)
m4_define([sdk_iversion], 0)
AC_INIT([voldemort], [sdk_pversion.sdk_sversion.sdk_iversion])
AC_SUBST(SDK_MARKETNAME, ["Voldemort C++ Client"])
AC_DEFINE(SDK_MARKETNAME, ["Voldemort C++ Client"], [Marketing name for SDK])

# For more information on libtool versioning, see:
# http://www.gnu.org/software/libtool/manual.html#Versioning
# 
# The version string is [current:revision:age] or [c:r:a].  Here are a
# set of rules to help you update your library version information:
#  1. Start with version information of 0:0:0 for each libtool
#     library.
#  2. Update the version information only immediately before a public
#     release of your software. More frequent updates are unnecessary,
#     and only guarantee that the current interface number gets larger
#     faster.
#  3. If the library source code has changed at all since the last
#     update, then increment revision (c:r:a becomes c:r+1:a).
#  4. If any interfaces have been added, removed, or changed since the
#     last update, increment current, and set revision to 0.
#  5. If any interfaces have been added since the last public release,
#     then increment age.
#  6. If any interfaces have been removed since the last public
#     release, then set age to 0.

AC_SUBST(VERSION_INFO, [0:0:0])

# ---------------------------------------------------------------
# Initialization

# initialize automake and libtool
AM_INIT_AUTOMAKE
AM_CONFIG_HEADER(config.h)
AC_PROG_LIBTOOL

# ---------------------------------------------------------------
# Configure options

# modify the name of the binary tgz package 
AC_SUBST([pkgtgz], ['${PACKAGE_TARNAME}-${PACKAGE_VERSION}-${SDK_BVERSION}-${host}.tgz'])
AC_ARG_WITH(pkgtgz,
            AC_HELP_STRING([--with-pkgtarball], 
                           [binary tarball file [[name-VERSION-HOST.tgz]]]),
            [pkgtgz=${withval}],
            [])
# control whether to generate a debug build or a regular build
AC_SUBST([tgztarget], ['install-strip'])
AC_ARG_ENABLE([debugtgz],
              AC_HELP_STRING([--enable-debugtgz], 
                             [Generate a tgz package with debug symbols]), 
              [tgztarget='install'], [])

# Modify the release/build version
AC_ARG_WITH(buildversion,
            AC_HELP_STRING([--with-buildversion], 
                           [Version number of build]),
            [sdk_bversion=${withval}],
            [sdk_bversion=0])

# ---------------------------------------------------------------
# Environment introspection

# check for compiler
AC_PROG_CC
AC_PROG_CXX
AC_PROG_INSTALL
AM_PROG_AS
AC_LANG([C++])

# check for doxygen
AC_CHECK_PROGS(DOXYGEN,doxygen,none)
AM_CONDITIONAL(HAVE_DOXYGEN, [test x$DOXYGEN != 'xnone']) 

# ---------------------------------------------------------------
# Dependency checks

# Checks for header files
AC_STDC_HEADERS
AC_HAVE_HEADERS(pthread.h sys/stat.h unistd.h getopt.h)

AX_BOOST_BASE([1.3.7])
AX_BOOST_ASIO
AX_BOOST_UNIT_TEST_FRAMEWORK
AX_BOOST_THREAD

if test "x$BOOST_ASIO_LIB" = "x"; then
   AC_MSG_ERROR([Boost ASIO library is required])
fi
if test "x$BOOST_UNIT_TEST_FRAMEWORK_LIB" = "x"; then
   AC_MSG_ERROR([Boost ASIO unit test framework is required])
fi
if test "x$BOOST_THREAD_LIB" = "x"; then
   AC_MSG_ERROR([Boost Thread library is required])
fi

# Check for system pthread convention.  See macro definition in
# acinclude.m4
ACX_PTHREAD([], [])
LIBS="$PTHREAD_LIBS $LIBS"
CFLAGS="$CFLAGS $PTHREAD_CFLAGS -D_REENTRANT"
CXXFLAGS="$CXXFLAGS $PTHREAD_CFLAGS"

if test x$have_pthread = 'xfalse'; then
   AC_MSG_NOTICE([Could not find pthreads library])
fi

# Check for protocol buffers library
AC_CHECK_HEADER(google/protobuf/message.h, [], 
                AC_MSG_ERROR([Could not include protocol buffers headers]))
AC_CHECK_LIB(protobuf, main, [], 
             AC_MSG_ERROR([Could not link protocol buffers library]), [])

AC_CHECK_HEADER(expat.h, [], 
                AC_MSG_ERROR([Could not include expat header]))
AC_CHECK_LIB(expat, XML_ParserCreate, [], 
             AC_MSG_ERROR([Could not link expat library]), [])

# Older versions of autoconf don't define docdir
if test x$docdir = x; then
   AC_SUBST(docdir, ['${prefix}/share/doc/'$PACKAGE])
fi

# ---------------------------------------------------------------
# Build constants

AC_DEFINE_UNQUOTED(SDK_PVERSION, [sdk_pversion], [Major version])
AC_DEFINE_UNQUOTED(SDK_SVERSION, [sdk_sversion], [Minor version])
AC_DEFINE_UNQUOTED(SDK_IVERSION, [sdk_iversion], [Incremental version])
AC_SUBST(SDK_BVERSION, [${sdk_bversion}])
AC_DEFINE_UNQUOTED(SDK_BVERSION, [${sdk_bversion}], [Build version])
AC_DEFINE_UNQUOTED(SDK_FULL_VERSION, ["sdk_pversion.sdk_sversion.sdk_iversion-${sdk_bversion}"], 
                                     [Full version])

# ---------------------------------------------------------------
# Output

AC_CONFIG_FILES([\
        Makefile \
        src/Makefile \
        utils/Makefile \
        test/Makefile \
        voldemort.pc \
        doc/Doxyfile \
        doc/Doxyfile-internal \
])
AC_OUTPUT

AC_MSG_NOTICE([
======================================================================
Configuration complete

You may now compile the software by running 'make'
======================================================================])
