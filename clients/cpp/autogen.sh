#!/bin/sh
#
#   Voldemort C client autogen.sh
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
# This autogen script will run the autotools to generate the build
# system.  You should run this script in order to initialize a build
# immediately following a checkout.

for LIBTOOLIZE in libtoolize glibtoolize nope; do
    $LIBTOOLIZE --version 2>&1 > /dev/null && break
done

if [ "x$LIBTOOLIZE" = "xnope" ]; then
    echo
    echo "You must have libtool installed to compile rocksafe."
    echo "Download the appropriate package for your distribution,"
    echo "or get the source tarball at ftp://ftp.gnu.org/pub/gnu/"
    exit 1
fi

ACLOCALDIRS= 
if [ -d  /usr/share/aclocal ]; then
    ACLOCALDIRS="-I /usr/share/aclocal"
fi
if [ -d  /usr/local/share/aclocal ]; then
    ACLOCALDIRS="$ACLOCALDIRS -I /usr/local/share/aclocal"
fi

$LIBTOOLIZE --automake --force && \
aclocal --force $ACLOCALDIRS && \
autoconf --force $ACLOCALDIRS && \
autoheader --force && \
automake --add-missing --foreign && \
echo "You may now configure the software by running ./configure" && \
echo "Run ./configure --help to get information on the options " && \
echo "that are available."

