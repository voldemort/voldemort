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

SUBDIRS = src utils test

library_includedir=$(includedir)/$(PACKAGE)
library_include_HEADERS = \
	include/voldemort/ClientConfig.h \
	include/voldemort/InconsistentDataException.h \
	include/voldemort/InconsistencyResolver.h \
	include/voldemort/InsufficientOperationalNodesException.h \
	include/voldemort/InvalidMetadataException.h \
	include/voldemort/ObsoleteVersionException.h \
	include/voldemort/PersistenceFailureException.h \
	include/voldemort/BootstrapFailureException.h \
	include/voldemort/SocketStoreClientFactory.h \
	include/voldemort/StoreClientFactory.h \
	include/voldemort/StoreClient.h \
	include/voldemort/Store.h \
	include/voldemort/StoreOperationFailureException.h \
	include/voldemort/UnreachableStoreException.h \
	include/voldemort/VersionedValue.h \
	include/voldemort/Version.h \
	include/voldemort/VoldemortException.h \
	include/voldemort/voldemort.h

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = voldemort.pc

# make a binary tgz package
tgz:
	rm -rf "${top_builddir}/tgz-tmp"
	mkdir "${top_builddir}/tgz-tmp"
	make ${tgztarget} DESTDIR="`cd ${top_builddir}/tgz-tmp; pwd`"
	test -z "${extratar}" || tar -xC "${top_builddir}/tgz-tmp" -f "${extratar}"
	(cd "${top_builddir}/tgz-tmp"; $(AMTAR) cf - --owner 0 --group 0 usr | GZIP=$(GZIP_ENV) $(GZIP_BIN) -c >../${pkgtgz})
	rm -rf "${top_builddir}/tgz-tmp"

if HAVE_DOXYGEN
  noinst_DATA = \
	doc/html
endif

doc/html: $(library_include_HEADERS) doc/Doxyfile 
	cd doc && ${DOXYGEN} Doxyfile
doc: doc/html
doc/html-internal: clean-doc-internal $(library_include_HEADERS) doc/Doxyfile-internal
	cd doc && ${DOXYGEN} Doxyfile-internal
doc-internal: doc/html-internal
install-data-local: doc
	@$(NORMAL_INSTALL)
	test -z "${DESTDIR}/${docdir}/html" || $(mkdir_p) "${DESTDIR}/${docdir}/html"
	for i in `ls $(srcdir)/doc/html`; do \
	  $(INSTALL) -m 0644 $(srcdir)/doc/html/$$i "${DESTDIR}/${docdir}/html"; \
	done
uninstall-local: 
	@$(NORMAL_UNINSTALL)
	rm -rf "${DESTDIR}/${docdir}/html"

clean-doc:
	rm -rf doc/html doc/latex
clean-doc-internal:
	rm -rf doc/html-internal
clean-local: clean-doc clean-doc-internal
	rm -f *.rpm
