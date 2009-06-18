/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for SocketStore class.
 * 
 * Copyright (c) 2009 Webroot Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include "SocketStore.h"
#include "ConnectionPool.h"
#include "voldemort/UnreachableStoreException.h"

#include <sstream>
#include <boost/bind.hpp>

namespace Voldemort {

using namespace boost;
using asio::ip::tcp;

SocketStore::SocketStore(const std::string& storeName,
                         const std::string& storeHost,
                         int storePort,
                         shared_ptr<ClientConfig>& conf,
                         shared_ptr<ConnectionPool>& pool,
                         RequestFormat::RequestFormatType requestFormatType,
                         bool shouldReroute) 
    : name(storeName), host(storeHost), port(storePort), reroute(shouldReroute), 
      config(conf), connPool(pool),
      request(RequestFormat::newRequestFormat(requestFormatType)) {

}

SocketStore::~SocketStore() {
    close();
}

std::list<VersionedValue>* SocketStore::get(const std::string& key) {
    try {
        ConnectionPoolSentinel conn(connPool->checkout(host, port), connPool);
        std::iostream& sstream = conn->get_io_stream();
        
        request->writeGetRequest(&sstream,
                                 &name,
                                 &key,
                                 reroute);
        sstream.flush();
        return request->readGetResponse(&sstream);
    } catch (UnreachableStoreException& e) {
        throw UnreachableStoreException(std::string("Failure to get ") + host + 
                                        std::string(": ") + e.what());
    }
}

void SocketStore::put(const std::string& key, const VersionedValue& value) {
    try {
        ConnectionPoolSentinel conn(connPool->checkout(host, port), connPool);
        std::iostream& sstream = conn->get_io_stream();

        request->writePutRequest(&sstream,
                                 &name,
                                 &key,
                                 value.getValue(),
                                 dynamic_cast<const VectorClock*>(value.getVersion()),
                                 reroute);
        sstream.flush();
        request->readPutResponse(&sstream);
    } catch (UnreachableStoreException& e) {
        throw UnreachableStoreException(std::string("Failure to put ") + host + 
                                        std::string(": ") + e.what());
    }
}

bool SocketStore::deleteKey(const std::string& key, const Version& version) {
    try {
        ConnectionPoolSentinel conn(connPool->checkout(host, port), connPool);
        std::iostream& sstream = conn->get_io_stream();

        request->writeDeleteRequest(&sstream,
                                    &name,
                                    &key,
                                    dynamic_cast<const VectorClock*>(&version),
                                    reroute);
        sstream.flush();
        return request->readDeleteResponse(&sstream);
    } catch (UnreachableStoreException& e) {
        throw UnreachableStoreException(std::string("Failure to delete ") + host + 
                                        std::string(": ") + e.what());
    }
}

const std::string* SocketStore::getName() {
    return &name;
}

void SocketStore::close() {
    /* noop */
}


} /* namespace Voldemort */
