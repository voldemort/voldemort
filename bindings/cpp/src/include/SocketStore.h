/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file SocketStore.h
 * @brief Interface definition file for SocketStore
 */
/* Copyright (c) 2009 Webroot Software, Inc.
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

#ifndef SOCKETSTORE_H
#define SOCKETSTORE_H

#include <voldemort/Store.h>
#include <voldemort/ClientConfig.h>
#include "RequestFormat.h"
#include "ConnectionPool.h"

#include <list>
#include <string>
#include <boost/asio.hpp>
#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;
using asio::ip::tcp;

/**
 * The client implementation of a socket store -- translates each request into a
 * network operation to be handled by the socket server on the other side.
 */
class SocketStore: public Store
{
public:
    /**
     * Construct a new SocketStore object to connect to the provided
     * host.
     *
     * @param storeName the name of the store
     * @param storeHost the hostname to connect to
     * @param storePort the port to connect to
     * @param conf the client config object
     * @param pool the connection pool for connecting to the
     * servers
     * @param requestFormatType the request format type
     * @param shouldReroute whether to enable server routing for
     * requests from this store
     */
    SocketStore(const std::string& storeName,
                const std::string& storeHost,
                int storePort,
                shared_ptr<ClientConfig>& conf,
                shared_ptr<ConnectionPool>& pool,
                RequestFormat::RequestFormatType requestFormatType,
                bool shouldReroute);

    virtual ~SocketStore();

    // Store interface 
    virtual std::list<VersionedValue>* get(const std::string& key);
    virtual void put(const std::string& key,
                     const VersionedValue& value);
    virtual bool deleteKey(const std::string& key,
                           const Version& version);
    virtual const std::string* getName();
    virtual void close();

private:
    std::string name;
    std::string host;
    int port;
    bool reroute;

    shared_ptr<ClientConfig> config;
    shared_ptr<ConnectionPool> connPool;
    shared_ptr<RequestFormat> request;
};

} /* namespace Voldemort */

#endif /* SOCKETSTORE_H */
