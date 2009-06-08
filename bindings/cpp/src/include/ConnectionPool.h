/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file ConnectionPool.h
 * @brief Interface definition file for ConnectionPool
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

#ifndef CONNECTIONPOOL_H
#define CONNECTIONPOOL_H

#include <stdlib.h>
#include <string>
#include <list>
#include <utility>
#include "Connection.h"
#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;
using namespace std;

class ConnectionPoolSentinel;

/**
 * A pool of persistent @ref Connection objects that can be used to
 * send pipelined requests to a Voldemort server.
 */
class ConnectionPool
{
public:
    /**
     * Construct a new connection pool using the provided @ref
     * ClientConfig and @ref RequestFormat objects.
     * 
     * @param config the client config
     */
    explicit ConnectionPool(shared_ptr<ClientConfig>& config);

    /**
     * Get a connection for the given host and port.  If the maximum
     * number of connections has been returned, the pool will block
     * the thread until one is available.
     * 
     * @param host the host name
     * @param port the port number
     * @return a shared pointer to the connection
     */
    shared_ptr<Connection>& checkout(string& host, int port);

    /**
     * Get a connection for the given host and port.  If the maximum
     * number of connections has been returned, the pool will block
     * the thread until one is available.
     * 
     * @param conn the connection to check in
     * @return a shared pointer to the connection
     */
    void checkin(shared_ptr<Connection>& conn);

private:
    shared_ptr<ClientConfig> clientConfig;
    shared_ptr<RequestFormat> requestFormat;

    typedef pair<int, shared_ptr<Connection> > conn_entry ;
    typedef map<size_t, conn_entry> host_entry;
    typedef shared_ptr<host_entry> host_entry_ptr;
    typedef map<string, host_entry_ptr> conn_pool;
    conn_pool pool;

    int totalConnections;

    mutex poolMutex;
    condition_variable checkinCond;
};

/**
 * A sentinel object that will ensure a connection is returned to the
 * pool when it goes out of scope.
 */
class ConnectionPoolSentinel
{
public:
    /** 
     * Create a new connection pool sentinel.  The provided @ref
     * Connection will be checked into the @ref ConnectionPool when
     * the object is destroyed
     * 
     * @param conn the connection
     * @param pool the pool for the connection
     */
    ConnectionPoolSentinel(shared_ptr<Connection>& conn,
                           shared_ptr<ConnectionPool> pool)
        : conn_(conn), pool_(pool) { }
    ~ConnectionPoolSentinel() { pool_->checkin(conn_); }

    /**
     * Arrow operator
     *
     * @return reference to the underlying @ref Connection
     */
    Connection* operator->() { return conn_.get(); }

    /**
     * Dereference operator
     *
     * @return reference to the underlying @ref Connection
     */
    Connection& operator*() { return *conn_; }

private:
    shared_ptr<Connection> conn_;
    shared_ptr<ConnectionPool> pool_;
};

} /* namespace Voldemort */

#endif /* CONNECTIONPOOL_H */
