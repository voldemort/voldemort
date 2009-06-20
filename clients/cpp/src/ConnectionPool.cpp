/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for ConnectionPool class.
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

#include "ConnectionPool.h"

#include <sstream>
#include <utility>

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>

namespace Voldemort {

using namespace boost;
using namespace std;

enum {
    STATUS_UNINIT = 0,
    STATUS_READY,
    STATUS_CHECKED_OUT
};

ConnectionPool::ConnectionPool(shared_ptr<ClientConfig>& config)
    : clientConfig(config), totalConnections(0) {

}

shared_ptr<Connection>& ConnectionPool::checkout(string& host, int port) {
    stringstream hostKey;
    hostKey << host << ":" << port;

    shared_ptr<Connection>* connRet = NULL;

    {
        // Ensure that we don't have too many connections in the pool
        unique_lock<mutex> lock(poolMutex);

        host_entry_ptr& hep = pool[hostKey.str()];
        int& host_ready_count = ready_count[hostKey.str()];

        if (!hep.get()) {
            hep = host_entry_ptr(new host_entry());
            host_ready_count = 0;
        }

        while ((host_ready_count == 0) &&
               (((clientConfig->getMaxConnectionsPerNode() > 0) && 
                 (hep->size() >= (size_t)clientConfig->getMaxConnectionsPerNode())) ||
                ((clientConfig->getMaxTotalConnections() > 0) &&
                 (totalConnections >= clientConfig->getMaxTotalConnections())))) {
            checkinCond.wait(lock);
        }

        // Search for a ready connection to try
        if (host_ready_count > 0) {
            host_entry::iterator heit;
            for (heit = hep->begin(); heit != hep->end(); ++heit) {
                if (heit->second.first == STATUS_READY) {
                    heit->second.first = STATUS_CHECKED_OUT;
                    host_ready_count -= 1;
                    return heit->second.second;
                }
            }
        }

        // Otherwise fall back to creating a new connection

        // Reserve a spot in the pool
        stringstream portStr;
        portStr << port;
        std::string portString = portStr.str();
        //cout << "Creating connection " << hostKey.str() << endl;
        shared_ptr<Connection> conn(new Connection(host, portString, 
                                                   clientConfig));
        (*hep)[(size_t)conn.get()] = make_pair((int)STATUS_UNINIT, conn);
        totalConnections += 1;
        connRet = &(((*hep)[(size_t)conn.get()]).second);
    }

    // Connect to the remote host
    try {
        (*connRet)->connect();
    } catch (...) {
        checkin(*connRet);
        throw;
    }

    {
        lock_guard<mutex> guard(poolMutex);
        (*pool[hostKey.str()])[(size_t)connRet->get()].first =
            (int)STATUS_CHECKED_OUT;
    }

    return *connRet;
}

void ConnectionPool::checkin(shared_ptr<Connection>& conn) {
    string hostKey = conn->get_host() + ":" + conn->get_port();

    //    cout << "Checking in " << hostKey << endl;

    {
        lock_guard<mutex> guard(poolMutex);

        host_entry_ptr& hep = pool[hostKey];
        if (!hep.get()) {
            hep = host_entry_ptr(new host_entry());
        }

        conn_entry& ce = (*hep)[(size_t)conn.get()];
        if (ce.first != STATUS_CHECKED_OUT || !conn->is_active()) {
            /* Something horrible has happened to our connection */
            hep->erase((size_t)conn.get());
            totalConnections -= 1;
            //cout << "Destroying connection " << hostKey << endl;
        } else {
            ready_count[hostKey] += 1;
            ce.first = STATUS_READY;
        }
    }

    checkinCond.notify_one();
}

} /* namespace Voldemort */
