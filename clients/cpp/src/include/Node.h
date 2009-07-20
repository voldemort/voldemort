/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file Node.h
 * @brief Interface definition file for Node
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

#ifndef NODE_H
#define NODE_H

#include <string>
#include <map>
#include <list>
#include <boost/shared_ptr.hpp>
#include <expat.h>
#include <sstream>
#include <stdint.h>

namespace Voldemort {
   
using namespace boost;

class Cluster;
 
/**
 * Represent a Voldemort node configuration
 */
class Node
{
public:
    /**
     * Construct a new Node object
     *
     * @param id the node ID
     * @param host the hostname for the node
     * @param httpPort the HTTP port for the node
     * @param socketPort the socket port for the node
     * @param adminPort the admin port for the node
     * @param partitions the list of partitions hosted on the node
     */
    Node(int id,
         std::string& host,
         int httpPort,
         int socketPort,
         int adminPort,
         shared_ptr<std::list<int> >& partitions);
    Node();
    ~Node() {}

    /**
     * Get the Node ID for this node
     * 
     * @return the node ID
     */
    int getId() { return id_; }

    /**
     * Get the host name for this node
     * 
     * @return the host name
     */
    const std::string& getHost() { return host_; }

    /**
     * Get the HTTP port for this node
     * 
     * @return the HTTP port
     */
    int getHttpPort() { return httpPort_; }

    /**
     * Get the socket port for this node
     * 
     * @return the socket port
     */
    int getSocketPort() { return socketPort_; }

    /**
     * Get the admin port for this node
     * 
     * @return the admin port
     */
    int getAdminPort() { return adminPort_; }

    /**
     * Return whether the node is current available in the node list.
     *
     * @return true if the node is available
     */
    bool isAvailable() { return isAvailable_; }

    /**
     * Return whether the node is current available in the node list,
     * or if its unavailable but the timeout period specified has
     * elapsed.
     *
     * @return true if the node is available
     */
    bool isAvailable(uint64_t timeout);

    /**
     * Set whether the node is available
     * 
     * @param avail true if the node is available, false otherwise
     */
    void setAvailable(bool avail);

    /**
     * Get the system time in milliseconds when this node was last
     * checked.
     *
     * @return the time in milliseconds
     */
    uint64_t getLastChecked() { return lastChecked_; }

    /**
     * Get the number of milliseconds since the last time this node
     * was checked for availability.
     *
     * @return the number of milliseconds since we last checked for
     * this node.
     */
    uint64_t getMsSinceLastChecked();

    /** 
     * Stream insertion operator for cluster 
     *
     * @param output the stream
     * @param node the node object
     * @return the stream
     */
    friend std::ostream& operator<<(std::ostream& output, const Node& node);

private:
    friend class Cluster;

    int id_;
    std::string host_;
    int httpPort_;
    int socketPort_;
    int adminPort_;
    shared_ptr<std::list<int> > partitions_;

    /* Node status */
    bool isAvailable_;
    uint64_t lastChecked_;
};


} /* namespace Voldemort */

#endif /* NODE_H */
