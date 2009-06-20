/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file ClientConfig.h
 * @brief Interface definition file for ClientConfig
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

#ifndef CLIENTCONFIG_H
#define CLIENTCONFIG_H

#include <list>
#include <string>

namespace Voldemort {

/**
 * Implementation details for ClientConfig
 */
class ClientConfigImpl;

/**
 * A configuration object that holds configuration parameters for the
 * client
 */
class ClientConfig
{
public:
    ClientConfig();
    /** Copy constructor */
    ClientConfig(const ClientConfig& cc);
    ~ClientConfig();

    /** 
     * Sets the list of bootstrap URLs to use to attempt a connection.
     * This list is not copied and the memory is owned by the caller.
     * There is no default list.
     * 
     * @param bootstrapUrls the list of URLs to use
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setBootstrapUrls(std::list<std::string>* bootstrapUrls);

    /**
     * Return the current list of bootstrap URLs.
     * 
     * @return pointer to the list set using @ref setBootstrapUrls
     */
    std::list<std::string>* getBootstrapUrls();

    /**
     * Get the maximum number of allowed connections per node
     * 
     * @return the value
     */
    int getMaxConnectionsPerNode();

    /**
     * Set the maximum number of allowed connections per node
     * 
     * @param val the value
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setMaxConnectionsPerNode(int val);

    /**
     * Get the maximum number of allowed connections total
     * 
     * @return the value
     */
    int getMaxTotalConnections();

    /**
     * Set the maximum number of allowed connections total
     * 
     * @param val the value
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setMaxTotalConnections(int val);

    /**
     * Get the number of milliseconds to wait for a connection to
     * start before timing out.
     * 
     * @return the value
     */
    long getConnectionTimeoutMs();

    /**
     * Set the number of milliseconds to wait for a connection to
     * start before timing out.
     * 
     * @param val the value
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setConnectionTimeoutMs(long val);

    /**
     * Get the number of milliseconds to wait for on reading or
     * writing to a socket before timing out.
     * 
     * @return the value
     */
    long getSocketTimeoutMs();

    /**
     * Set the number of milliseconds to wait for on reading or
     * writing to a socket before timing out.
     * 
     * @param val the value
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setSocketTimeoutMs(long val);

    /**
     * Get the number of milliseconds a node will be banned before we
     * try again to connect to it following a failure.
     * 
     * @return the value
     */
    long getNodeBannageMs();

    /**
     * Set the number of milliseconds a node will be banned before we
     * try again to connect to it following a failure.
     * 
     * @param val the value
     * @return a pointer to this object useful for chaining
     */
    ClientConfig* setNodeBannageMs(long val);
    
private:
    /** Internal implementation details for ClientConfig */
    ClientConfigImpl* pimpl_;
};

} /* namespace Voldemort */

#endif /* CLIENTCONFIG_H */

