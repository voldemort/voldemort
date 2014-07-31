/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file RoutingStrategy.h
 * @brief Interface definition file for RoutingStrategy
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

#ifndef VOLDEMORT_ROUTINGSTRATEGY_H
#define VOLDEMORT_ROUTINGSTRATEGY_H

#include <voldemort/ClientConfig.h>
#include "Cluster.h"
#include <map>

#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;

/**
 * Interface for methods to for routing requests to Voldemort nodes.
 */
class RoutingStrategy
{
public:
    /**
     * Construct a new routing strategy object
     * 
     * @param config the @ref ClientConfig object
     * @param clust the @ref Cluster object
     */
    RoutingStrategy(shared_ptr<ClientConfig>& config,
                    shared_ptr<Cluster>& clust) 
        : cluster(clust), clientConfig(config) { }

    virtual ~RoutingStrategy() { }

    /**
     * Preferred node list
     */
    typedef std::list<shared_ptr<Node> > prefList;

    /**
     * Shared pointer to a @ref prefList
     */
    typedef shared_ptr<std::list<shared_ptr<Node> > > prefListp;

    /**
     * Route the request and return the preferred node list
     *
     * @param key the key
     * @return The node list
     */
    virtual prefListp routeRequest(const std::string& key) = 0;

protected:
    /**
     * The @ref Cluster object for this cluster
     */
    shared_ptr<Cluster> cluster;

    /**
     * The @ref ClientConfig object for this instance
     */
    shared_ptr<ClientConfig> clientConfig;
};

} /* namespace Voldemort */

#endif/*VOLDEMORT_ROUTINGSTRATEGY_H*/
