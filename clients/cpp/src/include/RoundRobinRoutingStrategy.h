/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file RoundRobinRoutingStrategy.h
 * @brief Interface definition file for RoundRobinRoutingStrategy
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

#ifndef ROUNDROBINROUTINGSTRATEGY_H
#define ROUNDROBINROUTINGSTRATEGY_H

#include "RoutingStrategy.h"

namespace Voldemort {

using namespace boost;

/**
 * Round robin routing strategy.  Will route requests to 
 */
class RoundRobinRoutingStrategy : public RoutingStrategy
{
public:
    /**
     * Construct a new round robin routing strategy object
     * 
     * @param config the @ref ClientConfig object
     * @param clust the @ref Cluster object
     */
    RoundRobinRoutingStrategy(shared_ptr<ClientConfig>& config,
                              shared_ptr<Cluster>& clust) 
        : RoutingStrategy(config, clust), 
          startIterator(cluster->getNodeMap()->begin()) { }

    // RoutingStrategy interface
    virtual ~RoundRobinRoutingStrategy() { }
    virtual shared_ptr<std::list<shared_ptr<Node> > > 
    routeRequest(const std::string& key);

private:
    Cluster::nodeMap::const_iterator startIterator;
};

} /* namespace Voldemort */

#endif /* ROUNDROBINROUTINGSTRATEGY_H */
