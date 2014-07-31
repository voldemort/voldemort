/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file RoutedStore.h
 * @brief Interface definition file for RoutedStore
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

#ifndef VOLDEMORT_ROUTEDSTORE_H
#define VOLDEMORT_ROUTEDSTORE_H

#include <voldemort/Store.h>
#include <voldemort/ClientConfig.h>
#include "Cluster.h"
#include "RoutingStrategy.h"
#include <map>

#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;

/**
 * A Store which multiplexes requests to different internal Stores
 */
class RoutedStore: public Store
{
public:
    /**
     * Construct a new RoutedStore object which will route requests to
     * the given collection of nodes in the cluster.
     *
     * @param storeName the name of the storee
     * @param config the @ref ClientConfig object
     * @param clust the cluster object with which to configure the
     * routed store
     * @param map a mapping from node ID to Store used for routing
     * @param routingStrat the routing strategy to use
     */
    RoutedStore(const std::string& storeName,
                shared_ptr<ClientConfig>& config,
                shared_ptr<Cluster>& clust,
                shared_ptr<std::map<int, shared_ptr<Store> > >& map,
                shared_ptr<RoutingStrategy>& routingStrat);
    virtual ~RoutedStore();

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
    shared_ptr<ClientConfig> clientConfig;
    shared_ptr<Cluster> cluster;
    shared_ptr<std::map<int, shared_ptr<Store> > > clusterMap;
    shared_ptr<RoutingStrategy> routingStrategy;
};

} /* namespace Voldemort */

#endif/*VOLDEMORT_ROUTEDSTORE_H*/
