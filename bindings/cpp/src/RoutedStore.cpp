/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for RoutedStore class.
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

#include "RoutedStore.h"
#include "voldemort/UnreachableStoreException.h"
#include "voldemort/InsufficientOperationalNodesException.h"
#include <iostream>

namespace Voldemort {

using namespace boost;
using namespace std;

static const bool REPAIR_READS = true;

RoutedStore::RoutedStore(const std::string& storeName,
                         shared_ptr<ClientConfig>& config,
                         shared_ptr<Cluster>& clust,
                         shared_ptr<std::map<int, shared_ptr<Store> > >& map,
                         shared_ptr<RoutingStrategy>& routingStrat)
    : name(storeName), clientConfig(config), cluster(clust), clusterMap(map), 
      routingStrategy(routingStrat) {

}

RoutedStore::~RoutedStore() {
    close();
}

static bool doGetFromStore(const std::string& key, 
                           std::list<VersionedValue>** result,
                           Node* node,
                           Store* store) {
    *result = NULL;
    try {
        *result = store->get(key);
        node->setAvailable(true);
        return true;
    } catch (UnreachableStoreException& e) {
        /* XXX - TODO add real logging */
        std::cerr << "WARNING: Could not read node: " << e.what() << std::endl;
        node->setAvailable(false);
    }
    return false;
}

std::list<VersionedValue>* RoutedStore::get(const std::string& key) {
    std::list<VersionedValue>* result = NULL;
    bool status = false;
    {
        /* Start by routing to the preferred list one at a time. */
        RoutingStrategy::prefListp prefList = routingStrategy->routeRequest(key);
        RoutingStrategy::prefList::const_iterator it;
        for (it = prefList->begin(); it != prefList->end(); ++it) {
            status = doGetFromStore(key, &result,
                                    it->get(),
                                    (*clusterMap)[(*it)->getId()].get());
            if (status) return result;
        }
    }
    {
        /* If that fails just try every node in the cluster */
        const Cluster::nodeMap* nm = cluster->getNodeMap();
        Cluster::nodeMap::const_iterator it;
        for (it = nm->begin(); it != nm->end(); ++it) {
            if (it->second.get()->isAvailable(clientConfig->getNodeBannageMs())) {
                status = doGetFromStore(key, &result,
                                        it->second.get(),
                                        (*clusterMap)[it->first].get());
            }
            if (status) return result;
        }
    }
    
    throw InsufficientOperationalNodesException("Could not reach any "
                                                "node for get operation");
}

static bool doPutFromStore(const std::string& key, 
                           const VersionedValue& value,
                           Node* node,
                           Store* store) {
    try {
        store->put(key, value);
        node->setAvailable(true);
        return true;
    } catch (UnreachableStoreException& e) {
        node->setAvailable(false);
    }
    return false;
}

void RoutedStore::put(const std::string& key, const VersionedValue& value) {
    bool status = false;
    {
        /* Start by routing to the preferred list one at a time. */
        RoutingStrategy::prefListp prefList = routingStrategy->routeRequest(key);
        RoutingStrategy::prefList::const_iterator it;
        for (it = prefList->begin(); it != prefList->end(); ++it) {
            status = doPutFromStore(key, value,
                                    it->get(),
                                    (*clusterMap)[(*it)->getId()].get());
            if (status) return;
        }
    }
    {
        /* If that fails just try every node in the cluster */
        const Cluster::nodeMap* nm = cluster->getNodeMap();
        Cluster::nodeMap::const_iterator it;
        for (it = nm->begin(); it != nm->end(); ++it) {
            if (it->second.get()->isAvailable(clientConfig->getNodeBannageMs())) {
                status = doPutFromStore(key, value,
                                        it->second.get(),
                                        (*clusterMap)[it->first].get());
            }
            if (status) return;
        }
    }
    
    throw InsufficientOperationalNodesException("Could not reach any "
                                                "node for put operation");
}

static bool doDeleteFromStore(const std::string& key, 
                              const Version& version,
                              bool* result,
                              Node* node,
                              Store* store) {
    try {
        *result = store->deleteKey(key, version);
        node->setAvailable(true);
        return true;
    } catch (UnreachableStoreException& e) {
        node->setAvailable(false);
    }
    return false;
}

bool RoutedStore::deleteKey(const std::string& key, const Version& version) {
    bool status = false;
    bool result = false;
    {
        /* Start by routing to the preferred list one at a time. */
        RoutingStrategy::prefListp prefList = routingStrategy->routeRequest(key);
        RoutingStrategy::prefList::const_iterator it;
        for (it = prefList->begin(); it != prefList->end(); ++it) {
            status = doDeleteFromStore(key, version, &result,
                                       it->get(),
                                       (*clusterMap)[(*it)->getId()].get());
            if (status) return result;
        }
    }
    {
        /* If that fails just try every node in the cluster */
        const Cluster::nodeMap* nm = cluster->getNodeMap();
        Cluster::nodeMap::const_iterator it;
        for (it = nm->begin(); it != nm->end(); ++it) {
            if (it->second.get()->isAvailable(clientConfig->getNodeBannageMs())) {
                status = doDeleteFromStore(key, version, &result,
                                           it->second.get(),
                                           (*clusterMap)[it->first].get());
            }
            if (status) return result;
        }
    }
    
    throw InsufficientOperationalNodesException("Could not reach any "
                                                "node for delete operation");
}

const std::string* RoutedStore::getName() {
    return &name;
}

void RoutedStore::close() {
    std::map<int, shared_ptr<Store> >::const_iterator it;
    for (it = clusterMap->begin(); it != clusterMap->end(); ++it) {
        it->second->close();
    }
}


} /* namespace Voldemort */
