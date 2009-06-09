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
#include "voldemort/VoldemortException.h"


namespace Voldemort {

using namespace boost;

static const bool REPAIR_READS = true;

RoutedStore::RoutedStore(const std::string& storeName,
                         shared_ptr<ClientConfig>& config,
                         shared_ptr<Cluster>& clust,
                         shared_ptr<std::map<int, shared_ptr<Store> > >& map,
                         shared_ptr<threadpool::pool> pool)
    : name(storeName), cluster(clust), clusterMap(map), threadPool(pool) {

}

RoutedStore::~RoutedStore() {
    close();
}

std::list<VersionedValue>* RoutedStore::get(const std::string& key) {
    std::map<int, shared_ptr<Store> >::const_iterator it = 
        clusterMap->begin();
    return it->second->get(key);
}

void RoutedStore::put(const std::string& key, const VersionedValue& value) {
    std::map<int, shared_ptr<Store> >::const_iterator it = 
        clusterMap->begin();
    return it->second->put(key, value);
}

bool RoutedStore::deleteKey(const std::string& key, const Version& version) {
    std::map<int, shared_ptr<Store> >::const_iterator it = 
        clusterMap->begin();
    return it->second->deleteKey(key, version);
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
