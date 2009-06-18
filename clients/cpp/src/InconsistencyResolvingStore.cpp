/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for InconsistencyResolvingStore class.
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

#include "InconsistencyResolvingStore.h"
#include "voldemort/VoldemortException.h"


namespace Voldemort {

using namespace boost;

static const bool REPAIR_READS = true;

InconsistencyResolvingStore::
InconsistencyResolvingStore(shared_ptr<Store>& store)
    : substore(store) {

}

void InconsistencyResolvingStore::
addResolver(shared_ptr<InconsistencyResolver>& resolver) {
    chain.push_back(resolver);
}

std::list<VersionedValue>* InconsistencyResolvingStore::get(const std::string& key) {
    std::list<VersionedValue>* resultList = substore->get(key);
    if (resultList != NULL && resultList->size() > 1) {
        try {
            std::list<shared_ptr<InconsistencyResolver> >::const_iterator it;
            for (it = chain.begin(); 
                 it != chain.end() && resultList->size() > 1; ++it) {
                (*it)->resolveConflicts(resultList);
            }
        } catch (...) {
            delete resultList;
            throw;
        }
    }
    return resultList;
}

void InconsistencyResolvingStore::put(const std::string& key, 
                                       const VersionedValue& value) {
    return substore->put(key, value);
}

bool InconsistencyResolvingStore:: deleteKey(const std::string& key, 
                                             const Version& version) {
    return substore->deleteKey(key, version);
}

const std::string* InconsistencyResolvingStore::getName() {
    return substore->getName();
}

void InconsistencyResolvingStore::close() {
    substore->close();
}


} /* namespace Voldemort */
