/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for TimeBasedInconsistencyResolver class.
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

#include <string.h>
#include <voldemort/Version.h>
#include <voldemort/VersionedValue.h>

#include "TimeBasedInconsistencyResolver.h"
#include "VectorClock.h"

namespace Voldemort {

using namespace std;

void TimeBasedInconsistencyResolver::
resolveConflicts(std::list<VersionedValue>* items) const {
    if (items->size() <= 1)
        return;

    // Find the largest timestamp
    uint64_t maxTimestamp = 0;
    std::list<VersionedValue>::iterator it = items->begin();
    while (it != items->end()) {
        uint64_t t = dynamic_cast<VectorClock*>(it->getVersion())->getTimestamp();
        if (t > maxTimestamp) {
            maxTimestamp = t;
            ++it;
        } else {
            it = items->erase(it);
        }
    }
    
    // remove everything else.  We arbitrary take the leftmost value
    // if there's more than one value with the same timestamp.
    bool found = false;
    it = items->begin();
    while (it != items->end()) {
        uint64_t t = dynamic_cast<VectorClock*>(it->getVersion())->getTimestamp();
        if (t < maxTimestamp || (found && t == maxTimestamp)) {
            it = items->erase(it);
        } else 
            ++it;

        if (t == maxTimestamp)
            found = true;
    }
}

} /* namespace Voldemort */
