/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for VectorClockInconsistencyResolver class.
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

#include "VectorClockInconsistencyResolver.h"
#include "VectorClock.h"

namespace Voldemort {

using namespace std;

static bool compare_clocks(VersionedValue& v1, VersionedValue& v2) {
    VectorClock* vc1 = dynamic_cast<VectorClock*>(v1.getVersion());
    VectorClock* vc2 = dynamic_cast<VectorClock*>(v2.getVersion());

    Version::Occurred occurred = vc1->compare(vc2);
    if (occurred == Version::BEFORE)
        return true;

    return false;
}

void VectorClockInconsistencyResolver::
resolveConflicts(std::list<VersionedValue>* items) const {
    if (items->size() <= 1)
        return;

    items->sort(compare_clocks);

    VectorClock* lastClock = 
        dynamic_cast<VectorClock*>(items->back().getVersion());
    std::list<VersionedValue>::reverse_iterator it;
    for (it = items->rbegin(); it != items->rend(); ++it) {
        VectorClock* curClock = 
            dynamic_cast<VectorClock*>(it->getVersion());
        if (Version::CONCURRENTLY != curClock->compare(lastClock))
            items->erase((++it).base());
    }
}

} /* namespace Voldemort */
