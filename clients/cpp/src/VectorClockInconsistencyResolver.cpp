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

#include <iostream>

#include <string.h>
#include <voldemort/Version.h>
#include <voldemort/VersionedValue.h>

#include "VectorClockInconsistencyResolver.h"
#include "VectorClock.h"

namespace Voldemort {

using namespace std;

void VectorClockInconsistencyResolver::
resolveConflicts(std::list<VersionedValue>* items) const {
    if (items->size() <= 1)
        return;

    std::list<VersionedValue>::iterator it;
    std::list<VersionedValue>::iterator it2;

    std::list<VersionedValue> newitems;
    for (it = items->begin(); it != items->end(); ++it) {
        VectorClock* vc1 = 
            dynamic_cast<VectorClock*>(it->getVersion());
        bool found = false;
        it2 = newitems.begin();
        while (it2 != newitems.end()) {
            bool advance = true;
            VectorClock* vc2 = 
                dynamic_cast<VectorClock*>(it2->getVersion());

            Version::Occurred o = vc1->compare(vc2);

            switch (o) {
            case Version::AFTER:
                if (found) {
                    it2 = newitems.erase(it2);
                    advance = false;
                } else {
                    *it2 = *it;
                }
                /* fall through */
            case Version::BEFORE:
            case Version::EQUAL:
                found = true;
                break;
            default:
                break;
            }
            if (advance)
                ++it2;
        }
        if (!found)
            newitems.push_back(*it);
    }
    *items = newitems;
}

} /* namespace Voldemort */
