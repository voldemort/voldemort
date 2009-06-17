/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for VectorClock class.
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

#include <stdexcept>
#include <sys/time.h>
#include "VectorClock.h"

namespace Voldemort {

VectorClock::VectorClock(uint64_t timestamp_)
    : timestamp(timestamp_) {
    versions = new std::list<std::pair<short, uint64_t> > ();
}

VectorClock::VectorClock() {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    timestamp = (uint64_t)tv.tv_sec*1000 + (uint64_t)tv.tv_usec/1000;
    versions = new std::list<std::pair<short, uint64_t> > ();
}

VectorClock::VectorClock(std::list<std::pair<short, uint64_t> >* versions, uint64_t timestamp) {
    this->timestamp = timestamp;
    this->versions = new std::list<std::pair<short, uint64_t> >(*versions);
}

VectorClock::~VectorClock() {
    delete versions;
    versions = NULL;
}

VectorClock* VectorClock::copy() const{
    return new VectorClock(versions, timestamp);
}

VectorClock::Occurred VectorClock::compare(const Version* v) const {
    const VectorClock* vc = dynamic_cast<const VectorClock*>(v);
    if (vc)
        return compare(this, vc);

    throw std::invalid_argument("Could not cast Version to VectorClock");
}

VectorClock::Occurred VectorClock::compare(const VectorClock* v1, 
                                           const VectorClock* v2) {
    // We do two checks: v1 <= v2 and v2 <= v1 if both are true then
    bool v1Bigger = false;
    bool v2Bigger = false;

    std::list<std::pair<short, uint64_t> >::const_iterator it1, it2;
    it1 = v1->versions->begin();
    it2 = v2->versions->begin();

    while (it1 != v1->versions->end() && it2 != v2->versions->end()) {
        std::pair<short, uint64_t> ver1 = *it1;
        std::pair<short, uint64_t> ver2 = *it2;
        
        if (ver1.first == ver2.first) {
            if (ver1.second > ver2.second)
                v1Bigger = true;
            else if (ver2.second > ver1.second)
                v2Bigger = true;

            ++it1;
            ++it2;
        } else if (ver1.first > ver2.first) {
            // since ver1 is bigger that means it is missing a version that
            // ver2 has
            v2Bigger = true;
            ++it2;
        } else {
            // this means ver2 is bigger which means it is missing a version
            // ver1 has
            v1Bigger = true;
            ++it1;
        }
    }

    /* Okay, now check for left overs */
    if (it1 != v1->versions->end())
        v1Bigger = true;
    else if (it2 != v2->versions->end())
        v2Bigger = true;

    /* This is the case where they are equal, return BEFORE arbitrarily */
    if(!v1Bigger && !v2Bigger)
        return VectorClock::EQUAL;
    /* This is the case where v1 is a successor clock to v2 */
    else if(v1Bigger && !v2Bigger)
        return VectorClock::AFTER;
    /* This is the case where v2 is a successor clock to v1 */
    else if(!v1Bigger && v2Bigger)
        return VectorClock::BEFORE;
    /* This is the case where both clocks are parallel to one another */
    else
        return VectorClock::CONCURRENTLY;
}

const std::list<std::pair<short, uint64_t> >* VectorClock::getEntries() const {
    return versions;
}

uint64_t VectorClock::getTimestamp() {
    return timestamp;
}

void VectorClock::toStream(std::ostream& output) const {
    output << "version(";
    std::list<std::pair<short, uint64_t> >::const_iterator it;
    for (it = versions->begin(); it != versions->end(); ++it) {
        if (it != versions->begin()) 
            output << ", ";
        output << it->first << ":" << it->second;
    }
    output << ")";
}

} /* namespace Voldemort */
