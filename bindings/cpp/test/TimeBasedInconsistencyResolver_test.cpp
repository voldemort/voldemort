/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Test suite for TimeBasedInconsistencyResolver class.
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

#include "VectorClock.h"
#include "TimeBasedInconsistencyResolver.h"
using namespace Voldemort;
using namespace std;

#include <boost/test/unit_test.hpp>

struct TimeBasedFixture {
    TimeBasedFixture() {
        std::list<std::pair<short, uint64_t> > versions;

        current.setVersion(new VectorClock(&versions, 200L));
        prior.setVersion(new VectorClock(&versions, 100L));
        later.setVersion(new VectorClock(&versions, 300L));
        much_later.setVersion(new VectorClock(&versions, 400L));

    }

    std::list<VersionedValue> items;
    TimeBasedInconsistencyResolver r;

    VersionedValue later;
    VersionedValue prior;
    VersionedValue current;
    VersionedValue much_later;
};

BOOST_FIXTURE_TEST_SUITE(TimeBasedInconsistencyResolver_test, TimeBasedFixture)

BOOST_AUTO_TEST_CASE( empty_list_test ) {
    r.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 0, "Error resolving empty list");
}

BOOST_AUTO_TEST_CASE( duplicates_test ) {
    items.push_back(current);
    items.push_back(current);
    r.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 1, "Duplicates should resolve to one item");
}

BOOST_AUTO_TEST_CASE( normal_test ) {
    items.push_back(prior);
    items.push_back(current);
    items.push_back(later);
    r.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 1, 
                        "Should have one item in final result:" <<
                        items.size() << " items");
    BOOST_CHECK_MESSAGE(items.front().getVersion() == later.getVersion(),
                        "'later' element should be returned element");
}

BOOST_AUTO_TEST_SUITE_END()
