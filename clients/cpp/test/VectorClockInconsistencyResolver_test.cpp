/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Test suite for VectorClockInconsistencyResolver class.
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
#include "VectorClockInconsistencyResolver.h"
using namespace Voldemort;
using namespace std;

#include <boost/test/unit_test.hpp>

struct VectorClockFixture {
    VectorClockFixture() {
        std::list<std::pair<short, uint64_t> > versions;
        earlier.setVersion(new VectorClock(&versions, 0L));

        versions.push_back(make_pair((short)1, 2));
        versions.push_back(make_pair((short)2, 1));
        versions.push_back(make_pair((short)3, 1));
        current.setVersion(new VectorClock(&versions, 0L));

        versions.clear();
        versions.push_back(make_pair((short)1, 1));
        versions.push_back(make_pair((short)2, 1));
        versions.push_back(make_pair((short)3, 1));
        prior.setVersion(new VectorClock(&versions, 0L));

        versions.clear();
        versions.push_back(make_pair((short)1, 1));
        versions.push_back(make_pair((short)2, 1));
        versions.push_back(make_pair((short)3, 2));
        concurrent.setVersion(new VectorClock(&versions, 0L));

        versions.clear();
        versions.push_back(make_pair((short)1, 1));
        versions.push_back(make_pair((short)2, 1));
        versions.push_back(make_pair((short)3, 1));
        versions.push_back(make_pair((short)4, 1));
        concurrent2.setVersion(new VectorClock(&versions, 0L));

        versions.clear();
        versions.push_back(make_pair((short)1, 2));
        versions.push_back(make_pair((short)2, 2));
        versions.push_back(make_pair((short)3, 1));
        later.setVersion(new VectorClock(&versions, 0L));

        versions.clear();
        versions.push_back(make_pair((short)1, 3));
        versions.push_back(make_pair((short)2, 3));
        versions.push_back(make_pair((short)3, 3));
        much_later.setVersion(new VectorClock(&versions, 0L));
    }

    std::list<VersionedValue> items;
    VectorClockInconsistencyResolver v;

    VersionedValue later;
    VersionedValue prior;
    VersionedValue current;
    VersionedValue concurrent;
    VersionedValue concurrent2;
    VersionedValue much_later;
    VersionedValue earlier;
};

BOOST_FIXTURE_TEST_SUITE(VectorClockInconsistencyResolver_test, VectorClockFixture)

BOOST_AUTO_TEST_CASE( empty_list_test ) {
    v.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 0, "Error resolving empty list");
}

BOOST_AUTO_TEST_CASE( duplicates_resolve_test ) {
    items.push_back(concurrent);
    items.push_back(current);
    items.push_back(concurrent);
    items.push_back(current);
    items.push_back(concurrent);
    items.push_back(current);
    v.resolveConflicts(&items);

    BOOST_CHECK_MESSAGE(items.size() == 2, 
                        "Duplicates should resolve away: " 
                        << items.size() << " items remaining");
}

BOOST_AUTO_TEST_CASE( earlier_test ) {
    items.push_back(concurrent);
    items.push_back(current);
    items.push_back(current);
    items.push_back(concurrent);
    items.push_back(current);
    items.push_back(earlier);
    v.resolveConflicts(&items);

    BOOST_CHECK_MESSAGE(items.size() == 2, 
                        "Should end up with 2 items left: " 
                        << items.size() << " items remaining");
}

BOOST_AUTO_TEST_CASE( much_later_resolve_test ) {
    items.push_back(concurrent);
    items.push_back(current);
    items.push_back(current);
    items.push_back(concurrent);
    items.push_back(current);
    items.push_back(much_later);
    v.resolveConflicts(&items);

    BOOST_CHECK_MESSAGE(items.size() == 1, 
                        "Should end up with one items left");
}

BOOST_AUTO_TEST_CASE( normal_resolve_test ) {
    items.push_back(current);
    items.push_back(prior);
    items.push_back(later);
    v.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 1, "Should return just one element");
    BOOST_CHECK_MESSAGE(items.front().getVersion() == later.getVersion(),
                        "'later' element should be sole returned element");
}

BOOST_AUTO_TEST_CASE( normal_resolve_test2 ) {
    items.push_back(prior);
    items.push_back(current);
    items.push_back(later);
    v.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 1, "Should return just one element");
    BOOST_CHECK_MESSAGE(items.front().getVersion() == later.getVersion(),
                        "'later' element should be returned element");
}

BOOST_AUTO_TEST_CASE( normal_resolve_test3 ) {
    items.clear();
    items.push_back(later);
    items.push_back(current);
    items.push_back(prior);
    v.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 1, "Should return just one element");
    BOOST_CHECK_MESSAGE(items.front().getVersion() == later.getVersion(),
                        "'later' element should be returned element");
}

BOOST_AUTO_TEST_CASE( normal_resolve_larger_concurrent ) {
    items.clear();
    items.push_back(concurrent);
    items.push_back(concurrent2);
    items.push_back(current);
    items.push_back(concurrent2);
    items.push_back(current);
    items.push_back(concurrent);
    items.push_back(current);
    v.resolveConflicts(&items);
    BOOST_CHECK_MESSAGE(items.size() == 3, "Should return three elements");
}

BOOST_AUTO_TEST_SUITE_END()
