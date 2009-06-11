/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Test suite for VectorClock class.
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

#include "VectorClock.h"
using namespace Voldemort;
using namespace std;

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(VectorClock_test)

BOOST_AUTO_TEST_CASE( empty_test ) {
    std::list<std::pair<short, uint64_t> > versions;

    VectorClock emptyClock(&versions, 0L);
    BOOST_CHECK_MESSAGE(emptyClock.compare(&emptyClock) != Version::CONCURRENTLY,
                        "Empty clocks should not be concurrent");
}

BOOST_AUTO_TEST_CASE( identical_test ) {
    std::list<std::pair<short, uint64_t> > versions;
    versions.push_back(make_pair((short)1, 2));
    versions.push_back(make_pair((short)2, 1));

    VectorClock clock(&versions, 0L);
    BOOST_CHECK_MESSAGE(clock.compare(&clock) == Version::EQUAL,
                "Identical clocks should be equal");
}

BOOST_AUTO_TEST_CASE( single_addition_test ) {
    std::list<std::pair<short, uint64_t> > versions;
    versions.push_back(make_pair((short)1, 2));
    versions.push_back(make_pair((short)2, 1));
    VectorClock clock1(&versions, 0L);

    versions.push_back(make_pair((short)3, 1));
    VectorClock clock2(&versions, 0L);
    BOOST_CHECK_MESSAGE(clock1.compare(&clock2) == Version::BEFORE,
                "A clock should happen before an identical clock "
                "with a single additional event");
}

BOOST_AUTO_TEST_CASE( different_events_test ) {
    std::list<std::pair<short, uint64_t> > versions;

    versions.push_back(make_pair((short)1, 1));
    VectorClock clock3(&versions, 0L);

    versions.clear();
    versions.push_back(make_pair((short)2, 1));
    VectorClock clock4(&versions, 0L);

    BOOST_CHECK_MESSAGE(clock3.compare(&clock4) == Version::CONCURRENTLY,
                        "Clocks with different events should be concurrent.");

    versions.clear();
    versions.push_back(make_pair((short)1, 2));
    versions.push_back(make_pair((short)2, 1));
    VectorClock clock5(&versions, 0L);

    versions.clear();
    versions.push_back(make_pair((short)1, 2));
    versions.push_back(make_pair((short)3, 1));
    VectorClock clock6(&versions, 0L);

    BOOST_CHECK_MESSAGE(clock5.compare(&clock6) == Version::CONCURRENTLY,
                        "Clocks with different events should be concurrent.");
}
BOOST_AUTO_TEST_SUITE_END()
