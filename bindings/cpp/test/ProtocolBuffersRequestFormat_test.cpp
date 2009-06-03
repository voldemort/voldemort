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

#include "ProtocolBuffersRequestFormat.h"
#include "voldemort-client.pb.h"

using namespace Voldemort;
using namespace std;

#include <boost/test/unit_test.hpp>
#include <sstream>

BOOST_AUTO_TEST_SUITE(ProtocolBuffersRequestFormat_test)

BOOST_AUTO_TEST_CASE( get_test ) {
    stringstream stream;
    ProtocolBuffersRequestFormat rf;
    string name("name");
    string key("key");

    rf.writeGetRequest(&stream,
                       &name,
                       &key,
                       true);

    voldemort::VoldemortRequest req;
    req.ParseFromIstream(&stream);

    BOOST_CHECK_MESSAGE(req.type() == voldemort::GET,
                        "Request type must be GET");
    BOOST_CHECK_MESSAGE(0 == name.compare(req.store()),
                        "Store name must be \"name\" not \"" << req.store() << "\"");
    BOOST_CHECK_MESSAGE(0 == key.compare(req.get().key()),
                        "Key must be \"key\" not \"" << req.get().key() << "\"");

}
BOOST_AUTO_TEST_SUITE_END()
