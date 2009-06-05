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
using namespace google::protobuf::io;

#include <boost/test/unit_test.hpp>
#include <iostream>
#include <sstream>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <arpa/inet.h>

BOOST_AUTO_TEST_SUITE(ProtocolBuffersRequestFormat_test)

#define READ_INT(inputStream, val)              \
    inputStream->read((char*)&val, 4);          \
    val = ntohl(val);

static void readMessageWithLength(std::istream* inputStream,
                                  google::protobuf::Message* message) {
    uint32_t mLen;
    READ_INT(inputStream, mLen);

    IstreamInputStream isis(inputStream);
    LimitingInputStream lis(&isis, (uint64_t)mLen);

    message->ParseFromZeroCopyStream(&lis);
}

BOOST_AUTO_TEST_CASE( get_req_test ) {
    stringstream stream;
    ProtocolBuffersRequestFormat rf;
    string name("name");
    string key("key");

    rf.writeGetRequest(&stream,
                       &name,
                       &key,
                       true);

    voldemort::VoldemortRequest req;
    readMessageWithLength(&stream, &req);

    BOOST_REQUIRE_MESSAGE(req.type() == voldemort::GET,
                        "Request type must be GET");
    BOOST_CHECK_MESSAGE(0 == name.compare(req.store()),
                        "Store name must be \"name\" not \"" << req.store() << "\"");
    BOOST_CHECK_MESSAGE(0 == key.compare(req.get().key()),
                        "Key must be \"key\" not \"" << req.get().key() << "\"");

}

std::list<VersionedValue> * 
readGetResponse(std::istream* inputStream) {
    voldemort::GetResponse res;
    readMessageWithLength(inputStream, &res);
    if (res.has_error()) {
        throw "suck";
    }

    BOOST_CHECK_MESSAGE(res.versioned_size() == 1,
                       "readMessageWithLength Should return one versioned value");

    return NULL;
}
BOOST_AUTO_TEST_CASE( get_res_test ) {
    ProtocolBuffersRequestFormat rf;

    stringstream streamwithlen;
    streamwithlen.write("\x00\x00\x00\x18\x0a\x16\x0a\x05\x77\x6f\x72\x6c\x64\x12\x0d\x0a\x04\x08\x00\x10\x01\x10\xf8\x9d\xe2\x88\x9b\x24", 0x18+4);
    streamwithlen << "Some more gibberish";

    auto_ptr<std::list<VersionedValue> > vv(rf.readGetResponse(&streamwithlen));
    BOOST_REQUIRE_MESSAGE(vv.get(),
                          "Get response should return a value");
    BOOST_CHECK_MESSAGE(vv->size() == 1,
                        "Should return one versioned value");

}

BOOST_AUTO_TEST_SUITE_END()
