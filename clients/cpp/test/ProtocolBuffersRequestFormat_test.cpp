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
#include <google/protobuf/io/coded_stream.h>

BOOST_AUTO_TEST_SUITE(ProtocolBuffersRequestFormat_test)

#define READ_INT(inputStream, val)              \
    inputStream->read((char*)&val, 4);          \
    val = ntohl(val);

static void readMessageWithLength(std::istream* inputStream,
                                  google::protobuf::Message* message) {
    uint32_t mLen;
    IstreamInputStream zis(inputStream);
    CodedInputStream cis(&zis);
    cis.ReadVarint32(&mLen);
    cis.PushLimit(mLen);
    message->ParseFromCodedStream(&cis);
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
    BOOST_CHECK_MESSAGE(req.should_route(),
                        "Request should be routed");
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
        throw "failed to read request from input stream";
    }

    BOOST_CHECK_MESSAGE(res.versioned_size() == 1,
                       "readMessageWithLength Should return one versioned value");

    return NULL;
}

BOOST_AUTO_TEST_CASE( get_res_test ) {
    ProtocolBuffersRequestFormat rf;

    stringstream streamwithlen;
    streamwithlen.write("\x18\x0a\x16\x0a\x05\x77\x6f\x72\x6c\x64\x12\x0d\x0a\x04\x08\x00\x10\x01\x10\xf8\x9d\xe2\x88\x9b\x24", 0x18+1);
    streamwithlen << "Some more gibberish";

    auto_ptr<std::list<VersionedValue> > vv(rf.readGetResponse(&streamwithlen));
    BOOST_REQUIRE_MESSAGE(vv.get(),
                          "Get response should return a value");
    BOOST_CHECK_MESSAGE(vv->size() == 1,
                        "Should return one versioned value");

}

BOOST_AUTO_TEST_CASE( put_req_test ) {
    stringstream stream;
    ProtocolBuffersRequestFormat rf;
    string name("name");
    string key("key");
    string value("value");

    std::list<std::pair<short, uint64_t> > versions;
    versions.push_back(make_pair((short)1, 11));
    versions.push_back(make_pair((short)2, 12));
    VectorClock vc(&versions, 500L);

    rf.writePutRequest(&stream,
                       &name,
                       &key,
                       &value,
                       &vc,
                       true);

    voldemort::VoldemortRequest req;
    readMessageWithLength(&stream, &req);

    BOOST_REQUIRE_MESSAGE(req.type() == voldemort::PUT,
                        "Request type must be PUT");
    BOOST_CHECK_MESSAGE(req.should_route(),
                        "Request should be routed");
    BOOST_CHECK_MESSAGE(0 == name.compare(req.store()),
                        "Store name must be \"name\" not \"" 
                        << req.store() << "\"");
    BOOST_REQUIRE_MESSAGE(req.put().has_versioned(), 
                          "Request has no versioned object");
    BOOST_REQUIRE_MESSAGE(req.put().versioned().has_value(), 
                          "Request has versioned object has no value");
    BOOST_CHECK_MESSAGE(0 == value.compare(req.put().versioned().value()),
                        "Value must be \"value\" not \"" 
                        << req.put().versioned().value() << "\"");
    BOOST_REQUIRE_MESSAGE(req.put().versioned().has_version(), 
                          "Request has versioned object has no version");
    BOOST_REQUIRE_MESSAGE(req.put().versioned().version().has_timestamp(), 
                          "Request has version with no timestamp");
    BOOST_CHECK_MESSAGE(req.put().versioned().version().timestamp() == 500L, 
                          "Request has version with wrong timestamp");
    BOOST_REQUIRE_MESSAGE(req.put().versioned().version().entries_size() == 2, 
                          "Vector Clock length should be 2");
    for (int i = 0; i < 2; i++) {
        BOOST_REQUIRE_MESSAGE(req.put().versioned().version().entries(i).has_node_id(), 
                              "Vector Clock entry " << i << " has no node ID");
        BOOST_CHECK_MESSAGE((i+1) == req.put().versioned().version().entries(i).node_id(), 
                            "Vector Clock entry " << i << " has wrong node ID: " 
                            << req.put().versioned().version().entries(i).node_id());
        BOOST_REQUIRE_MESSAGE(req.put().versioned().version().entries(i).has_version(), 
                              "Vector Clock entry " << i << " has no version");
        BOOST_CHECK_MESSAGE((i+11) == req.put().versioned().version().entries(i).version(), 
                            "Vector Clock entry " << i << " has wrong version: "
                            << req.put().versioned().version().entries(i).version());
    }

}

BOOST_AUTO_TEST_CASE( delete_req_test ) {
    stringstream stream;
    ProtocolBuffersRequestFormat rf;
    string name("name");
    string key("key");

    std::list<std::pair<short, uint64_t> > versions;
    versions.push_back(make_pair((short)1, 11));
    versions.push_back(make_pair((short)2, 12));
    VectorClock vc(&versions, 500L);

    rf.writeDeleteRequest(&stream,
                          &name,
                          &key,
                          &vc,
                          true);

    voldemort::VoldemortRequest req;
    readMessageWithLength(&stream, &req);

    BOOST_REQUIRE_MESSAGE(req.type() == voldemort::DELETE,
                        "Request type must be DELETE");
    BOOST_CHECK_MESSAGE(req.should_route(),
                        "Request should be routed");
    BOOST_CHECK_MESSAGE(0 == name.compare(req.store()),
                        "Store name must be \"name\" not \"" << req.store() << "\"");
    BOOST_CHECK_MESSAGE(0 == key.compare(req.delete_().key()),
                        "Key must be \"key\" not \"" << req.get().key() << "\"");

    BOOST_REQUIRE_MESSAGE(req.delete_().has_version(), 
                          "Request has versioned object has no version");
    BOOST_REQUIRE_MESSAGE(req.delete_().version().has_timestamp(), 
                          "Request has version with no timestamp");
    BOOST_CHECK_MESSAGE(req.delete_().version().timestamp() == 500L, 
                          "Request has version with wrong timestamp");
    BOOST_REQUIRE_MESSAGE(req.delete_().version().entries_size() == 2, 
                          "Vector Clock length should be 2");
    for (int i = 0; i < 2; i++) {
        BOOST_REQUIRE_MESSAGE(req.delete_().version().entries(i).has_node_id(), 
                              "Vector Clock entry " << i << " has no node ID");
        BOOST_CHECK_MESSAGE((i+1) == req.delete_().version().entries(i).node_id(), 
                            "Vector Clock entry " << i << " has wrong node ID: " 
                            << req.delete_().version().entries(i).node_id());
        BOOST_REQUIRE_MESSAGE(req.delete_().version().entries(i).has_version(), 
                              "Vector Clock entry " << i << " has no version");
        BOOST_CHECK_MESSAGE((i+11) == req.delete_().version().entries(i).version(), 
                            "Vector Clock entry " << i << " has wrong version: "
                            << req.delete_().version().entries(i).version());
    }

}


BOOST_AUTO_TEST_SUITE_END()
