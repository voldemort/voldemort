/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for ProtocolBuffersRequestFormat class.
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

#include <utility>
#include <voldemort/VoldemortException.h>
#include "ProtocolBuffersRequestFormat.h"
#include "voldemort-client.pb.h"

namespace Voldemort {

ProtocolBuffersRequestFormat::ProtocolBuffersRequestFormat() {
    GOOGLE_PROTOBUF_VERIFY_VERSION;
}

ProtocolBuffersRequestFormat::~ProtocolBuffersRequestFormat() {

}

static void writeVectorClock(voldemort::VectorClock* vvc, VectorClock* vc) {
    std::list<std::pair<short, uint64_t> >::const_iterator it;
    std::list<std::pair<short, uint64_t> >* entries = vc->getEntries();

    for (it = entries->begin(); it != entries->end(); ++it) {
        voldemort::ClockEntry* entry = vvc->add_entries();
        entry->set_node_id((*it).first);
        entry->set_version((*it).second);
    }
}

static VectorClock* readVectorClock(voldemort::VectorClock* vvc) {
    std::list<std::pair<short, uint64_t> > entries;
    voldemort::ClockEntry** ventries = vvc->mutable_entries()->mutable_data();

    for (int i = 0; i < vvc->entries_size(); i++) {
        entries.push_back(std::make_pair((short)ventries[i]->node_id(),
                                         (uint64_t)ventries[i]->version()));
    }
    return new VectorClock(&entries, (uint64_t)vvc->timestamp());
}

static void throwException(const voldemort::Error& error) {
    /* XXX - TODO map error types to various exception objects derived
       from VoldemortException */
    throw VoldemortException(error.error_message());
}

void ProtocolBuffersRequestFormat::writeGetRequest(std::ostream* outputStream,
                                                   std::string* storeName,
                                                   std::string* key,
                                                   bool shouldReroute) {
    voldemort::VoldemortRequest req;
    req.set_type(voldemort::GET);
    req.set_store(*storeName);
    req.set_should_route(shouldReroute);
    req.mutable_get()->set_key(*key);

    req.SerializeToOstream(outputStream);
}

std::list<VersionedValue> * 
ProtocolBuffersRequestFormat::readGetResponse(std::istream* inputStream) {
    voldemort::GetResponse res;
    res.ParseFromIstream(inputStream);
    if (res.has_error()) {
        throwException(res.error());
    }
    std::list<VersionedValue>* responseList = 
        new std::list<VersionedValue>();
    voldemort::Versioned** versioned = res.mutable_versioned()->mutable_data();
    for (int i = 0; i < res.versioned_size(); i++) {
        std::string* val = new std::string(versioned[i]->value());
        VectorClock* clock = readVectorClock(versioned[i]->mutable_version());
        responseList->push_back(VersionedValue(val, clock));
    }

    return responseList;
}

void ProtocolBuffersRequestFormat::writeGetAllRequest(std::ostream* outputStream,
                                                      std::string* storeName,
                                                      std::list<std::string*>* keys,
                                                      bool shouldReroute) {
    throw "not implemented";
}

void ProtocolBuffersRequestFormat::writePutRequest(std::ostream* outputStream,
                                                   std::string* storeName,
                                                   std::string* key,
                                                   std::string* value,
                                                   VectorClock* version,
                                                   bool shouldReroute) {
    voldemort::VoldemortRequest req;
    req.set_type(voldemort::PUT);
    req.set_store(*storeName);
    req.set_should_route(shouldReroute);

    voldemort::PutRequest* preq = req.mutable_put();
    preq->set_key(*key);

    voldemort::Versioned* vers = preq->mutable_versioned();
    vers->set_value(*value);
    writeVectorClock(vers->mutable_version(), version);

    req.SerializeToOstream(outputStream);
}

void ProtocolBuffersRequestFormat::readPutResponse(std::istream* inputStream) {
    voldemort::PutResponse res;
    res.ParseFromIstream(inputStream);
    if (res.has_error()) {
        throwException(res.error());
    }
}

void ProtocolBuffersRequestFormat::writeDeleteRequest(std::ostream* outputStream,
                                                      std::string* storeName,
                                                      std::string* key,
                                                      VectorClock* version,
                                                      bool shouldReroute) {
    voldemort::VoldemortRequest req;
    req.set_type(voldemort::DELETE);
    req.set_store(*storeName);
    req.set_should_route(shouldReroute);

    voldemort::DeleteRequest* dreq = req.mutable_delete_();
    dreq->set_key(*key);
    writeVectorClock(dreq->mutable_version(), version);
    req.SerializeToOstream(outputStream);
}

bool ProtocolBuffersRequestFormat::readDeleteResponse(std::istream* inputStream) {
    voldemort::DeleteResponse res;
    res.ParseFromIstream(inputStream);
    if (res.has_error()) {
        throwException(res.error());
    }
    return res.success();
}

} /* namespace Voldemort */
