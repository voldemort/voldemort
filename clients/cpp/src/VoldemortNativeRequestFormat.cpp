/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for VoldemortNativeRequestFormat class.
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
#include <memory>
#include <voldemort/VoldemortException.h>
#include "VoldemortNativeRequestFormat.h"
#include <arpa/inet.h>
#include <sys/param.h>

namespace Voldemort {

VoldemortNativeRequestFormat::VoldemortNativeRequestFormat() {

}

VoldemortNativeRequestFormat::~VoldemortNativeRequestFormat() {

}

static const char GET_OP_CODE = 1;
static const char PUT_OP_CODE = 2;
static const char DELETE_OP_CODE = 3;
static const char GET_ALL_OP_CODE = 4;
static const char GET_PARTITION_AS_STREAM_OP_CODE = 4;
static const char PUT_ENTRIES_AS_STREAM_OP_CODE = 5;
static const char UPDATE_METADATA_OP_CODE = 6;
static const char SERVER_STATE_CHANGE_OP_CODE = 8;
static const char REDIRECT_GET_OP_CODE = 9;

#define READ_INT(inputStream, val)              \
    inputStream->read((char*)&val, 4);          \
    val = ntohl(val);
#define READ_SHORT(inputStream, val)              \
    inputStream->read((char*)&val, 2);          \
    val = ntohs(val);


static std::string* readUTF(std::istream* inputStream) {
    uint16_t strLen, red;
    char buffer[1024];
    std::string* result = new std::string;

    try {
        inputStream->read((char*)&strLen, 2);
        strLen = ntohs(strLen);
        result->reserve(strLen + 1);

        for (red = 0; red < strLen; red += sizeof(buffer)) {
            size_t toread = sizeof(buffer);
            if (toread > (size_t)(strLen - red)) toread = strLen - red;

            inputStream->read(buffer, toread);
            result->append(buffer, toread);
        }

        return result;
    } catch (...) {
        if (result) delete result;
        throw;
    }
}

static std::string* readBytes(std::istream* inputStream, int bytesToRead) {
    char buffer[1024];
    int red;
    std::string* result = new std::string;

    try {
        result->reserve(bytesToRead);

        for (red = 0; red < bytesToRead; red += sizeof(buffer)) {
            size_t toread = sizeof(buffer);
            if (toread > (size_t)(bytesToRead - red)) toread = bytesToRead - red;

            inputStream->read(buffer, toread);
            result->append(buffer, toread);
        }

        return result;
    } catch (...) {
        if (result) delete result;
        throw;
    }
}

static void checkException(std::istream* inputStream) {
    uint16_t retCode;
    READ_SHORT(inputStream, retCode);
    if (retCode != 0) {
        retCode = ntohs(retCode);
        std::auto_ptr<std::string> error(readUTF(inputStream));
        throw VoldemortException(error->c_str());
    }
}

static uint64_t readUINT64(std::istream* inputStream, int bytesToRead) {
    union {
        uint64_t v64;
        uint32_t v32[2];
        char c[8];
    } value;
    value.v64 = 0;
    for (int j = 0; j < bytesToRead; j++) {
        value.c[j+(8-bytesToRead)] = (char)inputStream->get();
    }
#if __BYTE_ORDER == __LITTLE_ENDIAN
    {
        uint32_t t = value.v32[0];
        value.v32[0] = ntohl(value.v32[1]);
        value.v32[1] = ntohl(t);
    }
#endif
    return value.v64;
}

static VectorClock* readVectorClock(std::istream* inputStream,
                                    int& bytesRead) {
    std::list<std::pair<short, uint64_t> > entries;

    bytesRead = 0;
    uint16_t numEntries;
    READ_SHORT(inputStream, numEntries);
    bytesRead += 2;

    int versionSize = inputStream->get();
    bytesRead += 1;
    
    for (uint32_t i = 0; i < numEntries; i++) {
        short nodeId;

        READ_SHORT(inputStream, nodeId);
        bytesRead += 2;

        uint64_t version = readUINT64(inputStream, versionSize);
        bytesRead += versionSize;
        entries.push_back(std::make_pair(nodeId, version));
    }

    uint64_t timestamp = readUINT64(inputStream, 8);
    bytesRead += 8;
    return new VectorClock(&entries, timestamp);
}

static std::list<VersionedValue> * readResults(std::istream* inputStream) {
    std::list<VersionedValue>* responseList = 
        new std::list<VersionedValue>();

    try {
        uint32_t resultSize;
        READ_INT(inputStream, resultSize);

        for (uint32_t i = 0; i < resultSize; i++) {
            uint32_t valueSize;
            READ_INT(inputStream, valueSize);
            VectorClock* vc = NULL;
            std::string* value = NULL;
            int vcsize = 0;
            try {
                vc = readVectorClock(inputStream, vcsize);
                value = readBytes(inputStream, valueSize - vcsize);
            } catch (...) {
                if (vc) delete vc;
                if (value) delete value;
                throw;
            }
            VersionedValue vv(value, vc);
            responseList->push_back(vv);
        }
        return responseList;

    } catch (...) {
        if (responseList) delete responseList;
        throw;
    }
}

void VoldemortNativeRequestFormat::writeGetRequest(std::ostream* outputStream,
                                                   const std::string* storeName,
                                                   const std::string* key,
                                                   bool shouldReroute) {
    uint32_t keyLen = htonl(key->length());
    uint16_t storeNameLen = htons(storeName->length());
    outputStream->put(GET_OP_CODE);
    outputStream->write((char*)&storeNameLen, 2);
    outputStream->write(storeName->c_str(), storeName->length());
    outputStream->put((char)shouldReroute);
    outputStream->write((char*)&keyLen, 4);
    outputStream->write(key->c_str(), key->length());
}

std::list<VersionedValue> * 
VoldemortNativeRequestFormat::readGetResponse(std::istream* inputStream) {
    checkException(inputStream);
    return readResults(inputStream);
}

void VoldemortNativeRequestFormat::writeGetAllRequest(std::ostream* outputStream,
                                                      const std::string* storeName,
                                                      std::list<const std::string*>* keys,
                                                      bool shouldReroute) {
    throw VoldemortException("Not implemented");
}

void VoldemortNativeRequestFormat::writePutRequest(std::ostream* outputStream,
                                                   const std::string* storeName,
                                                   const std::string* key,
                                                   const std::string* value,
                                                   const VectorClock* version,
                                                   bool shouldReroute) {
    throw VoldemortException("Not implemented");
}

void VoldemortNativeRequestFormat::readPutResponse(std::istream* inputStream) {
    throw VoldemortException("Not implemented");
}

void VoldemortNativeRequestFormat::writeDeleteRequest(std::ostream* outputStream,
                                                      const std::string* storeName,
                                                      const std::string* key,
                                                      const VectorClock* version,
                                                      bool shouldReroute) {
    throw VoldemortException("Not implemented");
}

bool VoldemortNativeRequestFormat::readDeleteResponse(std::istream* inputStream) {
    throw VoldemortException("Not implemented");
}

const std::string& VoldemortNativeRequestFormat::getNegotiationString() {
    static const std::string VOLDEMORT_NEG_STRING("vp0");
    return VOLDEMORT_NEG_STRING;
}

} /* namespace Voldemort */
