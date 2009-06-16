/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for DefaultStoreClient class.
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

#include <voldemort/InconsistentDataException.h>
#include <voldemort/InvalidMetadataException.h>
#include "DefaultStoreClient.h"
#include "VectorClock.h"

#include <iostream>
#include <list>

namespace Voldemort {

using namespace boost;
using namespace std;

static const int METADATA_REFRESH_ATTEMPTS = 3;

DefaultStoreClient::DefaultStoreClient(shared_ptr<Store>& store,
                                       shared_ptr<ClientConfig>& config) 
    : config_(config), store_(store), curValue_() {

}

DefaultStoreClient::~DefaultStoreClient() {

}

void DefaultStoreClient::reinit() {
    /* XXX - TODO */
    throw VoldemortException("Not implemented");
}

const std::string* DefaultStoreClient::getValue(const std::string* key) {
    return getValue(key, NULL);
}

const std::string* DefaultStoreClient::getValue(const std::string* key,
                                                const std::string* defaultValue) {
    const VersionedValue* vv = get(key, NULL);
    if (vv == NULL)
        return defaultValue;
    else
        return vv->getValue();
}

const VersionedValue* DefaultStoreClient::get(const std::string* key) {
    return get(key, NULL);
}

const VersionedValue* DefaultStoreClient::get(const std::string* key,
                                              const VersionedValue* defaultValue) {
    for (int attempts = 0; attempts < METADATA_REFRESH_ATTEMPTS; attempts++) {
        try {
            auto_ptr<list<VersionedValue> > items(store_->get(*key));
            if (items->size() == 0) {
                if (!defaultValue)
                    return NULL;
                curValue_ = *defaultValue;
            }
            else if (items->size() == 1)
                curValue_ = items->front();
            else
                throw InconsistentDataException("Unresolved versions returned from get(" +
                                                *key + ")");

            return &curValue_;
        } catch (InvalidMetadataException& e) {
            reinit();
        }
    }
    throw InvalidMetadataException("Exceeded maximum metadata refresh attempts");
}

void DefaultStoreClient::put(const std::string* key, 
                             const std::string* value) {
    const VersionedValue* vv = get(key);
    if (vv == NULL) {
        std::string* valuec = NULL;
        Version* version = NULL;
        try {
            version = new VectorClock();
            valuec = new std::string(*value);
            curValue_.setVersion(version);
            version = NULL;
            curValue_.setValue(valuec);
            valuec = NULL;
        } catch (...) {
            if (version) delete version;
            if (valuec) delete valuec;
            throw;
        }
    } else {
        curValue_ = *vv;
        curValue_.setValue(new std::string(*value));
    }
    put(key, &curValue_);
}

void DefaultStoreClient::put(const std::string* key, 
                             const VersionedValue* value) {
    for (int attempts = 0; attempts < METADATA_REFRESH_ATTEMPTS; attempts++) {
        try {
            store_->put(*key, *value);
            return;
        } catch (InvalidMetadataException& e) {
            reinit();
        }
    }
    throw InvalidMetadataException("Exceeded maximum metadata refresh attempts");
}

bool DefaultStoreClient::putifNotObsolete(const std::string* key,
                                          const VersionedValue* value) {
    try {
        put(key, value);
        return true;
    } catch (ObsoleteVersionException& e) {
        return false;
    }
}

bool DefaultStoreClient::deleteKey(const std::string* key) {
    const VersionedValue* vv = get(key);
    if (vv == NULL)
        return false;
    return deleteKey(key, vv->getVersion());
}

bool DefaultStoreClient::deleteKey(const std::string* key, 
                                   const Version* version) {
    for (int attempts = 0; attempts < METADATA_REFRESH_ATTEMPTS; attempts++) {
        try {
            return store_->deleteKey(*key, *version);
        } catch (InvalidMetadataException& e) {
            reinit();
        }
    }
    throw InvalidMetadataException("Exceeded maximum metadata refresh attempts");

}

} /* namespace Voldemort */
