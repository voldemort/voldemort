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

#include "DefaultStoreClient.h"
#include <iostream>
#include <boost/array.hpp>
#include <boost/asio.hpp>

namespace Voldemort {

DefaultStoreClient::DefaultStoreClient(std::string storeName,
                                       StoreClientFactory* storeFactory,
                                       int maxMetadataRefreshAttempts) 
    : maxMetadataRefreshAttempts_(maxMetadataRefreshAttempts),
      storeName_(storeName),
      storeFactory_(storeFactory) {

}

DefaultStoreClient::~DefaultStoreClient() {
    if (store_) {
        delete store_;
    }
}

std::string* DefaultStoreClient::getValue(std::string* key) {
    /* XXX - TODO */
}

std::string* DefaultStoreClient::getValue(std::string* key,
                      std::string* defaultValue) {
    /* XXX - TODO */
}

VersionedValue* DefaultStoreClient::get(std::string* key) {
    /* XXX - TODO */
}

VersionedValue* DefaultStoreClient::get(std::string* key,
                                        VersionedValue* defaultValue) {
    /* XXX - TODO */
}

void DefaultStoreClient::put(std::string* key, std::string* value) {
    /* XXX - TODO */
}

void DefaultStoreClient::put(std::string* key, VersionedValue* value) 
    throw(ObsoleteVersionException) {
    /* XXX - TODO */
}

void DefaultStoreClient::putifNotObsolete(std::string* key, VersionedValue* value) {
    /* XXX - TODO */
}

bool DefaultStoreClient::deleteKey(std::string* key) {
    /* XXX - TODO */
}

bool DefaultStoreClient::deleteKey(std::string* key, Version* version) {
    /* XXX - TODO */
}

} /* namespace Voldemort */
