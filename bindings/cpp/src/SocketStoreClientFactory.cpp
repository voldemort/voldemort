/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for SocketStoreClientFactory class.
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

#include <voldemort/SocketStoreClientFactory.h>
#include "SocketStore.h"
#include <iostream>

namespace Voldemort {

class SocketStoreClientFactoryImpl {
public:
    SocketStoreClientFactoryImpl(ClientConfig& conf);

    ClientConfig config;
    //SocketPool* socketPool;
    //ThreadPool* threadPool;
};

SocketStoreClientFactoryImpl::SocketStoreClientFactoryImpl(ClientConfig& conf) 
    : config(conf) {

}

SocketStoreClientFactory::SocketStoreClientFactory(ClientConfig& conf) {
    pimpl_ = new SocketStoreClientFactoryImpl(conf);

}

SocketStoreClientFactory::~SocketStoreClientFactory() {
    if (pimpl_) 
        delete pimpl_;
}

StoreClient* SocketStoreClientFactory::getStoreClient(std::string& storeName) {

}

Store* SocketStoreClientFactory::getRawStore(std::string& storeName) {
    std::string host("localhost");
    return new SocketStore(storeName,
                           host,
                           6666,
                           RequestFormat::VOLDEMORT);
}

} /* namespace Voldemort */
