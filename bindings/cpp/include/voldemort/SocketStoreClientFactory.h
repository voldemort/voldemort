/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file SocketStoreClientFactory.h
 * @brief Interface definition file for SocketStoreClientFactory
 */
/* Copyright (c) 2009 Webroot Software, Inc.
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

#ifndef SOCKETSTORECLIENTFACTORY_H
#define SOCKETSTORECLIENTFACTORY_H

#include <voldemort/StoreClientFactory.h>
#include <voldemort/ClientConfig.h>

namespace Voldemort {

/**
 * Implementation details
 */
class SocketStoreClientFactoryImpl;
    
/**
 * Represents a connection to a Voldemort cluster and can be used to
 * create @ref StoreClient instances to interact with individual
 * stores. The factory abstracts away any connection pools, thread
 * pools, or other details that will be shared by all the individual
 * @ref StoreClient
 */
class SocketStoreClientFactory: public StoreClientFactory
{
public:
    /**
     * Construct a new socket store client factory using the given
     * client config object.  The object is copied 
     */
    SocketStoreClientFactory(ClientConfig& conf);

    virtual ~SocketStoreClientFactory();

    virtual StoreClient* getStoreClient(std::string& storeName);

    virtual Store* getRawStore(std::string& storeName);

private:
    /** Internal implementation details */
    SocketStoreClientFactoryImpl* pimpl_;
};

} /* namespace Voldemort */

#endif /* SOCKETSTORECLIENTFACTORY_H */
