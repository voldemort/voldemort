/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file StoreClientFactory.h
 * @brief Interface definition file for StoreClientFactory
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

#ifndef STORECLIENTFACTORY_H
#define STORECLIENTFACTORY_H

#include <voldemort/StoreClient.h>
#include <voldemort/Store.h>
#include <voldemort/InconsistencyResolver.h>
#include <string>

#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;

/**
 * Represents a connection to a Voldemort cluster and can be used to
 * create @ref StoreClient instances to interact with individual
 * stores. The factory abstracts away any connection pools, thread
 * pools, or other details that will be shared by all the individual
 * @ref StoreClient
 */
class StoreClientFactory
{
public:
    virtual ~StoreClientFactory() { }

    /**
     * Get a @ref StoreClient for the given store.  Returns a newly
     * allocated object which is owned by the caller and must be
     * freed.  Use the default inconsistency resolution strategy.
     * 
     * @param storeName the name of the store
     * @return A fully-constructed @ref StoreClient
     * @see getStoreClient(std::string&, InconsistencyResolver*)
     */
    virtual StoreClient* getStoreClient(std::string& storeName) = 0;

    /**
     * Get a @ref StoreClient for the given store.  Returns a newly
     * allocated object which is owned by the caller and must be
     * freed.
     * 
     * @param storeName the name of the store
     * @param resolver a shared pointer to an @ref
     * InconsistencyResolver that should be used to resolve
     * inconsistencies.  May be NULL, in which case a default
     * time-based resolution scheme will be used.
     * @return A fully-constructed @ref StoreClient
     * @see getStoreClient(std::string&)
     */
    virtual StoreClient* getStoreClient(std::string& storeName,
                                        shared_ptr<InconsistencyResolver>& resolver) = 0;

    /**
     * Get the underlying store, not the public StoreClient interface.
     * Returns a newly allocated object which is owned by the caller
     * and must be freed.
     * 
     * @param storeName the name of the store
     * @param resolver a shared pointer to an @ref
     * InconsistencyResolver that should be used to resolve
     * inconsistencies.  May be NULL, in which case a default
     * time-based resolution scheme will be used.
     * @return the appropriate store
     */
    virtual Store* getRawStore(std::string& storeName,
                               shared_ptr<InconsistencyResolver>& resolver) = 0;
   
};

} /* namespace Voldemort */

#endif /* STORECLIENTFACTORY_H */
