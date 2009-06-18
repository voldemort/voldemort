/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file DefaultStoreClient.h
 * @brief Interface definition file for DefaultStoreClient
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

#ifndef DEFAULTSTORECLIENT_H
#define DEFAULTSTORECLIENT_H

#include <stdlib.h>
#include <string>
#include <voldemort/StoreClientFactory.h>
#include <voldemort/VersionedValue.h>
#include <voldemort/ObsoleteVersionException.h>
#include <voldemort/ClientConfig.h>

#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;

/**
 * The default @ref StoreClient implementation you get back from a
 * @ref StoreClientFactory
 */
class DefaultStoreClient: public StoreClient
{
public:
    /** 
     * Construct a default store client object
     * 
     * @param store The underlying store object
     * @param resolver The inconsistency resolver
     * @param config the @ref ClientConfig object
     * @param factory the store client factory that created us
     */
    DefaultStoreClient(shared_ptr<Store>& store,
                       shared_ptr<InconsistencyResolver>& resolver,
                       shared_ptr<ClientConfig>& config,
                       StoreClientFactory* factory);
    virtual ~DefaultStoreClient();

    /**
     * Reinitialize this store client to fetch fresh metadata
     */
    virtual void reinit();

    // StoreClient interface
    virtual const std::string* getValue(const std::string* key);
    virtual const std::string* getValue(const std::string* key,
                                        const std::string* defaultValue);
    virtual const VersionedValue* get(const std::string* key);
    virtual const VersionedValue* get(const std::string* key,
                                      const VersionedValue* defaultValue);
    virtual void put(const std::string* key, const std::string* value);
    virtual void put(const std::string* key, const VersionedValue* value);
    virtual bool putifNotObsolete(const std::string* key, const VersionedValue* value);
    virtual bool deleteKey(const std::string* key);
    virtual bool deleteKey(const std::string* key, const Version* version);
   
private:
    shared_ptr<ClientConfig> config_;
    shared_ptr<InconsistencyResolver> resolver_;
    shared_ptr<Store> store_;
    StoreClientFactory* factory_;

    VersionedValue curValue_;
};

} /* namespace Voldemort */

#endif /* DEFAULTSTORECLIENT_H */
