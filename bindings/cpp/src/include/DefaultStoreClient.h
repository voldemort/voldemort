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

namespace Voldemort {

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
     * @param storeName the name of the store
     * @param storeFactory a pointer to the store factor that created
     * us
     * @param maxMetadataRefreshAttempts the maximum number of times
     * to attempt to obtain metadata
     */
    DefaultStoreClient(std::string storeName,
                       StoreClientFactory* storeFactory,
                       int maxMetadataRefreshAttempts);
    ~DefaultStoreClient();

    // StoreClient interface
    virtual std::string* getValue(std::string* key);
    virtual std::string* getValue(std::string* key,
                                  std::string* defaultValue);
    virtual VersionedValue* get(std::string* key);
    virtual VersionedValue* get(std::string* key,
                                VersionedValue* defaultValue);
    virtual void put(std::string* key, std::string* value);
    virtual void put(std::string* key, VersionedValue* value) 
        throw(ObsoleteVersionException);
    virtual void putifNotObsolete(std::string* key, VersionedValue* value);
    virtual bool deleteKey(std::string* key);
    virtual bool deleteKey(std::string* key, Version* version);
   
private:
    int maxMetadataRefreshAttempts_;
    std::string storeName_;
    Store* store_;
    StoreClientFactory* storeFactory_;
};

} /* namespace Voldemort */

#endif /* DEFAULTSTORECLIENT_H */
