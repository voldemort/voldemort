/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file Store.h
 * @brief Interface definition file for Store
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

#ifndef STORE_H
#define STORE_H

#include <voldemort/VersionedValue.h>

#include <list>
#include <string>

namespace Voldemort {

/**
 * The basic interface used for storage.
 */
class Store
{
public:
    virtual ~Store() { }

    /** 
     * Get the values associated with the given key or NULL if no
     * values to retrieve.
     *
     * @param key The key to retrieve
     * @return A newly-allocated list of values or NULL if no values
     * to retrieve.
     */
    virtual std::list<VersionedValue>* get(const std::string& key) = 0;

    /**
     * Associate the value with the key and version in this store.
     *
     * @param key The key to store
     * @param value The value to store
     */
    virtual void put(const std::string& key,
                     const VersionedValue& value) = 0;

    /**
     * Delete all entries prior to the given version
     * 
     * @param key The key to delete
     * @param version the version to use
     * @return True if anything was deleted
     */
    virtual bool deleteKey(const std::string& key,
                           const Version& version) = 0;

    /**
     * Get the name for this store.  The memory is owned by the Store
     * object and should not be freed by the caller.
     * 
     * @returns the name of the store.
     */
    virtual const std::string* getName() = 0;

    /** 
     * Close the store
     */
    virtual void close() = 0;
   
};

} /* namespace Voldemort */

#endif /* STORE_H */
