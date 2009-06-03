/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file StoreClient.h
 * @brief Interface definition file for StoreClient
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

#ifndef STORECLIENT_H
#define STORECLIENT_H

#include <string>
#include <voldemort/VersionedValue.h>
#include <voldemort/ObsoleteVersionException.h>

namespace Voldemort {

/**
 * The user-facing interface to a Voldemort store. Gives basic
 * put/get/delete plus helper functions.
 */
class StoreClient
{
public:
    /** 
     * Get the value associated with the given key or null if there is
     * no value associated with this key.  
     *
     * @param key The key to retrieve
     * @return The return value.  May be null if no value associated
     * with this key.  Returned memory is owned by the caller and must
     * be freed.
     */
    virtual std::string* getValue(std::string* key) = 0;

    /** 
     * Get the value associated with the given key or null if there is
     * no value associated with this key.
     *
     * @param key The key to retrieve
     * @param defaultValue the default value 
     * @return The return value.  May be null if no value associated
     * with this key.
     */
    virtual std::string* getValue(std::string* key,
                                  std::string* defaultValue) = 0;

    /**
     * Get the versioned value associated with the given key or null
     * if no value is associated with the key.
     *
     * @param key the key to get
     * @return the versioned value associated with the key. May be
     * null if no value associated with this key.
     */
    virtual VersionedValue* get(std::string* key) = 0;

    /**
     * Get the versioned value associated with the given key or the
     * defaultValue if no value is associated with the key.
     *
     * @param key the key to get
     * @param defaultValue The default value to use if no value
     * associated with key
     * @return The versioned value, or the defaultValue if no value is
     * stored for this key.
     */
    virtual VersionedValue* get(std::string* key,
                                VersionedValue* defaultValue) = 0;

    /**
     * Associates the given value to the key, clobbering any existing
     * values stored for the key.
     *
     * @param key The key to store
     * @param value The value to store
     */
    virtual void put(std::string* key, std::string* value) = 0;

    /**
     * Put the given Versioned value into the store for the given key
     * if the version is greater to or concurrent with existing
     * values. Throw an ObsoleteVersionException otherwise.
     *
     * @param key The key to store
     * @param value the versioned value to store
     */
    virtual void put(std::string* key, VersionedValue* value) 
        throw(ObsoleteVersionException) = 0;

    /**
     * Put the versioned value to the key, ignoring any
     * ObsoleteVersionException that may be thrown
     *
     * @param key The key to store
     * @param value the versioned value to store.
     */
    virtual void putifNotObsolete(std::string* key, VersionedValue* value) = 0;

    /**
     * Delete any version of the given key which equal to or less than
     * the current versions
     *
     * @param key the key to delete
     * @return true if anything is deleted
     */
    virtual bool deleteKey(std::string* key) = 0;

    /**
     * Delete the specified version and any prior versions of the
     * given key
     *
     * @param key the key to delete
     * @param version The version of the key 
     * @return true if anything is deleted
     */
    virtual bool deleteKey(std::string* key, Version* version) = 0;
   
};

} /* namespace Voldemort */

#endif /* STORECLIENT_H */
