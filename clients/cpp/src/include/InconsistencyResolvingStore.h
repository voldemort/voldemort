/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file InconsistencyResolvingStore.h
 * @brief Interface definition file for InconsistencyResolvingStore
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

#ifndef VOLDEMORT_INCONSISTENCYRESOLVINGSTORE_H
#define VOLDEMORT_INCONSISTENCYRESOLVINGSTORE_H

#include <voldemort/Store.h>
#include <voldemort/InconsistencyResolver.h>

#include <list>
#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;

/**
 * A Store that uses an InconsistencyResolver to eleminate some
 * duplicates
 */
class InconsistencyResolvingStore: public Store
{
public:
    /**
     * Construct a new InconsistencyResolvingStore object to connect
     * to the provided host.
     *
     * @param store the underlying store
     */
    InconsistencyResolvingStore(shared_ptr<Store>& store);
    virtual ~InconsistencyResolvingStore() { }

    /**
     * Add an inconsistency resolver to the chain.  Resolvers are
     * processed in the order they are added.
     *
     * @param resolver the resolver to add to the chain
     */
    void addResolver(shared_ptr<InconsistencyResolver>& resolver);

    // Store interface 
    virtual std::list<VersionedValue>* get(const std::string& key);
    virtual void put(const std::string& key,
                     const VersionedValue& value);
    virtual bool deleteKey(const std::string& key,
                           const Version& version);
    virtual const std::string* getName();
    virtual void close();

private:
    shared_ptr<Store> substore;
    std::list<shared_ptr<InconsistencyResolver> > chain;
};

} /* namespace Voldemort */

#endif/*VOLDEMORT_INCONSISTENCYRESOLVINGSTORE_H*/
