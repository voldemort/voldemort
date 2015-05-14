/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file InconsistencyResolver.h
 * @brief Interface definition file for InconsistencyResolver
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

#ifndef VOLDEMORT_INCONSISTENCYRESOLVER_H
#define VOLDEMORT_INCONSISTENCYRESOLVER_H

#include <voldemort/VersionedValue.h>

#include <list>
#include <string>

namespace Voldemort {

/**
 * A method for resolving inconsistent object values into a single
 * value. Applications can implement this to provide a method for
 * reconciling conflicts that cannot be resolved simply by the version
 * information.
 */
class InconsistencyResolver
{
public:
    virtual ~InconsistencyResolver() { }

    /**
     * Resolve conflicts in the given list of items and modify the
     * list to contain the resolved set of items.  Note that data in a
     * @ref VersionedValue is shared via reference counting on
     * copy/assignment, so copying it is an inexpensive operation.
     * 
     * @param items the list of items to resolve
     */
    virtual void resolveConflicts(std::list<VersionedValue>* items) const = 0;

};

} /* namespace Voldemort */

#endif /*VOLDEMORT_INCONSISTENCYRESOLVER_H */
