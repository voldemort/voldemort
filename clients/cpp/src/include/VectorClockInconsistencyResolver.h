/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file VectorClockInconsistencyResolver.h
 * @brief Interface definition file for VectorClockInconsistencyResolver
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

#ifndef VOLDEMORT_VECTORCLOCKINCONSISTENCYRESOLVER_H
#define VOLDEMORT_VECTORCLOCKINCONSISTENCYRESOLVER_H

#include <voldemort/InconsistencyResolver.h>

#include <list>
#include <string>

namespace Voldemort {

/**
 * An inconsistency resolver that uses the object VectorClocks when
 * possible but will leave unresolved conflicts if there are
 * concurrent versions in the vector clocks.
 */
class VectorClockInconsistencyResolver : public InconsistencyResolver
{
public:
    virtual ~VectorClockInconsistencyResolver() { }

    virtual void resolveConflicts(std::list<VersionedValue>* items) const;

};

} /* namespace Voldemort */

#endif/*VOLDEMORT_VECTORCLOCKINCONSISTENCYRESOLVER_H*/
