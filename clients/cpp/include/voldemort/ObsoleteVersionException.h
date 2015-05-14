/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file ObsoleteVersionException.h
 * @brief Interface definition file for ObsoleteVersionException
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

#ifndef VOLDEMORT_OBSOLETEVERSIONEXCEPTION_H
#define VOLDEMORT_OBSOLETEVERSIONEXCEPTION_H

#include <voldemort/VoldemortException.h>

using namespace std;

namespace Voldemort {

/**
 * An exception that indicates an attempt by the user to overwrite a
 * newer value for a given key with an older value for the same
 * key. This is a application-level error, and indicates the
 * application has attempted to write stale data.
 */ 
class ObsoleteVersionException: public VoldemortException
{
public:
    /**
     * Create a new VoldemortException with the provided message 
     *
     * @param message The error message
     */
    ObsoleteVersionException(const std::string& message) :
        VoldemortException(message) { }
};

} // namespace Voldemort

#endif/*VOLDEMORT_OBSOLETEVERSIONEXCEPTION_H*/
