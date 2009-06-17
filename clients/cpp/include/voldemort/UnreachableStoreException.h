/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file UnreachableStoreException.h
 * @brief Interface definition file for UnreachableStoreException
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

#ifndef UNREACHABLESTOREEXCEPTION_H
#define UNREACHABLESTOREEXCEPTION_H

#include <voldemort/VoldemortException.h>

using namespace std;

namespace Voldemort {

/**
 * Indicates that the given store cannot be reached (say, due to
 * network failure).
 */ 
class UnreachableStoreException: public VoldemortException
{
public:
    /**
     * Create a new VoldemortException with the provided message 
     *
     * @param message The error message
     */
    UnreachableStoreException(const std::string& message) :
        VoldemortException(message) { }
};

} // namespace Voldemort

#endif // UNREACHABLESTOREEXCEPTION_H

