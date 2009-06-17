/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file StoreOperationFailureException.h
 * @brief Interface definition file for StoreOperationFailureException
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

#ifndef STOREOPERATIONFAILUREEXCEPTION_H
#define STOREOPERATIONFAILUREEXCEPTION_H

#include <voldemort/VoldemortException.h>

using namespace std;

namespace Voldemort {

/**
 * Thrown to indicate the failure of some store operation (put, get,
 * delete)
 */ 
class StoreOperationFailureException: public VoldemortException
{
public:
    /**
     * Create a new VoldemortException with the provided message 
     *
     * @param message The error message
     */
    StoreOperationFailureException(const std::string& message) :
        VoldemortException(message) { }
};

} // namespace Voldemort

#endif // STOREOPERATIONFAILUREEXCEPTION_H

