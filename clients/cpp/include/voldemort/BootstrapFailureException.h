/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file BootstrapFailureException.h
 * @brief Interface definition file for BootstrapFailureException
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

#ifndef VOLDEMORT_BOOTSTRAPFAILUREEXCEPTION_H
#define VOLDEMORT_BOOTSTRAPFAILUREEXCEPTION_H

#include <voldemort/VoldemortException.h>

using namespace std;

namespace Voldemort {

/**
 * The exception thrown when bootstrapping the store from the cluster
 * fails (e.g. because none of the given nodes could be connected
 * to). This is generally an unrecoverable failure.
 */ 
class BootstrapFailureException: public VoldemortException
{
public:
    /**
     * Create a new VoldemortException with the provided message 
     *
     * @param message The error message
     */
    BootstrapFailureException(const std::string& message) :
        VoldemortException(message) { }
};

} // namespace Voldemort

#endif/*VOLDEMORT_BOOTSTRAPFAILUREEXCEPTION_H*/
