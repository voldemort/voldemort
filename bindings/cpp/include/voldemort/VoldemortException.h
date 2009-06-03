/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file VoldemortException.h
 * @brief Interface definition file for VoldemortException
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

#ifndef VOLDEMORTEXCEPTION_H
#define VOLDEMORTEXCEPTION_H

#include <stdexcept>
#include <string>

namespace Voldemort {

/**
 * Base exception that all other Voldemort exceptions extend.
 */ 
class VoldemortException: public std::runtime_error
{
public:
    /**
     * Create a new VoldemortException with the provided message 
     *
     * @param message The error message
     */
    VoldemortException(const std::string& message) :
        runtime_error(message) { }
};

} // namespace Voldemort

#endif // VOLDEMORTEXCEPTION_H

