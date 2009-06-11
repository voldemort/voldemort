/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file Version.h
 * @brief Interface definition file for Version
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

#ifndef VERSION_H
#define VERSION_H

namespace Voldemort {

/** 
 * An interface that allows us to determine if a given version
 * happened before or after another version.
 */
class Version
{
 public:
    virtual ~Version() { }

    /**
     * Possible version comparison values
     */
    enum Occurred {
        /** version 1 is after version 2 */
        AFTER,
        /** version 1 is before version 2 */
        BEFORE,
        /** version 1 and 2 are concurrent */
        CONCURRENTLY,
        /** version 1 and 2 are equal */
        EQUAL
    };

    /**
     * Virtual copy constructor allocates a new Version object
     * containing the same information as this one.
     */
    virtual Version* copy() { throw "Pure abstract class"; }

    /**
     * Return whether or not the given version preceeded this one,
     * succeeded it, or is concurrant with it
     *
     * @param v The other version
     * @return one of the Occurred values
     */
    virtual Occurred compare(Version* v) { throw "Pure abstract class"; }
};

} /* namespace Voldemort */

#endif // VERSION_H
