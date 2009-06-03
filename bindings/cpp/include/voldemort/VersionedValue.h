/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file VersionedValue.h
 * @brief Interface definition file for VersionedValue
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

#ifndef VERSIONEDVALUE_H
#define VERSIONEDVALUE_H

#include <string>
#include <stdlib.h>
#include <voldemort/Version.h>

namespace Voldemort {

/**
 * Implementation details
 */
class VersionedValueImpl;

/** 
 * A wrapper for an value that adds a Version. 
 */
class VersionedValue
{
public:
    /**
     * Construct a VersionedValue object using the specified version
     * and value.  The VersionedValue object will take ownership of
     * these objects so they should not be freed by the caller.
     */
    VersionedValue(const std::string* value,
                   Version* version);
    ~VersionedValue();

    /**
     * Copy constructor.  The values contained will be shared between
     * both copies and only deleted when all copies are deleted.
     * 
     * @param v copy from this VersionedValue
     */
    VersionedValue(const VersionedValue& v);

    /**
     * Copy constructor.  The values contained will be shared between
     * both copies and only deleted when all copies are deleted.
     * 
     * @param r copy from this VersionedValue
     * @return Reference to the current object
     */
    VersionedValue& operator=(VersionedValue const & r);

    /**
     * Get the value stored in this object.  This memory is owned by
     * the VersionedValue object and should not be freed by the
     * caller.
     *
     * @return Pointer to the value for this object
     */
    const std::string* getValue() const;

    /**
     * Set the value for this object.  The object ownership is passed
     * to the VersionedValue object and should not be freed by the caller.
     *
     * @param val The value to set
     */
    void setValue(const std::string* val);

    /**
     * Get the version associated with this value.  This object is
     * owned by the VersionedValue object and should not be freed by
     * the caller.
     *
     * @return a pointer to a Version object.  
     */
    Version* getVersion() const;

    /**
     * Set the version for this object.  The object ownership is passed
     * to the VersionedValue object and should not be freed by the caller.
     *
     * @param version The version to set
     */
    void setVersion(Version* version);

private:
    /** Internal implementation details */
    VersionedValueImpl* pimpl_;
};

} /* namespace Voldemort */

#endif // VERSIONEDVALUE_H
