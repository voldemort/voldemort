/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file VoldemortNativeRequestFormat.h
 * @brief Interface definition file for VoldemortNativeRequestFormat
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

#ifndef VOLDEMORT_VOLDEMORTNATIVEREQUESTFORMAT_H
#define VOLDEMORT_VOLDEMORTNATIVEREQUESTFORMAT_H

#include "RequestFormat.h"

#include <list>
#include <string>

namespace Voldemort {

/**
 * Implements the Voldemort native client protocol.  Note that only
 * the @ref writeGetRequest method is actually implemented.
 */
class VoldemortNativeRequestFormat: public RequestFormat
{
public:
    VoldemortNativeRequestFormat();
    virtual ~VoldemortNativeRequestFormat();

    // RequestFormat interface 
    virtual void writeGetRequest(std::ostream* outputStream,
                                 const std::string* storeName,
                                 const std::string* key,
                                 bool shouldReroute);
    virtual std::list<VersionedValue>* readGetResponse(std::istream* inputStream);
    virtual void writeGetAllRequest(std::ostream* outputStream,
                                    const std::string* storeName,
                                    std::list<const std::string*>* keys,
                                    bool shouldReroute);
    /* XXX - TODO */
    //virtual void readGetAllResponse(std::istream* inputStream);
    virtual void writePutRequest(std::ostream* outputStream,
                                 const std::string* storeName,
                                 const std::string* key,
                                 const std::string* value,
                                 const VectorClock* version,
                                 bool shouldReroute);
    virtual void readPutResponse(std::istream* inputStream);
    virtual void writeDeleteRequest(std::ostream* outputStream,
                                    const std::string* storeName,
                                    const std::string* key,
                                    const VectorClock* version,
                                    bool shouldReroute);
    virtual bool readDeleteResponse(std::istream* inputStream);
    virtual const std::string& getNegotiationString();
};

} /* namespace Voldemort */

#endif/*VOLDEMORT_VOLDEMORTNATIVEREQUESTFORMAT_H*/
