/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file RequestFormat.h
 * @brief Interface definition file for RequestFormat
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

#ifndef REQUESTFORMAT_H
#define REQUESTFORMAT_H

#include <voldemort/Store.h>
#include "VectorClock.h"

#include <stdlib.h>
#include <string>
#include <iostream>

namespace Voldemort {

/**
 * The client implementation of a socket store -- translates each request into a
 * network operation to be handled by the socket server on the other side.
 */
class RequestFormat
{
public:
    virtual ~RequestFormat() { }

    /**
     * The request format types that are possible.  Note that not all
     * of these will be necessarily implemented by the C++ client.
     */
    enum RequestFormatType {
        /** The Voldemort native protocol */
        VOLDEMORT,
        /** Protocol buffers */
        PROTOCOL_BUFFERS,
        /** Admin request handler protocol */
        ADMIN_HANDLER
    };

    /**
     * Write a Get request to the server
     *
     * @param outputStream output stream to which the request should
     * be written
     * @param storeName the name of the store
     * @param key the key to get
     * @param shouldReroute true if the server should reroute the
     * request
     */
    virtual void writeGetRequest(std::ostream* outputStream,
                                 const std::string* storeName,
                                 const std::string* key,
                                 bool shouldReroute) = 0;

    /**
     * Read a get response from the server.  The list returned is
     * owned by the caller, as is each of the VersionedValue objects
     * contained within it.
     *
     * @param inputStream input stream from which to read the response
     * @return a list of versioned values
     */
    virtual std::list<VersionedValue>* readGetResponse(std::istream* inputStream) = 0;

    /**
     * Write a GetAll request to the server
     *
     * @param outputStream output stream to which the request should
     * be written
     * @param storeName the name of the store
     * @param keys the list of keys to get
     * @param shouldReroute true if the server should reroute the
     * request
     */
    virtual void writeGetAllRequest(std::ostream* outputStream,
                                    const std::string* storeName,
                                    std::list<const std::string*>* keys,
                                    bool shouldReroute) = 0;

    //virtual void readGetAllResponse(std::istream* inputStream) = 0;

    /**
     * Write a Put request to the server
     *
     * @param outputStream output stream to which the request should
     * be written
     * @param storeName the name of the store
     * @param key the key to write
     * @param value the value to write
     * @param version the version of the value to write
     * @param shouldReroute true if the server should reroute the
     * request
     */
    virtual void writePutRequest(std::ostream* outputStream,
                                 const std::string* storeName,
                                 const std::string* key,
                                 const std::string* value,
                                 const VectorClock* version,
                                 bool shouldReroute) = 0;
    
    /**
     * Read a get response from the server.  Note that nothing is
     * returned but an exception could still be thrown.
     *
     * @param inputStream input stream from which to read the response
     */
    virtual void readPutResponse(std::istream* inputStream) = 0;

    /**
     * Write a Delete request to the server
     *
     * @param outputStream output stream to which the request should
     * be written
     * @param storeName the name of the store
     * @param key the key to write
     * @param version the version of the value to write
     * @param shouldReroute true if the server should reroute the
     * request
     */
    virtual void writeDeleteRequest(std::ostream* outputStream,
                                    const std::string* storeName,
                                    const std::string* key,
                                    const VectorClock* version,
                                    bool shouldReroute) = 0;
    
    /**
     * Read a delete response from the server
     *
     * @param inputStream input stream from which to read the response
     * @return true if anything was deleted
     */
    virtual bool readDeleteResponse(std::istream* inputStream) = 0;

    /**
     * Allocate a new RequestFormat object based of the given type.
     *
     * @param type the type of the request format to construct
     * @return a newly-alloced RequestFormat object
     */
    static RequestFormat* newRequestFormat(RequestFormatType type);
};

} /* namespace Voldemort */

#endif /* REQUESTFORMAT_H */
