/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for SocketStore class.
 * 
 * Copyright (c) 2009 Webroot Software, Inc.
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

#include "SocketStore.h"
#include "voldemort/VoldemortException.h"

#include <sstream>

namespace Voldemort {

using boost::asio::ip::tcp;

SocketStore::SocketStore(std::string& storeName, 
                         std::string& storeHost, 
                         int storePort,
                         RequestFormat::RequestFormatType requestFormatType) 
    : name(storeName), host(storeHost), port(storePort), 
      io_service(), resolver(io_service) {
    request = RequestFormat::newRequestFormat(requestFormatType);
}

SocketStore::~SocketStore() {
    close();
    delete request;
}

std::list<VersionedValue>* SocketStore::get(std::string* key) {
    std::ostringstream portString;
    portString << port;

    tcp::iostream sstream(host, portString.str());
    if (!sstream) {
        // XXX - TODO use UnreachableStoreException 
        throw VoldemortException("Could not access store");
    }

    request->writeGetRequest(&sstream,
                             &name,
                             key,
                             false);
    sstream.flush();
    
    /*
    */
    /*
    tcp::resolver::query query(host, port);
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    tcp::resolver::iterator end;

    tcp::socket socket(io_service);
    boost::system::error_code error = boost::asio::error::host_not_found;
    while (error && endpoint_iterator != end)
    {
      socket.close();
      socket.connect(*endpoint_iterator++, error);
    }
    if (error)
      throw boost::system::system_error(error);

    boost::asio::streambuf request;
    std::ostream request_stream(&request);
    */

    //sstream << std::endl;
    
    std::list<VersionedValue>* result = request->readGetResponse(&sstream);
    sstream.close();
    //return NULL;
    return result;
}

void SocketStore::put(std::string* key,
                      VersionedValue value) {
    /* XXX - TODO */
}

bool SocketStore::deleteKey(std::string* key,
                            Version version) {
    /* XXX - TODO */
}

std::string* SocketStore::getName() {
    return &name;
}

void SocketStore::close() {
    /* XXX - TODO */
}


} /* namespace Voldemort */
