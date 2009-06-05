/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file Connection.h
 * @brief Interface definition file for Connection
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

#ifndef CONNECTION_H
#define CONNECTION_H

#include <voldemort/ClientConfig.h>
#include "RequestFormat.h"

#include <streambuf>
#include <sstream>
#include <iostream>
#include <boost/asio.hpp>
#include <boost/thread/condition_variable.hpp>

namespace Voldemort {

using namespace boost;
using asio::ip::tcp;

class ConnectionBuffer;

/** 
 * Connection class holds a persistent connection to a Voldemort
 * server
 */
class Connection {
public:
    /**
     * Construct a new Connection object 
     * 
     * @param hostName the host to connect to
     * @param portNum the port to connect to
     * @param conf the client config object
     * @param requestFormat the request format object */
    Connection(std::string& hostName,
               std::string& portNum,
               shared_ptr<ClientConfig>& conf,
               shared_ptr<RequestFormat>& requestFormat);
    ~Connection();

    /**
     * Close the underlying connection.  After calling this the
     * Connection object cannot be used.
     */
    void close();

    /**
     * Get a stream object for this connection that supports
     * appropriate buffering and timeouts.
     *
     * @param the stream
     */
    std::iostream& get_io_stream();

    size_t read_some(char* buffer, size_t bufferLen);
    size_t write(const char* buffer, size_t bufferLen);

protected:
    void wait_for_operation(long millis);
    void timeout();
    void handle_connect(const system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator);

    void handle_resolve(const system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator);
    void handle_data_op(const system::error_code& err,
                        size_t transferred);

    shared_ptr<ClientConfig> config;

    std::string& host;
    std::string& port;

    asio::io_service io_service;
    tcp::resolver resolver;
    asio::deadline_timer timer;
    tcp::socket socket;
    
    bool op_timeout;
    bool op_complete;

    size_t bytesTransferred;

    ConnectionBuffer* connbuf;
    std::iostream* connstream;
};

/** Stream buffer used to construct iostream */
class ConnectionBuffer
    : public std::basic_streambuf<char>
{
public:
    explicit ConnectionBuffer(Connection& con);
    virtual ~ConnectionBuffer();

    virtual traits_type::int_type overflow(traits_type::int_type c = EOF);
    virtual traits_type::int_type underflow();
    virtual int sync();

private:
    void init_buffers();

    Connection& conn;

    enum { putback_max = 8 };
    enum { buffer_size = 8196 };
    boost::array<char, buffer_size> get_buffer_;
    boost::array<char, buffer_size> put_buffer_;
    bool unbuffered_;
};

} /* namespace Voldemort */

#endif /* CONNECTION_H */
