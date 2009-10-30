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
     * @param negString the protocol negotiation string
     * @param conf the client config object */
    Connection(const std::string& hostName,
               const std::string& portNum,
               const std::string& negString,
               shared_ptr<ClientConfig>& conf);
    ~Connection();

    /**
     * Connect to the remote host
     */
    void connect();

    /**
     * Close the underlying connection.  After calling this the
     * Connection object cannot be used.
     */
    void close();

    /**
     * Get a stream object for this connection that supports
     * appropriate buffering and timeouts.
     *
     * @return the stream
     */
    std::iostream& get_io_stream();

    /**
     * Read some data from the socket up to the provided bufferLen
     * into buffer.  May read less than bufferLen.
     *
     * @param buffer the buffer to read into
     * @param bufferLen the maximum number of bytes
     * @return the number of bytes read
     */
    size_t read_some(char* buffer, size_t bufferLen);

    /**
     * Write the data provided to the socket from the buffer.  Will
     * write all the data or generate an error.
     *
     * @param buffer the buffer to read from
     * @param bufferLen the bytes to write
     * @return the number of bytes written
     */
    size_t write(const char* buffer, size_t bufferLen);

    /**
     * Checks whether the provided connection is still good
     *
     * @return whether the connection is functioning
     */
    bool is_active();

    /**
     * Get the host for this connection
     *
     * @return The host string
     */
    std::string& get_host();

    /**
     * Get the port for this connection
     *
     * @return The port string
     */
    std::string& get_port();

private:
    void wait_for_operation(long millis);
    void timeout();
    void handle_connect(const boost::system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator);

    void handle_resolve(const boost::system::error_code& err,
                        tcp::resolver::iterator endpoint_iterator);
#if 0
    void handle_data_op(const boost::system::error_code& err,
                        size_t transferred);
#endif
    void check_error(const boost::system::error_code& err);

    shared_ptr<ClientConfig> config;

    std::string host;
    std::string port;
    std::string negotiationString;

    asio::io_service io_service;
    tcp::resolver resolver;
    asio::deadline_timer timer;
    tcp::socket socket;
    
    bool op_timeout;
    bool op_complete;

    size_t bytesTransferred;

    ConnectionBuffer* connbuf;
    std::iostream* connstream;

    bool active;
};

/** Stream buffer used to construct iostream */
class ConnectionBuffer
    : public std::basic_streambuf<char>
{
public:
    /**
     * Create a new connection buffer object connected to the given
     * @ref Connection.
     */
    explicit ConnectionBuffer(Connection& con);
    virtual ~ConnectionBuffer();

    /**
     * Implementation of basic_streambuf::overflow
     * 
     * @param c the overflow character
     * @return status information
     */
    virtual traits_type::int_type overflow(traits_type::int_type c = EOF);

    /**
     * Implementation of basic_streambuf::underflow
     * 
     * @return status information
     */
    virtual traits_type::int_type underflow();

    /**
     * Implementation of basic_streambuf::sync
     *
     * @return status information
     */
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
