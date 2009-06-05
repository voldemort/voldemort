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

#include "Connection.h"
#include <voldemort/VoldemortException.h>

#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/bind.hpp>
#include <boost/utility/base_from_member.hpp>

namespace Voldemort {

using namespace std;
using namespace boost;
using asio::ip::tcp;

Connection::Connection(string& hostName,
                       string& portNum,
                       shared_ptr<ClientConfig>& conf)
    : config(conf), host(hostName), port(portNum), io_service(), 
      resolver(io_service), timer(io_service), socket(io_service), 
      connbuf(NULL), connstream(NULL), active(false) {
    // Start an asynchronous resolve to translate the server and service names
    // into a list of endpoints.
    tcp::resolver::query query(host, port);
    resolver.async_resolve(query,
                           boost::bind(&Connection::handle_resolve, this,
                                       asio::placeholders::error,
                                       asio::placeholders::iterator));
    
    wait_for_operation(5000L);
}

Connection::~Connection() {
    close();
    if (connstream) delete connstream;
    if (connbuf) delete connbuf;
}

void Connection::check_error(const system::error_code& err) {
    if (err) {
        active = false;
        throw VoldemortException(err.message());
    } 
}

static void set_result(optional<system::error_code>* a, system::error_code b) {
    a->reset(b);
} 

void Connection::wait_for_operation(long millis)
{
    op_timeout = false;
    op_complete = false;
    timer.expires_from_now(posix_time::milliseconds(millis));
    timer.async_wait(boost::bind(&Connection::timeout, this));

    io_service.reset();
    while (io_service.run_one()) {
        if (op_complete) {
            timer.cancel();
        } else if (op_timeout) {
            close();
            throw VoldemortException("Network operation timeout");
        }
    }

}

void Connection::timeout() {
    op_timeout = true;
}

void Connection::handle_resolve(const system::error_code& err,
                                tcp::resolver::iterator endpoint_iterator) {
    check_error(err);
    
    // Attempt a connection to the first endpoint in the list. Each endpoint
    // will be tried until we successfully establish a connection.
    tcp::endpoint endpoint = *endpoint_iterator;
    socket.async_connect(endpoint,
                         boost::bind(&Connection::handle_connect, 
                                     this,
                                     asio::placeholders::error, 
                                     ++endpoint_iterator));

}

void Connection::handle_connect(const system::error_code& err,
                                tcp::resolver::iterator endpoint_iterator) {
    if (!err) {
        op_complete = true;
        active = true;
    } else if (endpoint_iterator != tcp::resolver::iterator()) {
        // The connection failed. Try the next endpoint in the list.
        socket.close();
        tcp::endpoint endpoint = *endpoint_iterator;
        socket.async_connect(endpoint,
                             boost::bind(&Connection::handle_connect, 
                                         this,
                                         asio::placeholders::error, 
                                         ++endpoint_iterator));
    } else {
        check_error(err);
    }
}

size_t Connection::read_some(char* buffer, size_t bufferLen) {
    socket.async_read_some(asio::buffer(buffer, bufferLen),
                           boost::bind(&Connection::handle_data_op, this,
                                       asio::placeholders::error,
                                       asio::placeholders::bytes_transferred));
    wait_for_operation(1000);
    return bytesTransferred;
}

void Connection::handle_data_op(const system::error_code& err,
                                size_t transferred) {
    check_error(err);
    bytesTransferred = transferred;
    op_complete = true;
}

size_t Connection::write(const char* buffer, size_t bufferLen) {
    asio::async_write(socket,
                      asio::buffer(buffer, bufferLen),
                      boost::bind(&Connection::handle_data_op, this,
                                  asio::placeholders::error,
                                  asio::placeholders::bytes_transferred));
    wait_for_operation(1000);
    return bytesTransferred;
}

void Connection::close() {
    active = false;
    socket.close();
    resolver.cancel();
}

ConnectionBuffer::ConnectionBuffer(Connection& con) 
    : conn(con), unbuffered_(false) {
    init_buffers();
};

ConnectionBuffer::~ConnectionBuffer() {
    if (pptr() != pbase())
        overflow(traits_type::eof());
};

void ConnectionBuffer::init_buffers() {
    setg(get_buffer_.begin(),
         get_buffer_.begin() + putback_max,
         get_buffer_.begin() + putback_max);
    if (unbuffered_)
        setp(0, 0);
    else
        setp(put_buffer_.begin(), put_buffer_.end());
}

int ConnectionBuffer::underflow() {
    if (gptr() == egptr()) {
        size_t bytes_transferred = 
            conn.read_some(get_buffer_.begin() + putback_max, 
                           get_buffer_.size());
        if (bytes_transferred < 0)
            return traits_type::eof();
        setg(get_buffer_.begin(), get_buffer_.begin() + putback_max,
             get_buffer_.begin() + putback_max + bytes_transferred);
        return traits_type::to_int_type(*gptr());
    } else {
        return traits_type::eof();
    }
}

int ConnectionBuffer::overflow(int c) {
    if (unbuffered_) {
        if (traits_type::eq_int_type(c, traits_type::eof())) {
            // Nothing to do.
            return traits_type::not_eof(c);
        } else {
            // Send the single character immediately.
            char_type ch = traits_type::to_char_type(c);
            size_t bytes_transferred = conn.write(&ch, 1);

            if (bytes_transferred != 1)
                return traits_type::eof();
            return c;
        }
    } else {
        // Send all data in the output buffer.
         size_t bytes_transferred = 
             conn.write(pbase(), pptr() - pbase());
         if (bytes_transferred <= 0)
             return traits_type::eof();
         setp(put_buffer_.begin(), put_buffer_.end());

        // If the new character is eof then our work here is done.
        if (traits_type::eq_int_type(c, traits_type::eof()))
            return traits_type::not_eof(c);

        // Add the new character to the output buffer.
        *pptr() = traits_type::to_char_type(c);
        pbump(1);
        return c;
    }
}

int ConnectionBuffer::sync() {
    return overflow(traits_type::eof());
}

iostream& Connection::get_io_stream() {
    if (!connbuf) {
        connbuf = new ConnectionBuffer(*this);
    }
    if (!connstream) {
        connstream = new iostream(connbuf);
    }
    return *connstream;
}

bool Connection::is_active() {
    return active;
}

std::string& Connection::get_host() {
    return host;
}

std::string& Connection::get_port() {
    return port;
}

} /* namespace Voldemort */
