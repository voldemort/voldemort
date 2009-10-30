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
#include <voldemort/UnreachableStoreException.h>

#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/bind.hpp>
#include <boost/utility/base_from_member.hpp>

#include <sys/types.h>
#include <sys/socket.h>
#include <cerrno>

#if !defined(MSG_NOSIGNAL) && defined(__APPLE__)
#define MSG_NOSIGNAL MSG_HAVEMORE
#endif

namespace Voldemort {

using namespace std;
using namespace boost;
using asio::ip::tcp;

Connection::Connection(const string& hostName,
                       const string& portNum,
                       const string& negString,
                       shared_ptr<ClientConfig>& conf)
    : config(conf), host(hostName), port(portNum), negotiationString(negString),
      io_service(), resolver(io_service), timer(io_service),
      socket(io_service), connbuf(NULL), connstream(NULL), active(false) {
}

Connection::~Connection() {
    close();
    if (connstream) delete connstream;
    if (connbuf) delete connbuf;
}

void Connection::connect() {
    // Start an asynchronous resolve to translate the server and service names
    // into a list of endpoints.
    tcp::resolver::query query(host, port);
    resolver.async_resolve(query,
                           boost::bind(&Connection::handle_resolve, this,
                                       asio::placeholders::error,
                                       asio::placeholders::iterator));

    wait_for_operation(config->getConnectionTimeoutMs());
}

void Connection::check_error(const boost::system::error_code& err) {
    if (err) {
        active = false;
        throw UnreachableStoreException(err.message());
    }
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
            throw UnreachableStoreException("Network operation timeout");
        }
    }

}

void Connection::timeout() {
    op_timeout = true;
}

void Connection::handle_resolve(const boost::system::error_code& err,
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

void Connection::handle_connect(const boost::system::error_code& err,
                                tcp::resolver::iterator endpoint_iterator) {
    if (!err) {
        /* We're done with ASIO now, since it performs really poorly
           for reading/writing.  Set socket to blocking mode with
           timeouts. */
        int sock = socket.native();
        fcntl(sock, F_SETFL, 0);

        struct timeval tv;
        long to = config->getSocketTimeoutMs();
        tv.tv_sec = to/1000;
        tv.tv_usec = (to - tv.tv_sec*1000) * 1000;
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        /* Negotiate protocol for the open connection */
        write(negotiationString.c_str(), negotiationString.length());
        char res_buffer[2];
        size_t got = 0;
        while (got < 2) {
            got += read_some(res_buffer, 2-got);
        }
        if (res_buffer[0] != 'o' || res_buffer[1] != 'k') {
            throw UnreachableStoreException("Failed to negotiate "
                                            "protocol with server");
        }

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
#if 0
    socket.async_read_some(asio::buffer(buffer, bufferLen),
                           boost::bind(&Connection::handle_data_op, this,
                                       asio::placeholders::error,
                                       asio::placeholders::bytes_transferred));
    wait_for_operation(config->getSocketTimeoutMs());
    return bytesTransferred;
#endif

    int sock = socket.native();
    ssize_t bytes;

    do {
        bytes = recv(sock, buffer, bufferLen, MSG_NOSIGNAL);
    } while (bytes < 0 && errno == EAGAIN);

    if (bytes < 0) {
        active = false;
        throw UnreachableStoreException("read failed");
    }
    return (size_t)bytes;
}

#if 0
void Connection::handle_data_op(const boost::system::error_code& err,
                                size_t transferred) {
    check_error(err);
    bytesTransferred = transferred;
    op_complete = true;
}
#endif

size_t Connection::write(const char* buffer, size_t bufferLen) {
#if 0
    asio::async_write(socket,
                      asio::buffer(buffer, bufferLen),
                      boost::bind(&Connection::handle_data_op, this,
                                  asio::placeholders::error,
                                  asio::placeholders::bytes_transferred));
    wait_for_operation(config->getSocketTimeoutMs());

    return bytesTransferred;
#endif

    int sock = socket.native();
    ssize_t bytes = send(sock, buffer, bufferLen, MSG_NOSIGNAL);
    if (bytes < 0) {
        active = false;
        throw UnreachableStoreException("write failed");
    }

    return (size_t)bytes;

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
