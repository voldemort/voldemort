/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*!
 * @file ConnectionPool.h
 * @brief Interface definition file for ConnectionPool
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

#ifndef CONNECTIONPOOL_H
#define CONNECTIONPOOL_H

#include <stdlib.h>
#include <string>
#include "Connection.h"
#include <boost/shared_ptr.hpp>

namespace Voldemort {

using namespace boost;
using namespace std;

/**
 * A pool of persistent @ref Connection objects that can be used to
 * send pipelined requests to a Voldemort server.
 */
class ConnectionPool
{
public:

private:
    map<string, list<shared_ptr<Connection> > > pool;
};

} /* namespace Voldemort */

#endif /* CONNECTIONPOOL_H */
