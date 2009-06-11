/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for Cluster class.
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

#include "Cluster.h"
#include <voldemort/VoldemortException.h>
#include <iostream>
#include <string.h>

namespace Voldemort {

using namespace boost;

Node::Node(int id,
           std::string& host,
           int httpPort,
           int socketPort,
           int adminPort,
           shared_ptr<std::list<int> >& partitions) 
    : id_(id), host_(host), httpPort_(httpPort), socketPort_(socketPort),
      adminPort_(adminPort), partitions_(partitions) {

}

Node::Node()
  : id_(-1), httpPort_(0), socketPort_(0), 
    adminPort_(0), partitions_(new std::list<int>) {
}

} /* namespace Voldemort */
