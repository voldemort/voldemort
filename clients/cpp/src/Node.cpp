/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for Node class.
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

#include "Node.h"
#include <sys/time.h>

namespace Voldemort {

using namespace boost;

Node::Node(int id,
           std::string& host,
           int httpPort,
           int socketPort,
           int adminPort,
           shared_ptr<std::list<int> >& partitions) 
    : id_(id), host_(host), httpPort_(httpPort), socketPort_(socketPort),
      adminPort_(adminPort), partitions_(partitions), 
      isAvailable_(true), lastChecked_(0) {

}

Node::Node()
  : id_(-1), httpPort_(0), socketPort_(0), 
    adminPort_(0), partitions_(new std::list<int>), 
    isAvailable_(true), lastChecked_(0) {
}

void Node::setAvailable(bool avail) {
    isAvailable_ = avail;

    struct timeval tv;
    gettimeofday(&tv, NULL);
    lastChecked_ = (uint64_t)tv.tv_sec*1000 + (uint64_t)tv.tv_usec/1000;
}

uint64_t Node::getMsSinceLastChecked() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    uint64_t time = (uint64_t)tv.tv_sec*1000 + (uint64_t)tv.tv_usec/1000;
    return (time - lastChecked_);
}

bool Node::isAvailable(uint64_t timeout) {
    return (isAvailable_ ||
            (getMsSinceLastChecked() > timeout));
}

std::ostream& operator<<(std::ostream& output, const Node& node) {
    output << "Node" << node.id_;
    return output;
}

} /* namespace Voldemort */
