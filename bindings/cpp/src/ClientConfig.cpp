/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for ClientConfig class.
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

#include <voldemort/ClientConfig.h>

namespace Voldemort {

class ClientConfigImpl {
public:
    int maxConnectionsPerNode;
    int maxTotalConnections;
    int maxThreads;
    int maxQueuedRequests;
    long threadIdleMs;
    long connectionTimeoutMs;
    long socketTimeoutMs;
    long routingTimeoutMs;
    long defaultNodeBannageMs;
    int socketBufferSize;
    std::list<std::string>* bootstrapUrls;
};

ClientConfig::ClientConfig() {
    pimpl_ = new ClientConfigImpl();
    pimpl_->maxConnectionsPerNode = 0;
    pimpl_->maxTotalConnections = 500;
    pimpl_->maxThreads = 5;
    pimpl_->maxQueuedRequests = 500;
    pimpl_->threadIdleMs = 100000;
    pimpl_->connectionTimeoutMs = 500;
    pimpl_->socketTimeoutMs = 5000;
    pimpl_->routingTimeoutMs = 15000;
    pimpl_->defaultNodeBannageMs = 30000;
    pimpl_->socketBufferSize = 64 * 1024;
    pimpl_->bootstrapUrls = NULL;
}

ClientConfig::ClientConfig(const ClientConfig& cc) {
    pimpl_ = new ClientConfigImpl(*cc.pimpl_);
}

ClientConfig::~ClientConfig() {
    delete pimpl_;
}

ClientConfig* ClientConfig::setBootstrapUrls(std::list<std::string>* bootstrapUrls) {
    pimpl_->bootstrapUrls = bootstrapUrls;
    return this;
}

std::list<std::string>* ClientConfig::getBootstrapUrls() {
    return pimpl_->bootstrapUrls;
}

} /* namespace Voldemort */
