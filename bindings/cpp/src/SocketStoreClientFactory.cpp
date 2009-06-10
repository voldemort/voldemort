/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for SocketStoreClientFactory class.
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

#include <voldemort/BootstrapFailureException.h>
#include <voldemort/SocketStoreClientFactory.h>
#include "SocketStore.h"
#include "RoutedStore.h"
#include "RequestFormat.h"
#include "DefaultStoreClient.h"
#include "Cluster.h"
#include "InconsistencyResolvingStore.h"
#include "TimeBasedInconsistencyResolver.h"
#include "VectorClockInconsistencyResolver.h"

#include <iostream>
#include <exception>
#include <ctype.h>

#include "boost/threadpool.hpp"

namespace Voldemort {

using namespace boost;
using namespace std;

static const string METADATA_STORE_NAME("metadata");
static const string CLUSTER_KEY("cluster.xml");
static const string STORES_KEY("stores.xml");
static const string ROLLBACK_CLUSTER_KEY("rollback.cluster.xml");

class SocketStoreClientFactoryImpl {
public:
    SocketStoreClientFactoryImpl(ClientConfig& conf);
    ~SocketStoreClientFactoryImpl();

    /** 
     * Get a raw socket store object
     */
    Store* getStore(const string& storeName,
                    const string& host,
                    int port,
                    RequestFormat::RequestFormatType type);
    
    /**
     * Retrieve the given metadata key using the bootstrap list
     */
    VersionedValue bootstrapMetadata(const string& key);

    Store* getStore(const string& storeName,
                    const string& host,
                    int port,
                    RequestFormat::RequestFormatType type,
                    bool shouldReroute);

    shared_ptr<ClientConfig> config;
    shared_ptr<ConnectionPool> connPool;
    RequestFormat::RequestFormatType requestFormatType;
    shared_ptr<threadpool::pool> threadPool;
};

SocketStoreClientFactoryImpl::SocketStoreClientFactoryImpl(ClientConfig& conf) 
    : config(new ClientConfig(conf)), connPool(new ConnectionPool(config)),
      requestFormatType(RequestFormat::PROTOCOL_BUFFERS), 
      threadPool() 
{
    //threadPool->size_controller().resize(config->getMaxThreads());
}

SocketStoreClientFactoryImpl::~SocketStoreClientFactoryImpl() {

}

Store* SocketStoreClientFactoryImpl::getStore(const string& storeName,
                                              const string& host,
                                              int port,
                                              RequestFormat::RequestFormatType type,
                                              bool shouldReroute) {
    return new SocketStore(storeName,
                           host,
                           port,
                           config,
                           connPool,
                           type,
                           shouldReroute);
}

#define THROW_BOOTSTRAP \
    throw VoldemortException("Invalid bootstrap URL " + url + \
                             ": Expected tcp://host:port");
#define EXPECT_C(char, next) \
    if (tolower(*it) != char) \
        THROW_BOOTSTRAP; \
    state = next;

static void parseBootstrapUrl(const string& url, string& host, int& port) {
    static const int STATE_T = 0;
    static const int STATE_C = 1;
    static const int STATE_P = 2;
    static const int STATE_COL1 = 3;
    static const int STATE_SL1 = 4;
    static const int STATE_SL2 = 5;
    static const int STATE_HOSTSTRING = 6;
    static const int STATE_PORTSTRING = 7;

    stringstream hostStr, portStr;
    int state = STATE_T;

    string::const_iterator it;
    for (it = url.begin(); it != url.end(); ++it) {
        switch(state) {
        case STATE_T:
            EXPECT_C('t', STATE_C);
            break;
        case STATE_C:
            EXPECT_C('c', STATE_P);
            break;
        case STATE_P:
            EXPECT_C('p', STATE_COL1);
            break;
        case STATE_COL1:
            EXPECT_C(':', STATE_SL1);
            break;
        case STATE_SL1:
            EXPECT_C('/', STATE_SL2);
            break;
        case STATE_SL2:
            EXPECT_C('/', STATE_HOSTSTRING);
            break;
        case STATE_HOSTSTRING:
            if (isalnum(*it)) 
                hostStr << *it;
            else if (*it == ':') 
                state = STATE_PORTSTRING;
            else
                THROW_BOOTSTRAP;
            break;
        case STATE_PORTSTRING:
            if (isdigit(*it)) 
                portStr << *it;
            else
                THROW_BOOTSTRAP;
            break;
        }
    }

    if (hostStr.str().length() == 0)
        THROW_BOOTSTRAP;
    if (portStr.str().length() == 0)
        THROW_BOOTSTRAP;

    host = hostStr.str();
    portStr >> port;
    if (port == 0)
        THROW_BOOTSTRAP;
}

VersionedValue SocketStoreClientFactoryImpl::bootstrapMetadata(const string& key) {
    std::list<std::string>* boots = config->getBootstrapUrls();
    std::list<std::string>::const_iterator it;
    for (it = boots->begin(); it != boots->end(); ++it) {
        try {
            string host;
            int port;
            parseBootstrapUrl(*it, host, port);

            auto_ptr<Store> store(getStore(METADATA_STORE_NAME,
                                           host,
                                           port,
                                           requestFormatType,
                                           false));

            auto_ptr<std::list<VersionedValue> > vvs(store->get(key));
            if (vvs->size() == 1) {
                return vvs->front();
            }
        } catch (std::exception& e) {
            cerr << "Warning: Could not bootstrap '" << *it << "': "
                 << e.what() << endl;
        }
    }
    throw BootstrapFailureException("No available bootstrap servers found!");
}

SocketStoreClientFactory::SocketStoreClientFactory(ClientConfig& conf) {
    pimpl_ = new SocketStoreClientFactoryImpl(conf);

}

SocketStoreClientFactory::~SocketStoreClientFactory() {
    if (pimpl_) 
        delete pimpl_;
}

StoreClient* SocketStoreClientFactory::getStoreClient(std::string& storeName) {
    shared_ptr<InconsistencyResolver> nullResolver;
    return getStoreClient(storeName, nullResolver);
}

StoreClient* SocketStoreClientFactory::
getStoreClient(std::string& storeName,
               shared_ptr<InconsistencyResolver>& resolver) {
    shared_ptr<Store> store(getRawStore(storeName, resolver));
    return new DefaultStoreClient(store,
                                  pimpl_->config);

}

Store* SocketStoreClientFactory::getRawStore(std::string& storeName,
                                             shared_ptr<InconsistencyResolver>& resolver) {
    VersionedValue clustervv = pimpl_->bootstrapMetadata(CLUSTER_KEY);
    const std::string* clusterXml = clustervv.getValue();
    shared_ptr<Cluster> cluster(new Cluster(*clusterXml));

    shared_ptr<std::map<int, shared_ptr<Store> > > 
        clusterMap(new std::map<int, shared_ptr<Store> >());

    const std::map<int, boost::shared_ptr<Node> >* nodeMap = cluster->getNodeMap();
    std::map<int, boost::shared_ptr<Node> >::const_iterator it;
    for (it = nodeMap->begin(); it != nodeMap->end(); ++it) {
        shared_ptr<Store> store(pimpl_->getStore(storeName,
                                                 it->second->getHost(),
                                                 it->second->getSocketPort(),
                                                 pimpl_->requestFormatType,
                                                 true));
        (*clusterMap)[it->second->getId()] = store;
    }

    //VersionedValue storevv = pimpl_->bootstrapMetadata(STORES_KEY);
    //const std::string* storesXml = storevv.getValue();

    shared_ptr<Store> routedStore(new RoutedStore(storeName,
                                                  pimpl_->config,
                                                  cluster,
                                                  clusterMap,
                                                  pimpl_->threadPool));

    /* XXX - TODO Add inconsistency resolver */
    InconsistencyResolvingStore* conStore = new InconsistencyResolvingStore(routedStore);
    try {
        shared_ptr<InconsistencyResolver> 
            vcResolver(new VectorClockInconsistencyResolver());
        conStore->addResolver(vcResolver);
        
        if (resolver.get()) {
            conStore->addResolver(resolver);
        } else {
            shared_ptr<InconsistencyResolver> 
                tbResolver(new TimeBasedInconsistencyResolver());
            conStore->addResolver(tbResolver);
        }
    } catch (...) {
        if (conStore) delete conStore;
        throw;
    }

    return conStore;
}

} /* namespace Voldemort */
