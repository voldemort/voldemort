/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Client application for accessing a Voldemort server through the C++
 * client library.
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

#include <voldemort/voldemort.h>
#include <string>
#include <list>
#include <memory>
#include <iostream>

using namespace std;
using namespace Voldemort;

void doGet(Store* rawStore, const std::string& key) {
    cout << "Getting key \"" << key << "\"" << endl;

    auto_ptr<list<VersionedValue> > values(rawStore->get(key));
    if (values.get() != NULL) {
        list<VersionedValue>::const_iterator it;
        
        for (it = values->begin(); it != values->end(); ++it) {
            const char* cstr = it->getValue()->c_str();
            cout << "  Value: " << cstr << endl;
            
        }
    }
}

int main(int argc, char** argv) {
    try {
        // Initialize the bootstrap URLs.  This is a list of server URLs
        // in the cluster that we use to download metadata for the
        // cluster.  You only need one to be able to use the cluster, but
        // more will increase availability when initializing.
        list<string> bootstrapUrls;
        bootstrapUrls.push_back(string("tcp://localhost:6666"));

        // The store name is essentially a namespace on the Voldemort
        // cluster
        string storeName("test");

        // The ClientConfig object allows you to configure settings on how
        // we access the Voldemort cluster.  The set of bootstrap URLs is
        // the only thing that must be configured.
        ClientConfig config;
        config.setBootstrapUrls(&bootstrapUrls);

        // We access the server using a StoreClient object.  We create
        // StoreClients using a StoreClientFactory.  In this case we're
        // using the SocketStoreClientFactory which will connect to a
        // Voldemort cluster over TCP.
        SocketStoreClientFactory factory(config);
    
        auto_ptr<Store> rawStore(factory.getRawStore(storeName));
    
        doGet(rawStore.get(), "hello");
        doGet(rawStore.get(), "hello1");
        doGet(rawStore.get(), "hello2");

    } catch (VoldemortException& v) {
        cerr << "Voldemort Error: " << v.what() << endl;
    }
#if 0
    auto_ptr<StoreClient> client(factory.getStoreClient(storeName));

    // Get a value
    auto_ptr<VersionedValue> value(client->get(&key));

    cout << "Value: " << value->getValue() << endl;

    // Modify the value
    value->setValue(new string("world!"));

    // update the value
    client->put(&key, value.get());
#endif
}
