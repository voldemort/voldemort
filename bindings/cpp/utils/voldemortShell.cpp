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
#include <map>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <vector>

using namespace std;
using namespace Voldemort;

typedef bool (*command)(StoreClient* client, const vector<string>& tokens);

bool getCommand(StoreClient* client, const vector<string>& tokens) {
    if (tokens.size() < 2) {
        cerr << "Usage: get {list of keys}" << endl;
        return true;
    }
    
    for (unsigned int i = 1; i < tokens.size(); i++) {
        const VersionedValue* result = client->get(&tokens[i]);
        cout << tokens[i] << ": ";
        if (result) {
            cout << *(result->getValue()) << " " << *(result->getVersion());
        } else {
            cout << "null";
        }
        cout << endl;
    }
    return true;
}

bool putCommand(StoreClient* client, const vector<string>& tokens) {
    if (tokens.size() < 3) {
        cerr << "Usage: put key value" << endl;
        return true;
    }
    
    client->put(&tokens[1], &tokens[2]);
    return true;
}

bool deleteCommand(StoreClient* client, const vector<string>& tokens) {
    if (tokens.size() < 2) {
        cerr << "Usage: delete {list of keys}" << endl;
        return true;
    }
    
    for (unsigned int i = 1; i < tokens.size(); i++) {
        cout << tokens[i] << ": "
             << client->deleteKey(&tokens[i]) << endl;
    }
    return true;
}

bool quitCommand(StoreClient* client, const vector<string>& tokens) {
    return false;
}

int main(int argc, char** argv) {
    map<string, command> comMap; 
    comMap[string("get")] = getCommand;
    comMap[string("put")] = putCommand;
    comMap[string("delete")] = deleteCommand;
    comMap[string("exit")] = quitCommand;
    comMap[string("quit")] = quitCommand;

    try {
        // Initialize the bootstrap URLs.  This is a list of server URLs
        // in the cluster that we use to download metadata for the
        // cluster.  You only need one to be able to use the cluster, but
        // more will increase availability when initializing.
        list<string> bootstrapUrls;
        for (int i = 1; i < argc; i++) {
            bootstrapUrls.push_back(string(argv[i]));
        }

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
        auto_ptr<StoreClient> client(factory.getStoreClient(storeName));

        while (!cin.eof()) {
            try {
                string line;
                cerr << "> ";

                getline (cin, line);
                if (cin.eof()) {
                    cerr << endl;
                    break;
                }

                istringstream iss(line);
                vector<string> tokens;
                copy(istream_iterator<string>(iss),
                     istream_iterator<string>(),
                     back_inserter<vector<string> >(tokens));

                map<string, command>::iterator it;
                if (tokens.size() > 0) 
                    it = comMap.find(tokens[0]);
                if (it == comMap.end()) {
                    cerr << "Error: unrecognized command" << endl;
                } else if (!(it)->second(client.get(), tokens))
                    break;

            } catch (VoldemortException& v) {
                cerr << "Error: " << v.what() << endl;
            }
        }

    } catch (std::exception& e) {
        cerr << "Error while initializing: " << e.what() << endl;
        exit(1);
    }

}
