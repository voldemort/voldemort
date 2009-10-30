/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Stress tester for Voldemort C++ client
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

#include <signal.h>
#include <sys/time.h>
#include <time.h>
#include <memory>
#include <vector>
#include <sstream>
#include <iostream>

#include <voldemort/voldemort.h>
#include <boost/thread/thread.hpp>
#include <boost/shared_ptr.hpp>

using namespace Voldemort;
using namespace std;
using namespace boost;

const int STRESS_THREADS = 10;
const string STORE_NAME("test");
const string KEY("hello");
const string VALUE("world");

volatile int continueStress = 1;

class stresser
{
public:
    stresser(StoreClient* client_,
             int& count_) 
        : client(client_), count(count_) { }

    void operator()() {
        while (continueStress) {
            try {
                // Simple GET
                client->get(&KEY);
#if 0
                // This does an implicit GET followed by a PUT
                stringstream str;
                str << KEY << count << "_" << client;
                string key(str.str());
                client->put(&key, &VALUE);
                count += 1;
#endif
            } catch (VoldemortException& e) {
                cerr << e.what() << endl;
            }
        }
    }

    StoreClient* client;
    int& count;
};

/* Halt tester threads on sigint */
void handleSig(int sig) {
    continueStress = 0;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        cerr << "Usage: " << argv[0] << "{bootstrap URLs list}" << endl
             << "   URLs of the form tcp://host:port" << endl;
        exit(1);
    }

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

        vector<shared_ptr<StoreClient> > clients;
        for (int i = 0; i < STRESS_THREADS; i++) {
            shared_ptr<StoreClient> client(factory.getStoreClient(STORE_NAME));
            if (!client.get()) {
                cerr << "Count not initialize client " << i << endl;
                exit(1);
            }
            clients.push_back(client);
        }        
        int counts[STRESS_THREADS];
        memset(counts, 0, sizeof(int) * STRESS_THREADS);

        struct timeval starttime;
        struct timeval endtime;

        // Start up stress threads
        gettimeofday(&starttime, NULL);
        thread_group group;
        for (int i = 0; i < STRESS_THREADS; i++) {
            group.create_thread(stresser(clients.at(i).get(), counts[i]));
        }

        signal(SIGINT, handleSig);

        // join stress threads
        group.join_all();
        gettimeofday(&endtime, NULL);

        int total = 0;
        for (int i = 0; i < STRESS_THREADS; i++) {
            total += counts[i];
        }
        unsigned long time = 
            ((((unsigned long)endtime.tv_sec)*1000000L) + 
             (unsigned long)endtime.tv_usec) -
            ((((unsigned long)starttime.tv_sec)*1000000L) + 
             (unsigned long)starttime.tv_usec);
        time /= 1000;
        double throughput = ((double)total) * (double)1000 / time;

        cout << "Performed " << total << " ops in " 
             << ((double)time)/1000.0 
             << " seconds (" << throughput << " ops/sec)"
             << endl;
    } catch (VoldemortException& e) {
        cerr << "Error while initializing: " << e.what() << endl;
        exit(1);
    }
}
