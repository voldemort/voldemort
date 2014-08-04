/*!
 * @file voldemort.h
 * @brief Main include file for Project Voldemort C/C++ Client
 * 
 * This is the master include file for the Project Voldemort C/C++ Client
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
/*! 
 * @mainpage
 *
 * This is the documentation for the Voldemort C++ client.
 *
 * <h2>Overview</h2>
 *
 * This client allows you to communicate with a Project Voldemort
 * server from C++.  It currently requires server routing, and does
 * not directly support the serialization features of the Voldemort
 * server (though it's possible to do so externally).
 *
 * <h2>Example</h2>
 * @code
 * #include <string>
 * #include <list>
 * #include <memory>
 * #include <iostream>
 *
 * using namespace std;
 * using namespace Voldemort;
 *
 * // ...
 *
 * // Initialize the bootstrap URLs.  This is a list of server URLs
 * // in the cluster that we use to download metadata for the
 * // cluster.  You only need one to be able to use the cluster, but
 * // more will increase availability when initializing.
 * list<string> bootstrapUrls;
 * for (int i = 1; i < argc; i++) {
 *     bootstrapUrls.push_back(string(argv[i]));
 * }
 *
 * // The store name is essentially a namespace on the Voldemort
 * // cluster
 * string storeName("test");
 *
 * // The ClientConfig object allows you to configure settings on how
 * // we access the Voldemort cluster.  The set of bootstrap URLs is
 * // the only thing that must be configured.
 * ClientConfig config;
 * config.setBootstrapUrls(&bootstrapUrls);
 *
 * // We access the server using a StoreClient object.  We create
 * // StoreClients using a StoreClientFactory.  In this case we're
 * // using the SocketStoreClientFactory which will connect to a
 * // Voldemort cluster over TCP.
 * SocketStoreClientFactory factory(config);
 * auto_ptr<StoreClient> client(factory.getStoreClient(storeName)); *
 * 
 * // Get a value
 * std::string key("hello");
 * const VersionedValue* result = client->get(&key);
 * VersionedValue value;
 * if (result) {
 *     value = *result;
 *     cout << "Value: " << *(value.getValue()) << endl;
 * } else {
 *     cout << "Value not set" << endl;
 * }
 * 
 * // Modify the value
 * value.setValue(new string("world!"));
 * 
 * // update the value
 * client->put(&key, &value);
 * 
 * value = *client->get(&key);
 * cout << "Value: " << *(value.getValue()) << endl;
 * 
 * // Set and then delete a key
 * std::string key2("keytest");
 * std::string value2("valuetest");
 * client->put(&key2, &value2);
 * client->deleteKey(&key2);
 * @endcode
 */

#ifndef VOLDEMORT_VOLDEMORT_H
#define VOLDEMORT_VOLDEMORT_H

#include <voldemort/SocketStoreClientFactory.h>

#include <voldemort/BootstrapFailureException.h>
#include <voldemort/InconsistentDataException.h>
#include <voldemort/InsufficientOperationalNodesException.h>
#include <voldemort/InvalidMetadataException.h>
#include <voldemort/ObsoleteVersionException.h>
#include <voldemort/PersistenceFailureException.h>
#include <voldemort/StoreOperationFailureException.h>
#include <voldemort/UnreachableStoreException.h>

#endif/*VOLDEMORT_VOLDEMORT_H*/
