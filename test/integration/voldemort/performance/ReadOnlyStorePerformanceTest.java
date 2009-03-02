/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.performance;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.Store;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.RandomAccessFileStorageConfiguration;
import voldemort.utils.ByteArray;
import voldemort.utils.Props;
import voldemort.utils.Utils;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

public class ReadOnlyStorePerformanceTest {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length != 4)
            Utils.croak("USAGE: java " + ReadOnlyStorePerformanceTest.class.getName()
                        + " num-threads num-requests server-properties-file storeName");
        int numThreads = Integer.parseInt(args[0]);
        int numRequests = Integer.parseInt(args[1]);
        String serverPropsFile = args[2];
        String storeName = args[3];

        final VoldemortConfig voldemortConfig = new VoldemortConfig(new Props(new File(serverPropsFile)));
        final Store<ByteArray, byte[]> store = new RandomAccessFileStorageConfiguration(voldemortConfig).getStore(storeName);
        File storeFile = new File(voldemortConfig.getMetadataDirectory() + File.separatorChar
                                  + "stores.xml");
        final List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new java.io.FileReader(storeFile));

        final AtomicInteger obsoletes = new AtomicInteger(0);
        final AtomicInteger nullResults = new AtomicInteger(0);
        final AtomicInteger totalResults = new AtomicInteger(0);

        int storeIndex = -1;
        boolean found = false;
        for(StoreDefinition def: storeDefs) {
            storeIndex++;
            if(def.getName().equals(storeName)) {
                found = true;
                break;
            }
        }

        if(!found) {
            Utils.croak("Store should be present in StoreList");
        }

        final Serializer keySerializer = new DefaultSerializerFactory().getSerializer(storeDefs.get(storeIndex)
                                                                                               .getKeySerializer());

        PerformanceTest readWriteTest = new PerformanceTest() {

            private final int MaxMemberID = (int) (35 * 1000 * 1000);

            public void doOperation(int index) throws Exception {
                try {
                    Integer memberId = new Integer((int) (Math.random() * MaxMemberID));
                    totalResults.incrementAndGet();
                    List<Versioned<byte[]>> results = store.get(new ByteArray(keySerializer.toBytes(memberId)));

                    if(results.size() == 0)
                        nullResults.incrementAndGet();

                } catch(ObsoleteVersionException e) {
                    obsoletes.incrementAndGet();
                }
            }
        };
        readWriteTest.run(numRequests, numThreads);
        System.out.println("Random Access Read Only store Results:");
        System.out.println("null Reads ratio:" + (nullResults.doubleValue())
                           / totalResults.doubleValue());
        readWriteTest.printStats();
    }
}
