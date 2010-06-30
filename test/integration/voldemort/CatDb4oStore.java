/*
 * Copyright 2010 Versant Corporation
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

package voldemort;

import java.io.File;
import java.util.Iterator;

import voldemort.serialization.StringSerializer;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageEngine;
import voldemort.store.db4o.Db4oByteArrayStorageEngine;
import voldemort.store.db4o.Db4oKeyValuePair;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Props;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.db4o.config.QueryEvaluationMode;
import com.db4o.cs.Db4oClientServer;
import com.db4o.cs.config.ServerConfiguration;

public class CatDb4oStore {

    private static ServerConfiguration newDb4oConfig() {
        return getDb4oConfig(Db4oKeyValuePair.class, "key");
    }

    @SuppressWarnings("unchecked")
    private static ServerConfiguration getDb4oConfig(Class keyValuePairClass, String keyFieldName) {
        ServerConfiguration config = Db4oClientServer.newServerConfiguration();
        // Use lazy mode
        config.common().queries().evaluationMode(QueryEvaluationMode.LAZY);
        // Set activation depth to 0
        config.common().activationDepth(0);
        // Set index by Key
        config.common().objectClass(keyValuePairClass).objectField(keyFieldName).indexed(true);
        // Cascade on delete
        config.common().objectClass(keyValuePairClass).cascadeOnDelete(true);
        return config;
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 2)
            Utils.croak("USAGE: java " + CatDb4oStore.class.getName() + "db4o_dir" + "storeName"
                        + " server.properties.path");
        String db4oDir = args[0];
        String storeName = args[1];
        String serverProperties = args[2];

        VoldemortConfig config = new VoldemortConfig(new Props(new File(serverProperties)));

        // environmentConfig.setTxnNoSync(true);
        // environmentConfig.setAllowCreate(true);
        // environmentConfig.setTransactional(config.isBdbWriteTransactionsEnabled());
        // databaseConfig.setAllowCreate(true);
        // databaseConfig.setTransactional(config.isBdbWriteTransactionsEnabled());
        // databaseConfig.setSortedDuplicates(config.isBdbSortedDuplicatesEnabled());

        // TODO Read Voldemort config and apply to db4o configuraton instance

        String databasePath = db4oDir + storeName + ".yap";

        StorageEngine<ByteArray, byte[]> store = new Db4oByteArrayStorageEngine(databasePath,
                                                                                newDb4oConfig());

        StorageEngine<String, String> stringStore = SerializingStorageEngine.wrap(store,
                                                                                  new StringSerializer(),
                                                                                  new StringSerializer());
        Iterator<Pair<String, Versioned<String>>> iter = stringStore.entries();
        while(iter.hasNext()) {
            Pair<String, Versioned<String>> entry = iter.next();
            System.out.println(entry.getFirst() + " => " + entry.getSecond().getValue());
        }
    }
}
