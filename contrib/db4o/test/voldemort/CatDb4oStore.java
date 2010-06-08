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
import voldemort.store.db4o.Db4oStorageEngine;
import voldemort.store.serialized.SerializingStorageEngine;
import voldemort.utils.ByteArray;
import voldemort.utils.Pair;
import voldemort.utils.Props;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.db4o.Db4oEmbedded;
import com.db4o.config.EmbeddedConfiguration;

public class CatDb4oStore {

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

        // TODO Read Voldemort config and apply to EmbeddedConfiguraton instance

        EmbeddedConfiguration databaseConfig = Db4oEmbedded.newConfiguration();
        String databasePath = db4oDir + storeName + ".yap";

        StorageEngine<ByteArray, byte[]> store = new Db4oStorageEngine(databasePath, databaseConfig);

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
