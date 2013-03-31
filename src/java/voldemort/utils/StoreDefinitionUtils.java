/*
 * Copyright 2013 LinkedIn, Inc
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

package voldemort.utils;

import java.util.ArrayList;
import java.util.List;

import voldemort.VoldemortException;
import voldemort.store.StoreDefinition;
import voldemort.store.readonly.ReadOnlyStorageConfiguration;

import com.google.common.collect.Lists;

public class StoreDefinitionUtils {

    /**
     * Given a list of store definitions, filters the list depending on the
     * boolean
     * 
     * @param storeDefs Complete list of store definitions
     * @param isReadOnly Boolean indicating whether filter on read-only or not?
     * @return List of filtered store definition
     */
    public static List<StoreDefinition> filterStores(List<StoreDefinition> storeDefs,
                                                     final boolean isReadOnly) {
        List<StoreDefinition> filteredStores = Lists.newArrayList();
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getType().equals(ReadOnlyStorageConfiguration.TYPE_NAME) == isReadOnly) {
                filteredStores.add(storeDef);
            }
        }
        return filteredStores;
    }

    /**
     * Given a list of store definitions return a list of store names
     * 
     * @param storeDefList The list of store definitions
     * @return Returns a list of store names
     */
    public static List<String> getStoreNames(List<StoreDefinition> storeDefList) {
        List<String> storeList = new ArrayList<String>();
        for(StoreDefinition def: storeDefList) {
            storeList.add(def.getName());
        }
        return storeList;
    }

    /**
     * Given a store name and a list of store definitions, returns the
     * appropriate store definition ( if it exists )
     * 
     * @param storeDefs List of store definitions
     * @param storeName The store name whose store definition is required
     * @return The store definition
     */
    public static StoreDefinition getStoreDefinitionWithName(List<StoreDefinition> storeDefs,
                                                             String storeName) {
        StoreDefinition def = null;
        for(StoreDefinition storeDef: storeDefs) {
            if(storeDef.getName().compareTo(storeName) == 0) {
                def = storeDef;
                break;
            }
        }
    
        if(def == null) {
            throw new VoldemortException("Could not find store " + storeName);
        }
        return def;
    }

}
