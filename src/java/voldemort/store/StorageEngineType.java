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

package voldemort.store;

/**
 * Enum constant for various Storage Engine types
 * 
 * @author jay
 * 
 */
public enum StorageEngineType {

    /**
     * An engine based on Oracle BDB, the default
     */
    BDB("bdb"),

    /**
     * An engine based on MySQL
     */
    MYSQL("mysql"),

    /**
     * A non-persistent store that uses a ConcurrentHashMap to store data.
     */
    MEMORY("memory"),

    /**
     * A non-persistent store that uses a ConcurrentMap and stores data with
     * soft references; this means that data will be garbage collected when the
     * GC is under memory pressure
     */
    CACHE("cache"),

    /**
     * A filesystem based store that uses a single file for each value
     */
    FILESYSTEM("fs"),

    /**
     * A readonly, file-system store that keeps all data in a large sorted file
     */
    READONLY("read-only");

    private final String text;

    private StorageEngineType(String text) {
        this.text = text;
    }

    public static StorageEngineType fromDisplay(String type) {
        for(StorageEngineType t: StorageEngineType.values())
            if(t.toDisplay().equals(type))
                return t;
        throw new IllegalArgumentException("No StoreType " + type + " exists.");
    }

    public String toDisplay() {
        return text;
    }
};
