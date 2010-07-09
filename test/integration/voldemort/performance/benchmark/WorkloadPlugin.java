/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.performance.benchmark;

/**
 * Abstract class for benchmark plugins which specify custom/compound 
 * operations
 */
public abstract class WorkloadPlugin {
    private volatile VoldemortWrapper db;

    public WorkloadPlugin() {
    }

    protected VoldemortWrapper getDb() {
        return db;
    }

    protected void setDb(VoldemortWrapper db) {
        this.db = db;
    }

    public boolean doTransaction(String op) {
        return doTransaction(op, db);
    }

    /**
     * Plugins should override this method with code performing
     * their custom transactions e.g., appending to a list and then retrieving
     * the appended object.
     *
     * @param op Operation to perform
     * @param db Instantiated {@link VoldemortWrapper} instance
     * @return true if transaction succeeded, false otherwise
     */
    public abstract boolean doTransaction(String op, VoldemortWrapper db);

    /**
     * Plugins should override this method with code performing record population
     * e.g., populating a store with lists corresponding to keys related to the
     * value
     *
     * @param key Key to write
     * @param value Value to write
     * @return true if write succeeded, false otherwise
     */
    public abstract boolean doWrite(Object key, String value);
}
