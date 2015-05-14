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

package voldemort.store;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

public class ForceFailStore<K, V, T> extends DelegatingStore<K, V, T> {

    private final static Logger logger = Logger.getLogger(ForceFailStore.class);
    private final VoldemortException e;

    private volatile boolean fail = false;

    public ForceFailStore(Store<K, V, T> innerStore, VoldemortException e) {
        super(innerStore);
        this.e = e;
    }

    public void setFail(boolean fail) {
        this.fail = fail;
    }

    @Override
    public void put(K key, Versioned<V> value, T transform) throws VoldemortException {
        if(fail) {
            if(logger.isDebugEnabled()) {
                logger.debug("PUT key " + key + " was forced to fail");
            }
            throw e;
        }

        getInnerStore().put(key, value, transform);
    }

    @Override
    public boolean delete(K key, Version version) throws VoldemortException {
        if(fail) {
            if(logger.isDebugEnabled()) {
                logger.debug("DELETE key " + key + " was forced to fail");
            }
            throw e;
        }

        return getInnerStore().delete(key, version);
    }

    @Override
    public Map<K, List<Versioned<V>>> getAll(Iterable<K> keys, Map<K, T> transforms)
            throws VoldemortException {
        if(fail) {
            if(logger.isDebugEnabled()) {
                logger.debug("GETALL was forced to fail");
            }
            throw e;
        }

        return getInnerStore().getAll(keys, transforms);
    }

    @Override
    public List<Versioned<V>> get(K key, T transform) throws VoldemortException {
        if(fail) {
            if(logger.isDebugEnabled()) {
                logger.debug("GET key " + key + " was forced to fail");
            }
            throw e;
        }

        return getInnerStore().get(key, transform);
    }
}
