/*
 * Copyright 2009 Geir Magnusson Jr.
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

package voldemort.store.noop;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.NoSuchCapabilityException;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * Implementation of a store that does the least amount possible. It will
 * 'reflect' values sent to it so that it can be tested with real values. It's
 * being done this way to avoid coupling the engine or it's configuration with
 * knowledge of the serializer being used
 * 
 */
public class NoopStorageEngine implements StorageEngine<ByteArray, byte[]> {

    protected String name;
    protected boolean dataReflect;
    protected ByteArray key;
    protected Versioned<byte[]> value;
    protected List<Versioned<byte[]>> dataList = new MyList();
    protected Map<ByteArray, List<Versioned<byte[]>>> dataMap = new MyMap();

    public NoopStorageEngine(String name, boolean reflect) {
        this.name = name;
        this.dataReflect = reflect;
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return null;
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        return dataList;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        return dataMap;
    }

    public List<Version> getVersions(ByteArray key) {
        return StoreUtils.getVersions(get(key));
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {

        if(dataReflect) {
            this.key = key;
            this.value = value;
        }
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return true;
    }

    public String getName() {
        return name;
    }

    public void close() throws VoldemortException {}

    public Object getCapability(StoreCapabilityType capability) {
        throw new NoSuchCapabilityException(capability, getName());
    }

    class MyMap extends HashMap<ByteArray, List<Versioned<byte[]>>> {

        public static final long serialVersionUID = 1;

        @Override
        public List<Versioned<byte[]>> get(Object key) {
            return dataList;
        }
    }

    class MyList extends ArrayList<Versioned<byte[]>> {

        public static final long serialVersionUID = 1;

        @Override
        public Versioned<byte[]> get(int index) {
            return value;
        }

        @Override
        public int size() {
            return value == null ? 0 : 1;
        }
    }
}
