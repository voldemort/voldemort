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

package voldemort.store.gzip;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

import voldemort.VoldemortException;
import voldemort.store.DelegatingStore;
import voldemort.store.Store;
import voldemort.versioning.Versioned;

/**
 * A Store decorator that gzips and ungzips its contents as it stores them
 * 
 * 
 */
public class GzipStore<K> extends DelegatingStore<K, byte[], byte[]> {

    public GzipStore(Store<K, byte[], byte[]> innerStore) {
        super(innerStore);
    }

    @Override
    public List<Versioned<byte[]>> get(K key, byte[] transforms) throws VoldemortException {
        List<Versioned<byte[]>> found = getInnerStore().get(key, transforms);
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>(found.size());
        try {
            for(Versioned<byte[]> item: found)
                results.add(new Versioned<byte[]>(IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(item.getValue()))),
                                                  item.getVersion()));
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        return results;
    }

    @Override
    public void put(K key, Versioned<byte[]> value, byte[] transforms) throws VoldemortException {
        try {
            getInnerStore().put(key,
                                new Versioned<byte[]>(IOUtils.toByteArray(new GZIPInputStream(new ByteArrayInputStream(value.getValue()))),
                                                      value.getVersion()),
                                transforms);
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

}
