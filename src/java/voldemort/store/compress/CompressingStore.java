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

package voldemort.store.compress;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.store.AbstractStore;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A Store Decorator that compresses keys and values as it stores them and
 * uncompresses them as it reads them based on the CompressionStrategy objects
 * provided. A {@link NoopCompressionStrategy} can be used if no compression is
 * desired for either keys or values.
 * 
 * Transforms are not compressed
 * 
 * @see CompressionStrategy
 * @see NoopCompressionStrategy
 * @see GzipCompressionStrategy
 */
public class CompressingStore extends AbstractStore<ByteArray, byte[], byte[]> {

    private final Store<ByteArray, byte[], byte[]> innerStore;
    private final CompressionStrategy keysCompressionStrategy;
    private final CompressionStrategy valuesCompressionStrategy;

    public CompressingStore(Store<ByteArray, byte[], byte[]> innerStore,
                            CompressionStrategy keysCompressionStrategy,
                            CompressionStrategy valuesCompressionStrategy) {
        super(innerStore.getName());
        this.keysCompressionStrategy = Utils.notNull(keysCompressionStrategy);
        this.valuesCompressionStrategy = Utils.notNull(valuesCompressionStrategy);
        this.innerStore = Utils.notNull(innerStore);
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        StoreUtils.assertValidKeys(keys);
        Iterable<ByteArray> processedKeys = keys;
        List<ByteArray> deflatedKeys = Lists.newArrayList();
        for(ByteArray key: keys)
            deflatedKeys.add(deflateKey(key));
        processedKeys = deflatedKeys;
        Map<ByteArray, byte[]> newTransforms = Maps.newHashMap();
        if(transforms != null) {
            for(Map.Entry<ByteArray, byte[]> transform: transforms.entrySet()) {
                newTransforms.put(deflateKey(transform.getKey()), transform.getValue());
            }
        } else {
            for(ByteArray deflatedKey: processedKeys) {
                newTransforms.put(deflatedKey, null);
            }
        }
        Map<ByteArray, List<Versioned<byte[]>>> deflatedResult = innerStore.getAll(processedKeys,
                                                                                   newTransforms);
        Map<ByteArray, List<Versioned<byte[]>>> result = Maps.newHashMapWithExpectedSize(deflatedResult.size());
        for(Map.Entry<ByteArray, List<Versioned<byte[]>>> mapEntry: deflatedResult.entrySet())
            result.put(inflateKey(mapEntry.getKey()), inflateValues(mapEntry.getValue()));
        return result;
    }

    private ByteArray inflateKey(ByteArray key) {
        byte[] inflated = inflate(keysCompressionStrategy, key.get());
        /* This usually means that keys are not compressed */
        if(inflated == key.get())
            return key;
        return new ByteArray(inflated);
    }

    private ByteArray deflateKey(ByteArray key) {
        byte[] deflated = deflate(keysCompressionStrategy, key.get());
        /* This usually means that keys are not compressed */
        if(deflated == key.get())
            return key;
        return new ByteArray(deflated);
    }

    private Versioned<byte[]> deflateValue(Versioned<byte[]> versioned) {
        return new Versioned<byte[]>(deflate(valuesCompressionStrategy, versioned.getValue()),
                                     versioned.getVersion());
    }

    private Versioned<byte[]> inflateValue(Versioned<byte[]> versioned) {
        return new Versioned<byte[]>(inflate(valuesCompressionStrategy, versioned.getValue()),
                                     versioned.getVersion());
    }

    private byte[] inflate(CompressionStrategy compressionStrategy, byte[] data)
            throws VoldemortException {
        try {
            return compressionStrategy.inflate(data);
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    private byte[] deflate(CompressionStrategy compressionStrategy, byte[] data)
            throws VoldemortException {
        try {
            return compressionStrategy.deflate(data);
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return inflateValues(innerStore.get(deflateKey(key), transforms));
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return innerStore.getVersions(deflateKey(key));
    }

    private List<Versioned<byte[]>> inflateValues(List<Versioned<byte[]>> result) {
        List<Versioned<byte[]>> inflated = new ArrayList<Versioned<byte[]>>(result.size());
        for(Versioned<byte[]> item: result)
            inflated.add(inflateValue(item));

        return inflated;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        StoreUtils.assertValidKey(key);
        innerStore.put(deflateKey(key), deflateValue(value), transforms);
    }

    @Override
    public void close() throws VoldemortException {
        innerStore.close();
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        return innerStore.getCapability(capability);
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        StoreUtils.assertValidKey(key);
        return innerStore.delete(deflateKey(key), version);
    }
}
