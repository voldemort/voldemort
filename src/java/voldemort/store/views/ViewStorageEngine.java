package voldemort.store.views;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.annotations.Experimental;
import voldemort.serialization.Serializer;
import voldemort.store.AbstractStorageEngine;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.serialized.SerializingStore;
import voldemort.utils.ByteArray;
import voldemort.utils.ClosableIterator;
import voldemort.utils.Pair;
import voldemort.utils.Utils;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

import com.google.common.collect.AbstractIterator;

/**
 * Views are transformations of other stores
 * 
 * 
 */
@Experimental
public class ViewStorageEngine extends AbstractStorageEngine<ByteArray, byte[], byte[]> {

    private final Store<Object, Object, Object> serializingStore;
    private final StorageEngine<ByteArray, byte[], byte[]> target;
    private final Serializer<Object> valSerializer;
    private final Serializer<Object> transformSerializer;
    private final Serializer<Object> targetKeySerializer;
    private final Serializer<Object> targetValSerializer;
    private final View<Object, Object, Object, Object> view;
    private final CompressionStrategy valueCompressionStrategy;

    @SuppressWarnings("unchecked")
    public ViewStorageEngine(String name,
                             StorageEngine<ByteArray, byte[], byte[]> target,
                             Serializer<?> valSerializer,
                             Serializer<?> transformSerializer,
                             Serializer<?> targetKeySerializer,
                             Serializer<?> targetValSerializer,
                             CompressionStrategy valueCompressionStrategy,
                             View<?, ?, ?, ?> valueTrans) {
        super(name);
        this.target = Utils.notNull(target);
        this.serializingStore = new SerializingStore(target,
                                                     targetKeySerializer,
                                                     targetValSerializer,
                                                     null);
        this.valSerializer = (Serializer<Object>) valSerializer;
        this.transformSerializer = (Serializer<Object>) transformSerializer;
        this.targetKeySerializer = (Serializer<Object>) targetKeySerializer;
        this.targetValSerializer = (Serializer<Object>) targetValSerializer;
        this.view = (View<Object, Object, Object, Object>) valueTrans;
        this.valueCompressionStrategy = valueCompressionStrategy;
        if(valueTrans == null)
            throw new IllegalArgumentException("View without either a key transformation or a value transformation.");
    }

    private List<Versioned<byte[]>> inflateValues(List<Versioned<byte[]>> result) {
        List<Versioned<byte[]>> inflated = new ArrayList<Versioned<byte[]>>(result.size());
        for(Versioned<byte[]> item: result) {
            inflated.add(inflateValue(item));
        }
        return inflated;
    }

    private List<Versioned<byte[]>> deflateValues(List<Versioned<byte[]>> values) {
        List<Versioned<byte[]>> deflated = new ArrayList<Versioned<byte[]>>(values.size());
        for(Versioned<byte[]> item: values) {
            deflated.add(deflateValue(item));
        }
        return deflated;
    }

    private Versioned<byte[]> deflateValue(Versioned<byte[]> versioned) throws VoldemortException {
        byte[] deflatedData = null;
        try {
            deflatedData = valueCompressionStrategy.deflate(versioned.getValue());
        } catch(IOException e) {
            throw new VoldemortException(e);
        }

        return new Versioned<byte[]>(deflatedData, versioned.getVersion());
    }

    private Versioned<byte[]> inflateValue(Versioned<byte[]> versioned) throws VoldemortException {
        byte[] inflatedData = null;
        try {
            inflatedData = valueCompressionStrategy.inflate(versioned.getValue());
        } catch(IOException e) {
            throw new VoldemortException(e);
        }
        return new Versioned<byte[]>(inflatedData, versioned.getVersion());
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return target.delete(key, version);
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        List<Versioned<byte[]>> values = target.get(key, null);

        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();

        if(valueCompressionStrategy != null)
            values = inflateValues(values);

        for(Versioned<byte[]> v: values) {
            results.add(new Versioned<byte[]>(valueToViewSchema(key, v.getValue(), transforms),
                                              v.getVersion()));
        }

        if(valueCompressionStrategy != null)
            results = deflateValues(results);

        return results;
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        return StoreUtils.getAll(this, keys, transforms);
    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        return target.getVersions(key);
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        if(valueCompressionStrategy != null)
            value = inflateValue(value);
        Versioned<byte[]> result = Versioned.value(valueFromViewSchema(key,
                                                                       value.getValue(),
                                                                       transforms),
                                                   value.getVersion());
        if(valueCompressionStrategy != null)
            result = deflateValue(result);
        target.put(key, result, null);
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new ViewIterator(target.entries());
    }

    @Override
    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    @Override
    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries(int partition) {
        return new ViewIterator(target.entries(partition));
    }

    @Override
    public ClosableIterator<ByteArray> keys(int partition) {
        return StoreUtils.keys(entries(partition));
    }

    @Override
    public void truncate() {
        ViewIterator iterator = new ViewIterator(target.entries());
        while(iterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> pair = iterator.next();
            target.delete(pair.getFirst(), pair.getSecond().getVersion());
        }
    }

    @Override
    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.VIEW_TARGET)
            return this.target;
        else
            return null;
    }

    // public void close() throws VoldemortException {}

    private byte[] valueFromViewSchema(ByteArray key, byte[] value, byte[] transforms) {
        return this.targetValSerializer.toBytes(this.view.viewToStore(this.serializingStore,
                                                                      this.targetKeySerializer.toObject(key.get()),
                                                                      this.valSerializer.toObject(value),
                                                                      (transformSerializer != null && transforms != null) ? this.transformSerializer.toObject(transforms)
                                                                                                                         : null));
    }

    private byte[] valueToViewSchema(ByteArray key, byte[] value, byte[] transforms) {
        return this.valSerializer.toBytes(this.view.storeToView(this.serializingStore,
                                                                this.targetKeySerializer.toObject(key.get()),
                                                                this.targetValSerializer.toObject(value),
                                                                (transformSerializer != null && transforms != null) ? this.transformSerializer.toObject(transforms)
                                                                                                                   : null));
    }

    private class ViewIterator extends AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>
            implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private final ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> inner;

        public ViewIterator(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> inner) {
            this.inner = inner;
        }

        @Override
        public void close() {
            this.inner.close();
        }

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
            Pair<ByteArray, Versioned<byte[]>> p = inner.next();
            Versioned<byte[]> newVal = Versioned.value(valueToViewSchema(p.getFirst(),
                                                                         p.getSecond().getValue(),
                                                                         null),
                                                       p.getSecond().getVersion());
            return Pair.create(p.getFirst(), newVal);
        }
    }

    @Override
    public boolean isPartitionAware() {
        return target.isPartitionAware();
    }

    @Override
    public boolean isPartitionScanSupported() {
        return target.isPartitionScanSupported();
    }
}
