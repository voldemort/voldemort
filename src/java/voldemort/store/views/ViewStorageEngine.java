package voldemort.store.views;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.annotations.Experimental;
import voldemort.serialization.Serializer;
import voldemort.store.StorageEngine;
import voldemort.store.Store;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
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
public class ViewStorageEngine implements StorageEngine<ByteArray, byte[], byte[]> {

    private final String name;
    private final Store<Object, Object, Object> serializingStore;
    private final StorageEngine<ByteArray, byte[], byte[]> target;
    private final Serializer<Object> valSerializer;
    private final Serializer<Object> transformSerializer;
    private final Serializer<Object> targetKeySerializer;
    private final Serializer<Object> targetValSerializer;
    private final View<Object, Object, Object, Object> view;

    @SuppressWarnings("unchecked")
    public ViewStorageEngine(String name,
                             StorageEngine<ByteArray, byte[], byte[]> target,
                             Serializer<?> valSerializer,
                             Serializer<?> transformSerializer,
                             Serializer<?> targetKeySerializer,
                             Serializer<?> targetValSerializer,
                             View<?, ?, ?, ?> valueTrans) {
        this.name = name;
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
        if(valueTrans == null)
            throw new IllegalArgumentException("View without either a key transformation or a value transformation.");
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return target.delete(key, version);
    }

    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        List<Versioned<byte[]>> values = target.get(key, null);
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();
        for(Versioned<byte[]> v: values) {
            results.add(new Versioned<byte[]>(valueToViewSchema(key, v.getValue(), transforms),
                                              v.getVersion()));
        }
        return results;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {
        return StoreUtils.getAll(this, keys, transforms);
    }

    public String getName() {
        return name;
    }

    public List<Version> getVersions(ByteArray key) {
        return target.getVersions(key);
    }

    public void put(ByteArray key, Versioned<byte[]> value, byte[] transforms)
            throws VoldemortException {
        target.put(key, Versioned.value(valueFromViewSchema(key, value.getValue(), transforms),
                                        value.getVersion()), null);
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new ViewIterator(target.entries());
    }

    public ClosableIterator<ByteArray> keys() {
        return StoreUtils.keys(entries());
    }

    public void truncate() {
        ViewIterator iterator = new ViewIterator(target.entries());
        while(iterator.hasNext()) {
            Pair<ByteArray, Versioned<byte[]>> pair = iterator.next();
            target.delete(pair.getFirst(), pair.getSecond().getVersion());
        }
    }

    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.VIEW_TARGET)
            return this.target;
        else
            return null;
    }

    public void close() throws VoldemortException {}

    private byte[] valueFromViewSchema(ByteArray key, byte[] value, byte[] transforms) {
        return this.targetValSerializer.toBytes(this.view.viewToStore(this.serializingStore,
                                                                      this.targetKeySerializer.toObject(key.get()),
                                                                      this.valSerializer.toObject(value),
                                                                      this.transformSerializer != null
                                                                              && transforms != null ? this.transformSerializer.toObject(transforms)
                                                                                                   : null));
    }

    private byte[] valueToViewSchema(ByteArray key, byte[] value, byte[] transforms) {
        return this.valSerializer.toBytes(this.view.storeToView(this.serializingStore,
                                                                this.targetKeySerializer.toObject(key.get()),
                                                                this.targetValSerializer.toObject(value),
                                                                this.transformSerializer != null
                                                                        && transforms != null ? this.transformSerializer.toObject(transforms)
                                                                                             : null));
    }

    private class ViewIterator extends AbstractIterator<Pair<ByteArray, Versioned<byte[]>>>
            implements ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> {

        private final ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> inner;

        public ViewIterator(ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> inner) {
            this.inner = inner;
        }

        public void close() {
            this.inner.close();
        }

        @Override
        protected Pair<ByteArray, Versioned<byte[]>> computeNext() {
            Pair<ByteArray, Versioned<byte[]>> p = inner.next();
            Versioned<byte[]> newVal = Versioned.value(valueToViewSchema(p.getFirst(),
                                                                         p.getSecond().getValue(),
                                                                         null), p.getSecond()
                                                                                 .getVersion());
            return Pair.create(p.getFirst(), newVal);
        }
    }
}
