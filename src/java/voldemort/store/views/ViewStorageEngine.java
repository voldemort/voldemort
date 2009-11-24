package voldemort.store.views;

import java.util.List;
import java.util.Map;

import voldemort.VoldemortException;
import voldemort.serialization.Serializer;
import voldemort.store.StorageEngine;
import voldemort.store.StoreCapabilityType;
import voldemort.store.StoreUtils;
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
 * @author jay
 * 
 */
public class ViewStorageEngine implements StorageEngine<ByteArray, byte[]> {

    private final String name;
    private final StorageEngine<ByteArray, byte[]> target;
    private final Serializer<Object> keySerializer;
    private final Serializer<Object> valSerializer;
    private final Serializer<Object> targetKeySerializer;
    private final Serializer<Object> targetValSerializer;
    private final ViewTransformation<Object, Object> keyTrans;
    private final ViewTransformation<Object, Object> valueTrans;

    @SuppressWarnings("unchecked")
    public ViewStorageEngine(String name,
                             StorageEngine<ByteArray, byte[]> target,
                             Serializer<?> keySerializer,
                             Serializer<?> valSerializer,
                             Serializer<?> targetKeySerializer,
                             Serializer<?> targetValSerializer,
                             ViewTransformation<?, ?> keyTrans,
                             ViewTransformation<?, ?> valueTrans) {
        this.name = name;
        this.target = Utils.notNull(target);
        this.keySerializer = (Serializer<Object>) keySerializer;
        this.valSerializer = (Serializer<Object>) valSerializer;
        this.targetKeySerializer = (Serializer<Object>) targetKeySerializer;
        this.targetValSerializer = (Serializer<Object>) targetValSerializer;
        this.keyTrans = (ViewTransformation<Object, Object>) keyTrans;
        this.valueTrans = (ViewTransformation<Object, Object>) valueTrans;
        if(keyTrans == null && valueTrans == null)
            throw new IllegalArgumentException("View without either a key transformation or a value transformation.");
    }

    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        return target.delete(keyFromViewSchema(key), version);
    }

    public List<Versioned<byte[]>> get(ByteArray key) throws VoldemortException {
        List<Versioned<byte[]>> values = target.get(keyFromViewSchema(key));
        for(Versioned<byte[]> v: values)
            v.setObject(valueToViewSchema(v.getValue()));
        return values;
    }

    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys)
            throws VoldemortException {
        return StoreUtils.getAll(this, keys);
    }

    public String getName() {
        return name;
    }

    public List<Version> getVersions(ByteArray key) {
        return target.getVersions(keyFromViewSchema(key));
    }

    public void put(ByteArray key, Versioned<byte[]> value) throws VoldemortException {
        target.put(keyFromViewSchema(key), Versioned.value(valueFromViewSchema(value.getValue()),
                                                           value.getVersion()));
    }

    public ClosableIterator<Pair<ByteArray, Versioned<byte[]>>> entries() {
        return new ViewIterator(target.entries());
    }

    public Object getCapability(StoreCapabilityType capability) {
        if(capability == StoreCapabilityType.VIEW_TARGET)
            return this.target;
        else
            return null;
    }

    public void close() throws VoldemortException {}

    private ByteArray keyFromViewSchema(ByteArray key) {
        if(keyTrans == null)
            return key;
        else
            return new ByteArray(this.targetKeySerializer.toBytes(this.keyTrans.fromView(this.keySerializer.toObject(key.get()))));
    }

    private ByteArray keyToViewSchema(ByteArray key) {
        if(this.keyTrans == null)
            return key;
        else
            return new ByteArray(this.keySerializer.toBytes(this.keyTrans.fromView(this.targetKeySerializer.toObject(key.get()))));
    }

    private byte[] valueFromViewSchema(byte[] value) {
        if(this.valueTrans == null)
            return value;
        else
            return this.targetValSerializer.toBytes(this.valueTrans.fromView(this.valSerializer.toObject(value)));
    }

    private byte[] valueToViewSchema(byte[] value) {
        if(this.valueTrans == null)
            return value;
        else
            return this.valSerializer.toBytes(this.valueTrans.fromStore(this.targetValSerializer.toObject(value)));
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
            ByteArray newKey = keyToViewSchema(p.getFirst());
            Versioned<byte[]> newVal = Versioned.value(valueToViewSchema(p.getSecond().getValue()),
                                                       p.getSecond().getVersion());
            return Pair.create(newKey, newVal);
        }
    }
}
