package voldemort.store.views;

import java.util.List;

import voldemort.VoldemortException;
import voldemort.routing.RoutingStrategy;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.SerializerFactory;
import voldemort.server.StoreRepository;
import voldemort.server.VoldemortConfig;
import voldemort.store.StorageConfiguration;
import voldemort.store.StorageEngine;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreUtils;
import voldemort.store.compress.CompressionStrategy;
import voldemort.store.compress.CompressionStrategyFactory;
import voldemort.utils.ByteArray;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

public class ViewStorageConfiguration implements StorageConfiguration {

    public final static String TYPE_NAME = "view";

    private StoreRepository storeRepo;
    private List<StoreDefinition> storeDefs;

    @SuppressWarnings("unused")
    public ViewStorageConfiguration(VoldemortConfig config,
                                    List<StoreDefinition> stores,
                                    StoreRepository repo) {
        this.storeDefs = Utils.notNull(stores);
        this.storeRepo = repo;
    }

    public void close() {}

    public StorageEngine<ByteArray, byte[], byte[]> getStore(StoreDefinition storeDef,
                                                             RoutingStrategy strategy) {
        String name = storeDef.getName();
        StoreDefinition def = StoreUtils.getStoreDef(storeDefs, name);
        String targetName = def.getViewTargetStoreName();
        StoreDefinition targetDef = StoreUtils.getStoreDef(storeDefs, targetName);
        StorageEngine<ByteArray, byte[], byte[]> target = storeRepo.getStorageEngine(targetName);
        if(target == null)
            throw new VoldemortException("View \"" + name + "\" has a target store \"" + targetName
                                         + "\" which does not exist.");
        String factoryName = def.getSerializerFactory();
        SerializerFactory factory;
        if(factoryName == null)
            factory = new DefaultSerializerFactory();
        else
            factory = loadSerializerFactory(factoryName);

        CompressionStrategy valueCompressionStrategy = null;
        if(targetDef.getValueSerializer().hasCompression()) {
            valueCompressionStrategy = new CompressionStrategyFactory().get(targetDef.getValueSerializer()
                                                                                     .getCompression());
        }

        View<?, ?, ?, ?> view = loadTransformation(def.getValueTransformation());

        return new ViewStorageEngine(name,
                                     target,
                                     factory.getSerializer(def.getValueSerializer()),
                                     def.getTransformsSerializer() != null ? factory.getSerializer(def.getTransformsSerializer())
                                                                          : null,
                                     factory.getSerializer(targetDef.getKeySerializer()),
                                     factory.getSerializer(targetDef.getValueSerializer()),
                                     valueCompressionStrategy,
                                     view);
    }

    public String getType() {
        return TYPE_NAME;
    }

    public static SerializerFactory loadSerializerFactory(String className) {
        if(className == null)
            return null;
        Class<?> factoryClass = ReflectUtils.loadClass(className.trim());
        return (SerializerFactory) ReflectUtils.callConstructor(factoryClass, new Object[] {});
    }

    public static View<?, ?, ?, ?> loadTransformation(String className) {
        if(className == null)
            return null;
        Class<?> viewClass = ReflectUtils.loadClass(className.trim());
        return (View<?, ?, ?, ?>) ReflectUtils.callConstructor(viewClass, new Object[] {});
    }

    public void update(StoreDefinition storeDef) {
        throw new UnsupportedViewOperationException("Storage config updates not permitted for "
                                                    + this.getClass().getCanonicalName());
    }
}
