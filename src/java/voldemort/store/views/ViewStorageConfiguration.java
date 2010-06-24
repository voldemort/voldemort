package voldemort.store.views;

import java.util.List;

import voldemort.VoldemortException;
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

    public static String TYPE_NAME = "view";

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

    public StorageEngine<ByteArray, byte[], byte[]> getStore(String name) {
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

        /* Check if the values in the target store are compressed */
        CompressionStrategy valueCompressionStrategy = null;
        if(targetDef.getValueSerializer().hasCompression()) {
            valueCompressionStrategy = new CompressionStrategyFactory().get(targetDef.getValueSerializer()
                                                                                     .getCompression());
        }
        return new ViewStorageEngine(name,
                                     target,
                                     factory.getSerializer(def.getValueSerializer()),
                                     def.getTransformsSerializer() != null ? factory.getSerializer(def.getTransformsSerializer())
                                                                          : null,
                                     factory.getSerializer(targetDef.getKeySerializer()),
                                     factory.getSerializer(targetDef.getValueSerializer()),
                                     valueCompressionStrategy,
                                     def.getValueTransformation());
    }

    public String getType() {
        return TYPE_NAME;
    }

    private SerializerFactory loadSerializerFactory(String className) {
        if(className == null)
            return null;
        Class<?> transClass = ReflectUtils.loadClass(className.trim());
        return (SerializerFactory) ReflectUtils.callConstructor(transClass, new Object[] {});
    }

}
