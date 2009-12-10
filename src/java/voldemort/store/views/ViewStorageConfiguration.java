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
import voldemort.utils.ByteArray;
import voldemort.utils.Utils;

public class ViewStorageConfiguration implements StorageConfiguration {

    public static String TYPE = "view";

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

    public StorageEngine<ByteArray, byte[]> getStore(String name) {
        StoreDefinition def = StoreUtils.getStoreDef(storeDefs, name);
        String targetName = def.getViewTargetStoreName();
        StoreDefinition targetDef = StoreUtils.getStoreDef(storeDefs, targetName);
        StorageEngine<ByteArray, byte[]> target = storeRepo.getStorageEngine(targetName);
        if(target == null)
            throw new VoldemortException("View \"" + name + "\" has a target store \"" + targetName
                                         + "\" which does not exist.");
        SerializerFactory factory = new DefaultSerializerFactory();
        return new ViewStorageEngine(name,
                                     target,
                                     factory.getSerializer(def.getValueSerializer()),
                                     factory.getSerializer(targetDef.getKeySerializer()),
                                     factory.getSerializer(targetDef.getValueSerializer()),
                                     def.getValueTransformation());
    }

    public String getType() {
        return TYPE;
    }

}
