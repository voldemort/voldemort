package voldemort.rest.coordinator.config;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.util.Utf8;

import com.google.common.collect.Maps;

/**
 * Class with static functions for interacting with store client config files.
 */
public class ClientConfigUtil {

    public final static String CLIENT_CONFIG_AVRO_SCHEMA_STRING = "{ \"name\": \"clientConfig\", \"type\": \"map\", \"values\": \"string\" }";
    public final static Schema CLIENT_CONFIG_AVRO_SCHEMA = Schema.parse(CLIENT_CONFIG_AVRO_SCHEMA_STRING);
    public final static String CLIENT_CONFIGS_AVRO_SCHEMA_STRING = "{ \"name\": \"clientConfigs\", \"type\": \"map\", \"values\": "
                                                                   + CLIENT_CONFIG_AVRO_SCHEMA_STRING
                                                                   + "}";
    public final static Schema CLIENT_CONFIGS_AVRO_SCHEMA = Schema.parse(CLIENT_CONFIGS_AVRO_SCHEMA_STRING);

    /**
     * Parses a string that contains single fat client config string in avro
     * format
     * 
     * @param configAvro Input string of avro format, that contains config for
     *        multiple stores
     * @return Properties of single fat client config
     */
    @SuppressWarnings("unchecked")
    public static Properties readSingleClientConfigAvro(String configAvro) {
        Properties props = new Properties();
        try {
            JsonDecoder decoder = new JsonDecoder(CLIENT_CONFIG_AVRO_SCHEMA, configAvro);
            GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(CLIENT_CONFIG_AVRO_SCHEMA);
            Map<Utf8, Utf8> flowMap = (Map<Utf8, Utf8>) datumReader.read(null, decoder);
            for(Utf8 key: flowMap.keySet()) {
                props.put(key.toString(), flowMap.get(key).toString());
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return props;
    }

    /**
     * Parses a string that contains multiple fat client configs in avro format
     * 
     * @param configAvro Input string of avro format, that contains config for
     *        multiple stores
     * @return Map of store names to store config properties
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Properties> readMultipleClientConfigAvro(String configAvro) {
        Map<String, Properties> mapStoreToProps = Maps.newHashMap();
        try {
            JsonDecoder decoder = new JsonDecoder(CLIENT_CONFIGS_AVRO_SCHEMA, configAvro);
            GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>(CLIENT_CONFIGS_AVRO_SCHEMA);

            Map<Utf8, Map<Utf8, Utf8>> storeConfigs = (Map<Utf8, Map<Utf8, Utf8>>) datumReader.read(null,
                                                                                                    decoder);
            // Store config props to return back
            for(Utf8 storeName: storeConfigs.keySet()) {
                Properties props = new Properties();
                Map<Utf8, Utf8> singleConfig = storeConfigs.get(storeName);

                for(Utf8 key: singleConfig.keySet()) {
                    props.put(key.toString(), singleConfig.get(key).toString());
                }

                if(storeName == null || storeName.length() == 0) {
                    throw new Exception("Invalid store name found!");
                }

                mapStoreToProps.put(storeName.toString(), props);
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
        return mapStoreToProps;
    }

    /**
     * Assembles an avro format string of single store config from store
     * properties
     * 
     * @param props Store properties
     * @return String in avro format that contains single store configs
     */
    public static String writeSingleClientConfigAvro(Properties props) {
        // TODO: Use a dedicated json lib. We shouldn't be manually manipulating json...
        String avroConfig = "";
        Boolean firstProp = true;
        for(String key: props.stringPropertyNames()) {
            if(firstProp) {
                firstProp = false;
            } else {
                avroConfig = avroConfig + ",\n";
            }
            avroConfig = avroConfig + "\t\t\"" + key + "\": \"" + props.getProperty(key) + "\"";
        }
        if(avroConfig.isEmpty()) {
            return "{}";
        } else {
            return "{\n" + avroConfig + "\n\t}";
        }
    }

    /**
     * Assembles an avro format string that contains multiple fat client configs
     * from map of store to properties
     * 
     * @param mapStoreToProps A map of store names to their properties
     * @return Avro string that contains multiple store configs
     */
    public static String writeMultipleClientConfigAvro(Map<String, Properties> mapStoreToProps) {
        // TODO: Use a dedicated json lib. We shouldn't be manually manipulating json...
        String avroConfig = "";
        Boolean firstStore = true;
        for(String storeName: mapStoreToProps.keySet()) {
            if(firstStore) {
                firstStore = false;
            } else {
                avroConfig = avroConfig + ",\n";
            }
            Properties props = mapStoreToProps.get(storeName);
            avroConfig = avroConfig + "\t\"" + storeName + "\": "
                         + writeSingleClientConfigAvro(props);

        }
        return "{\n" + avroConfig + "\n}";
    }

    /**
     * Compares two avro strings which contains single store configs
     * 
     * @param configAvro1
     * @param configAvro2
     * @return true if two config avro strings have same content
     */
    public static Boolean compareSingleClientConfigAvro(String configAvro1, String configAvro2) {
        Properties props1 = readSingleClientConfigAvro(configAvro1);
        Properties props2 = readSingleClientConfigAvro(configAvro2);
        if(props1.equals(props2)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Compares two avro strings which contains multiple store configs
     * 
     * @param configAvro1
     * @param configAvro2
     * @return true if two config avro strings have same content
     */
    public static Boolean compareMultipleClientConfigAvro(String configAvro1, String configAvro2) {
        Map<String, Properties> mapStoreToProps1 = readMultipleClientConfigAvro(configAvro1);
        Map<String, Properties> mapStoreToProps2 = readMultipleClientConfigAvro(configAvro2);
        Set<String> keySet1 = mapStoreToProps1.keySet();
        Set<String> keySet2 = mapStoreToProps2.keySet();
        if(!keySet1.equals(keySet2)) {
            return false;
        }
        for(String storeName: keySet1) {
            Properties props1 = mapStoreToProps1.get(storeName);
            Properties props2 = mapStoreToProps2.get(storeName);
            if(!props1.equals(props2)) {
                return false;
            }
        }
        return true;
    }
}
