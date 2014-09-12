package voldemort.rest.coordinator;

import com.google.common.collect.Maps;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.util.Utf8;

import java.util.Map;
import java.util.Properties;

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

    // TODO: Remove STORE_NAME_KEY, as it is useless now.
    static final String STORE_NAME_KEY = "store_name";
    static final String IDENTIFIER_STRING_KEY = "identifier_string";

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

            // FIXME: This is never going to be contained in the config itself... Probably should be removed.
            String storeName = props.getProperty(STORE_NAME_KEY);
            if(storeName == null || storeName.length() == 0) {
                throw new Exception("Invalid store name found!");
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


            Map<Utf8, Map<Utf8, Utf8>> storeConfigs = (Map<Utf8, Map<Utf8, Utf8>>) datumReader.read(null, decoder);
            // Store config props to return back
            for (Utf8 storeName: storeConfigs.keySet()) {
                Properties props = new Properties();
                Map<Utf8, Utf8> singleConfig = storeConfigs.get(storeName);

                for (Utf8 key: singleConfig.keySet()) {
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
        String avroConfig = new String();
        Boolean firstProp = true;
        for(String key: props.stringPropertyNames()) {
            if(firstProp) {
                firstProp = false;
            } else {
                avroConfig = avroConfig + ", ";
            }
            avroConfig = avroConfig + "\"" + key + "\": \"" + props.getProperty(key) + "\"";
        }
        return "{" + avroConfig + "}";
    }

    /**
     * Assembles an avro format string that contains multiple fat client configs
     * from map of store to properties
     *
     * @param mapStoreToProps A map of store names to their properties
     * @return Avro string that contains multiple store configs
     */
    public static String writeMultipleClientConfigAvro(Map<String, Properties> mapStoreToProps) {
        String avroConfig = new String();
        Boolean firstStore = true;
        for(String storeName: mapStoreToProps.keySet()) {
            if(firstStore) {
                firstStore = false;
            } else {
                avroConfig = avroConfig + ", ";
            }
            Properties props = mapStoreToProps.get(storeName);
            avroConfig = avroConfig + "\"" + storeName + "\": "
                         + writeSingleClientConfigAvro(props);

        }
        return "{" + avroConfig + "}";
    }
}
