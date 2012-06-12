package voldemort.store.system;

import java.io.StringReader;
import java.util.List;

import voldemort.store.StoreDefinition;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * The various system stores
 */
public class SystemStoreConstants {

    public static final String NAME_PREFIX = "voldsys$_";

    public static enum SystemStoreName {
        voldsys$_client_registry,
        voldsys$_client_store_definition,
        voldsys$_metadata_version;
    }

    public static final String SYSTEM_STORE_SCHEMA = "<stores>"
                                                     + "  <store>"
                                                     + "    <name>voldsys$_client_registry</name>"
                                                     + "    <routing-strategy>zone-routing</routing-strategy>"
                                                     + "    <hinted-handoff-strategy>proximity-handoff</hinted-handoff-strategy>"
                                                     + "    <persistence>memory</persistence>"
                                                     + "    <routing>client</routing>"
                                                     + "    <replication-factor>4</replication-factor>"
                                                     + "    <zone-replication-factor>"
                                                     + "      <replication-factor zone-id=\"0\">2</replication-factor>"
                                                     + "      <replication-factor zone-id=\"1\">2</replication-factor>"
                                                     + "    </zone-replication-factor>"
                                                     + "    <required-reads>1</required-reads>"
                                                     + "    <required-writes>1</required-writes>"
                                                     + "    <key-serializer>"
                                                     + "      <type>string</type>"
                                                     + "     <schema-info version=\"0\">utf8</schema-info>"
                                                     + "    </key-serializer>"
                                                     + "    <value-serializer>"
                                                     // +
                                                     // "      <type>avro-specific</type>"
                                                     // +
                                                     // "      <schema-info version=\"0\">java=voldemort.client.ClientInfo</schema-info>"
                                                     + "      <type>java-serialization</type>"
                                                     + "    </value-serializer>"
                                                     + "    <retention-days>7</retention-days>"
                                                     + "  </store>"

                                                     + "  <store>"
                                                     + "    <name>voldsys$_client_store_definition</name>"
                                                     + "    <routing-strategy>zone-routing</routing-strategy>"
                                                     + "    <hinted-handoff-strategy>proximity-handoff</hinted-handoff-strategy>"
                                                     + "    <persistence>memory</persistence>"
                                                     + "    <routing>client</routing>"
                                                     + "    <replication-factor>1</replication-factor>"
                                                     + "    <required-reads>1</required-reads>"
                                                     + "    <required-writes>1</required-writes>"
                                                     + "    <key-serializer>"
                                                     + "      <type>string</type>"
                                                     + "    </key-serializer>"
                                                     + "    <value-serializer>"
                                                     + "      <type>string</type>"
                                                     + "    </value-serializer>"
                                                     + "    <retention-days>7</retention-days>"
                                                     + "  </store>"

                                                     + "  <store>"
                                                     + "    <name>voldsys$_metadata_version</name>"
                                                     + "    <routing-strategy>local-pref-all-routing</routing-strategy>"
                                                     + "    <hinted-handoff-strategy>proximity-handoff</hinted-handoff-strategy>"
                                                     + "    <persistence>memory</persistence>"
                                                     + "    <routing>client</routing>"
                                                     + "    <replication-factor>1</replication-factor>"
                                                     + "    <required-reads>1</required-reads>"
                                                     + "    <required-writes>1</required-writes>"
                                                     + "    <key-serializer>"
                                                     + "      <type>string</type>"
                                                     + "    </key-serializer>"
                                                     + "    <value-serializer>"
                                                     + "      <type>java-serialization</type>"
                                                     + "    </value-serializer>" + "  </store>"

                                                     + "</stores>";

    public static boolean isSystemStore(String storeName) {
        return (null == storeName ? false : storeName.startsWith(NAME_PREFIX));
    }

    public static List<StoreDefinition> getAllSystemStoreDefs() {
        return (new StoreDefinitionsMapper()).readStoreList(new StringReader(SystemStoreConstants.SYSTEM_STORE_SCHEMA));
    }

    public static StoreDefinition getSystemStoreDef(String name) {
        StoreDefinition storeDef = null;
        List<StoreDefinition> allDefs = getAllSystemStoreDefs();
        for(StoreDefinition def: allDefs) {
            if(name.equals(def.getName())) {
                storeDef = def;
            }
        }
        return storeDef;
    }
}