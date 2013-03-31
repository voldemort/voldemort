/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.store.system;

import java.io.StringReader;
import java.util.List;

import voldemort.store.StoreDefinition;
import voldemort.utils.StoreDefinitionUtils;
import voldemort.xml.StoreDefinitionsMapper;

/**
 * A file that defines all constants for system stores, including the store
 * definitions
 * 
 */
public class SystemStoreConstants {

    public static final String NAME_PREFIX = "voldsys$_";

    public static enum SystemStoreName {
        voldsys$_client_registry,
        voldsys$_metadata_version_persistence;
    }

    public static final String SYSTEM_STORE_SCHEMA = "<stores>"
                                                     + "  <store>"
                                                     + "    <name>voldsys$_client_registry</name>"
                                                     + "    <routing-strategy>all-routing</routing-strategy>"
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
                                                     + "    </key-serializer>"
                                                     + "    <value-serializer>"
                                                     + "      <type>string</type>"
                                                     + "    </value-serializer>"
                                                     + "    <retention-days>7</retention-days>"
                                                     + "  </store>"

                                                     + "  <store>"
                                                     + "    <name>voldsys$_metadata_version_persistence</name>"
                                                     + "    <routing-strategy>local-pref-all-routing</routing-strategy>"
                                                     + "    <hinted-handoff-strategy>proximity-handoff</hinted-handoff-strategy>"
                                                     + "    <persistence>file-backed-cache</persistence>"
                                                     + "    <routing>client</routing>"
                                                     + "    <replication-factor>1</replication-factor>"
                                                     + "    <required-reads>1</required-reads>"
                                                     + "    <required-writes>1</required-writes>"
                                                     + "    <key-serializer>"
                                                     + "      <type>string</type>"
                                                     + "    </key-serializer>"
                                                     + "    <value-serializer>"
                                                     + "      <type>string</type>"
                                                     + "    </value-serializer>" + "  </store>"

                                                     + "</stores>";

    public static boolean isSystemStore(String storeName) {
        return (null == storeName ? false : storeName.startsWith(NAME_PREFIX));
    }

    public static List<StoreDefinition> getAllSystemStoreDefs() {
        return (new StoreDefinitionsMapper()).readStoreList(new StringReader(SystemStoreConstants.SYSTEM_STORE_SCHEMA));
    }

    public static StoreDefinition getSystemStoreDef(String name) {
        List<StoreDefinition> allDefs = getAllSystemStoreDefs();
        return StoreDefinitionUtils.getStoreDefinitionWithName(allDefs, name);
    }
}