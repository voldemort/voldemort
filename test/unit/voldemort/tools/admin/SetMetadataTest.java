/**
 * Copyright 2013 LinkedIn, Inc
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
package voldemort.tools.admin;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import voldemort.ClusterTestUtils;
import voldemort.ServerTestUtils;
import voldemort.TestUtils;
import voldemort.VoldemortAdminTool;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.server.VoldemortConfig;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.StoreDefinitionBuilder;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;
import voldemort.utils.MetadataVersionStoreUtils;
import voldemort.versioning.Versioned;
import voldemort.xml.StoreDefinitionsMapper;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

public class SetMetadataTest {

    HashMap<Integer, VoldemortServer> vservers = new HashMap<Integer, VoldemortServer>();
    HashMap<Integer, SocketStoreFactory> socketStoreFactories = new HashMap<Integer, SocketStoreFactory>();
    String bsURL;
    Cluster cluster;
    List<StoreDefinition> oldStores;
    /**
     * This test is to partially test the functionality of SetMetadata feature of the VoldemortAdminTool
     */

    @Before
    public void setup() throws IOException {
        // setup cluster
        cluster = ClusterTestUtils.getZZZCluster();
        oldStores = ClusterTestUtils.getZZZStoreDefsInMemory();
        bsURL = cluster.getNodes().iterator().next().getSocketUrl().toString();

        for(Node node: cluster.getNodes()) {
            SocketStoreFactory ssf = new ClientRequestExecutorPool(2,10000,100000,1024);
            VoldemortConfig config = ServerTestUtils.createServerConfigWithDefs(
                    true,
                    node.getId(),
                    TestUtils.createTempDir().getAbsolutePath(),
                    cluster,
                    oldStores,
                    new Properties()
            );
            VoldemortServer vs = ServerTestUtils.startVoldemortServer(ssf, config, cluster);
            vservers.put(node.getId(), vs);
            socketStoreFactories.put(node.getId(), ssf);
        }
    }

    @Test
    public void testSetMetadataStoreXmlVerifyVersionUpdate() throws Exception {
        // setup new stores xml
        final String newStoreXMLFilePrefix = "updated.stores";
        final String newStoreXMLFileSuffix = "xml";

        List<StoreDefinition> newStores = new ArrayList<StoreDefinition>();
        for(StoreDefinition storeDef: ClusterTestUtils.getZZZStoreDefsInMemory()) {
            StoreDefinitionBuilder sb = storeDefToBuilder(storeDef);
            Map<Integer, Integer> zrf = sb.getZoneReplicationFactor();
            Integer zone0RepFactor = zrf.get(0);
            zrf.remove(0);
            sb.setReplicationFactor(sb.getReplicationFactor() - zone0RepFactor);
            newStores.add(sb.build());
        }

        File newStoresXMLFolder = TestUtils.createTempDir();
        File newStoreXMLFile = File.createTempFile(newStoreXMLFilePrefix, newStoreXMLFileSuffix, newStoresXMLFolder);
        FileWriter fwriter = new FileWriter(newStoreXMLFile);
        fwriter.write(new StoreDefinitionsMapper().writeStoreList(newStores));
        fwriter.close();

        // check version
        String sysStoreName = SystemStoreConstants.SystemStoreName.voldsys$_metadata_version_persistence.name();
        ByteArray metadataKey = new ByteArray(ByteUtils.getBytes(MetadataVersionStoreUtils.VERSIONS_METADATA_KEY, "utf-8"));
        for(VoldemortServer vs: vservers.values()) {
            List<Versioned<byte[]>> result = vs.getStoreRepository().getLocalStore(sysStoreName).get(metadataKey, null);
            String versionInfo = new String(result.get(0).getValue());
            System.out.format("[INITIAL]Version values on node [%d] is: \n %s\n", vs.getIdentityNode().getId(), versionInfo);

            Properties props = new Properties();
            props.load(new ByteArrayInputStream(versionInfo.getBytes()));
            for(StoreDefinition sd: oldStores) {
                if(!props.getProperty(sd.getName()).equals("0")) {
                    Assert.fail("Initial version of key [" + sd.getName() + "] on node [" + vs.getIdentityNode().getId() + "] is expected to be 0 but not");
                }
            }
            if(!props.getProperty("cluster.xml").equals("0")) {
                Assert.fail("Final version of key [stores.xml] on node [" + vs.getIdentityNode().getId() + "] is expected to greater than 0 but not");
            }
        }

        // update the stores xml
        VoldemortAdminTool.main(new String[]{
                "--set-metadata", MetadataStore.STORES_KEY,
                "--set-metadata-value", newStoreXMLFile.getAbsolutePath(),
                "--url", bsURL
        });

        // check version
        for(VoldemortServer vs: vservers.values()) {
            List<Versioned<byte[]>> result = vs.getStoreRepository().getLocalStore(sysStoreName).get(metadataKey, null);
            String versionInfo = new String(result.get(0).getValue());
            System.out.format("[FINAL]Version values on node [%d] is: \n %s\n", vs.getIdentityNode().getId(), versionInfo);

            Properties props = new Properties();
            props.load(new ByteArrayInputStream(versionInfo.getBytes()));
            for(StoreDefinition sd: oldStores) {
                if(!(Long.parseLong(props.getProperty(sd.getName())) > 0)) {
                    Assert.fail("Final version of key [" + sd.getName() + "] on node [" + vs.getIdentityNode().getId() + "] is expected to greater than 0 but not");
                }
            }
            if(!(Long.parseLong(props.getProperty("stores.xml")) > 0)) {
                Assert.fail("Final version of key [stores.xml] on node [" + vs.getIdentityNode().getId() + "] is expected to greater than 0 but not");
            }
        }
    }

    @After
    public void teardown() {
        // shutdown
        for(VoldemortServer vs: vservers.values()) {
            vs.stop();
        }
        for(SocketStoreFactory ssf: socketStoreFactories.values()) {
            ssf.close();
        }
    }

    // This function is not a full implementation and should only used
    // in this test suite
    private StoreDefinitionBuilder storeDefToBuilder(StoreDefinition sd) {
        StoreDefinitionBuilder sb = new StoreDefinitionBuilder();
        sb.setName(sd.getName())
                .setDescription(sd.getDescription())
                .setType(sd.getType())
                .setRoutingPolicy(sd.getRoutingPolicy())
                .setRoutingStrategyType(sd.getRoutingStrategyType())
                .setKeySerializer(sd.getKeySerializer())
                .setValueSerializer(sd.getKeySerializer())
                .setReplicationFactor(sd.getReplicationFactor())
                .setZoneReplicationFactor(sd.getZoneReplicationFactor())
                .setRequiredReads(sd.getRequiredReads())
                .setRequiredWrites(sd.getRequiredWrites())
                .setZoneCountReads(sd.getZoneCountReads())
                .setZoneCountWrites(sd.getZoneCountWrites());
        return sb;
    }
}
