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

package voldemort.client;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.client.rebalance.QuotaResetter;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.quota.QuotaType;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.collect.Sets;

/**
 * Tests voldemort server behavior under NORMAL_SERVER and OFFLINE_SERVER
 * states, especially after state transitions.
 */
public class QuotaResetterTest {

    private static final String STORE_NAME = "test-basic-replication-memory";

    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

    private Set<String> storeNames;

    private VoldemortServer[] servers;

    private Cluster cluster;

    private AdminClient adminClient;

    @Before
    public void setUp() throws IOException {
        int numServers = 2;
        servers = new VoldemortServer[numServers];
        int partitionMap[][] = { { 0, 1, 2, 3 }, { 4, 5, 6, 7 } };
        Properties serverProperties = new Properties();
        serverProperties.setProperty("client.max.connections.per.node", "20");
        serverProperties.setProperty("enforce.retention.policy.on.read", Boolean.toString(false));
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        true,
                                                        null,
                                                        storesXmlfile,
                                                        serverProperties);

        List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(storesXmlfile));
        this.storeNames = Sets.newHashSet();
        for(StoreDefinition storeDef: storeDefs) {
            storeNames.add(storeDef.getName());
        }

        Properties adminProperties = new Properties();
        adminProperties.setProperty("max_connections", "20");
        adminClient = new AdminClient(cluster,
                                      new AdminClientConfig(adminProperties),
                                      new ClientConfig());
    }

    @After
    public void tearDown() throws IOException {
        adminClient.close();
        for(VoldemortServer server: servers) {
            ServerTestUtils.stopVoldemortServer(server);
        }
        socketStoreFactory.close();
    }

    @Test
    public void testQuotaResetter() {

        // set unbalanced quota values for nodes
        adminClient.quotaMgmtOps.setQuotaForNode(STORE_NAME, QuotaType.GET_THROUGHPUT, 0, 1000);

        // verify whether quota values initialized
        assertEquals(Integer.parseInt(adminClient.quotaMgmtOps.getQuotaForNode(STORE_NAME,
                                                                               QuotaType.GET_THROUGHPUT,
                                                                               0)
                                                              .getValue()),
                     1000);
        assertEquals(adminClient.quotaMgmtOps.getQuotaForNode(STORE_NAME,
                                                              QuotaType.GET_THROUGHPUT,
                                                              1), null);

        // set initial quota enforcement settings
        adminClient.metadataMgmtOps.updateRemoteMetadata(0,
                                                         MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY,
                                                         Boolean.toString(true));
        adminClient.metadataMgmtOps.updateRemoteMetadata(1,
                                                         MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY,
                                                         Boolean.toString(false));

        // verify whether quota enforcement settings initialized
        assertEquals(adminClient.metadataMgmtOps.getRemoteMetadata(0,
                                                                   MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                .getValue(),
                     Boolean.toString(true));
        assertEquals(adminClient.metadataMgmtOps.getRemoteMetadata(1,
                                                                   MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                .getValue(),
                     Boolean.toString(false));

        // initialize quota resetter
        QuotaResetter quotaResetter = new QuotaResetter(adminClient,
                                                        storeNames,
                                                        cluster.getNodeIds());

        // test rememberAndDisableQuota
        quotaResetter.rememberAndDisableQuota();

        // verify whether quota values changed
        assertEquals(Integer.parseInt(adminClient.quotaMgmtOps.getQuotaForNode(STORE_NAME,
                                                                               QuotaType.GET_THROUGHPUT,
                                                                               0)
                                                              .getValue()),
                     1000);
        assertEquals(adminClient.quotaMgmtOps.getQuotaForNode(STORE_NAME,
                                                              QuotaType.GET_THROUGHPUT,
                                                              1), null);

        // verify whether quota enforcement disabled
        assertEquals(adminClient.metadataMgmtOps.getRemoteMetadata(0,
                                                                   MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                .getValue(),
                     Boolean.toString(false));
        assertEquals(adminClient.metadataMgmtOps.getRemoteMetadata(1,
                                                                   MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                .getValue(),
                     Boolean.toString(false));

        // test resetQuotaAndRecoverEnforcement
        quotaResetter.resetQuotaAndRecoverEnforcement();

        // verify whether quota values changed
        assertEquals(Integer.parseInt(adminClient.quotaMgmtOps.getQuotaForNode(STORE_NAME,
                                                                               QuotaType.GET_THROUGHPUT,
                                                                               0)
                                                              .getValue()),
                     500);
        assertEquals(Integer.parseInt(adminClient.quotaMgmtOps.getQuotaForNode(STORE_NAME,
                                                                               QuotaType.GET_THROUGHPUT,
                                                                               1)
                                                              .getValue()),
                     500);

        // verify whether quota enforcement settings recovered
        assertEquals(adminClient.metadataMgmtOps.getRemoteMetadata(0,
                                                                   MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                .getValue(),
                     Boolean.toString(true));
        assertEquals(adminClient.metadataMgmtOps.getRemoteMetadata(1,
                                                                   MetadataStore.QUOTA_ENFORCEMENT_ENABLED_KEY)
                                                .getValue(),
                     Boolean.toString(false));
    }
}
