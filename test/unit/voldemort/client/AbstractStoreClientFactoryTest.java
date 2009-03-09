/*
 * Copyright 2008-2009 LinkedIn, Inc
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

import java.net.URISyntaxException;
import java.util.Date;

import junit.framework.TestCase;
import voldemort.ServerTestUtils;
import voldemort.VoldemortTestConstants;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.serialization.ObjectSerializer;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerDefinition;
import voldemort.serialization.SerializerFactory;
import voldemort.xml.ClusterMapper;

/**
 * @author jay
 * 
 */
public abstract class AbstractStoreClientFactoryTest extends TestCase {

    private Node node;
    private Cluster cluster;
    private String storeDefinitionXml;

    @Override
    public void setUp() throws Exception {
        this.storeDefinitionXml = VoldemortTestConstants.getSingleStoreDefinitionsXml();
        this.cluster = ServerTestUtils.getLocalCluster(1);
        this.node = cluster.getNodes().iterator().next();
    }

    protected abstract String getValidBootstrapUrl() throws URISyntaxException;

    protected abstract String getValidScheme();

    protected abstract StoreClientFactory getFactory(String... bootstrapUrls);

    protected abstract StoreClientFactory getFactoryWithSerializer(SerializerFactory factory,
                                                                   String... bootstrapUrls);

    protected String getValidStoreName() {
        // this name comes from the xml definition
        return "test";
    }

    protected Node getLocalNode() {
        return this.node;
    }

    protected Cluster getCluster() {
        return this.cluster;
    }

    protected String getClusterXml() {
        return new ClusterMapper().writeCluster(this.cluster);
    }

    protected String getStoreDefXml() {
        return this.storeDefinitionXml;
    }

    public void testHappyCase() throws Exception {
        assertNotNull(getFactory(getValidBootstrapUrl()).getStoreClient(getValidStoreName()));
    }

    public void testCustomSerializerFactory() throws Exception {
        StoreClient<Object, Object> factory = getFactoryWithSerializer(new CustomSerializerFactory(),
                                                                       getValidBootstrapUrl()).getStoreClient(getValidStoreName());
        String key = "hello";
        Date value = new Date();
        factory.put(key, value);
        assertEquals(value, factory.getValue(key));
    }

    private class CustomSerializerFactory implements SerializerFactory {

        public Serializer<?> getSerializer(SerializerDefinition serializerDef) {
            return new ObjectSerializer<Object>();
        }
    }

    public void testBootstrapServerDown() throws Exception {
        try {
            getFactory(getValidScheme() + "://localhost:58558").getStoreClient(getValidStoreName());
            fail("Should throw exception.");
        } catch(BootstrapFailureException e) {
            // this is good
        }
    }

    public void testBootstrapFailoverSucceeds() throws Exception {
        getFactory(getValidScheme() + "://localhost:58558", getValidBootstrapUrl()).getStoreClient(getValidStoreName());
    }

    public void testUnknownStoreName() throws Exception {
        try {
            assertNotNull(getFactory(getValidBootstrapUrl()).getStoreClient("12345"));
            fail("Bootstrapped a bad name.");
        } catch(BootstrapFailureException e) {
            // this is good
        }
    }

}
