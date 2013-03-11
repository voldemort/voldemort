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

package voldemort.performance;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.cluster.Node;
import voldemort.routing.RoutingStrategy;
import voldemort.routing.RoutingStrategyFactory;
import voldemort.serialization.DefaultSerializerFactory;
import voldemort.serialization.Serializer;
import voldemort.serialization.SerializerFactory;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Utils;

import com.google.common.base.Joiner;

/**
 * Filter a request file for keys that are mastered by a specified node.
 */
public class RequestFileFilter {

    private final StoreDefinition storeDefinition;
    private final RoutingStrategy routingStrategy;
    private final String inputFile;
    private final String outputFile;
    private final Node node;
    private final boolean stringKeys;

    public RequestFileFilter(StoreDefinition storeDefinition,
                             RoutingStrategy routingStrategy,
                             String inputFile,
                             String outputFile,
                             Node node,
                             boolean stringKeys) {
        this.storeDefinition = storeDefinition;
        this.routingStrategy = routingStrategy;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.node = node;
        this.stringKeys = stringKeys;
    }

    // TODO: support keys other than integer or string, general cleanup
    public void filter() throws IOException {
        SerializerFactory factory = new DefaultSerializerFactory();
        @SuppressWarnings("unchecked")
        Serializer<Object> keySerializer = (Serializer<Object>) factory.getSerializer(storeDefinition.getKeySerializer());

        BufferedReader in = new BufferedReader(new FileReader(inputFile));
        BufferedWriter out = new BufferedWriter(new FileWriter(outputFile));
        try {
            String line = null;
            while((line = in.readLine()) != null) {
                String keyStr = line.replaceAll("\\s+$", "");
                Object key = null;
                if(stringKeys) {
                    key = keyStr;
                } else {
                    key = Integer.valueOf(keyStr);
                }
                byte[] keyBytes = keySerializer.toBytes(key);
                List<Node> nodes = routingStrategy.routeRequest(keyBytes);
                if(nodes.contains(node)) {
                    out.write(key + "\n");
                }
            }
        } finally {
            try {
                in.close();
            } finally {
                out.close();
            }
        }
    }

    /**
     * Filter requests specified in a file, generating a new file containing
     * only requests destined for a specific node.
     * 
     * @param args See usage for more information
     * @throws Exception In case of I/O or Voldemort-specific errors
     */
    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print usage information");
        parser.accepts("node", "[REQUIRED] node id")
              .withRequiredArg()
              .ofType(Integer.class)
              .describedAs("node id");
        parser.accepts("store-name", "[REQUIRED] store name")
              .withRequiredArg()
              .describedAs("store name");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url");
        parser.accepts("input", "[REQUIRED] input request file")
              .withRequiredArg()
              .describedAs("input-file");
        parser.accepts("output", "[REQUIRED] output file")
              .withRequiredArg()
              .describedAs("output-file");
        parser.accepts("string-keys");

        OptionSet options = parser.parse(args);

        if(options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "node",
                                               "store-name",
                                               "url",
                                               "input",
                                               "output");
        if(missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        int nodeId = (Integer) options.valueOf("node");
        String storeName = (String) options.valueOf("store-name");
        String bootstrapURL = (String) options.valueOf("url");
        String inputFile = (String) options.valueOf("input");
        String outputFile = (String) options.valueOf("output");
        boolean stringKeys = options.has("string-keys");

        AdminClient adminClient = new AdminClient(bootstrapURL,
                                                  new AdminClientConfig(),
                                                  new ClientConfig());
        List<StoreDefinition> storeDefinitionList = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                               .getValue();

        StoreDefinition storeDefinition = null;
        for(StoreDefinition def: storeDefinitionList) {
            if(storeName.equals(def.getName())) {
                storeDefinition = def;
            }
        }

        if(storeDefinition == null) {
            Utils.croak("No store found with name\"" + storeName + "\"");
        }

        Cluster cluster = adminClient.metadataMgmtOps.getRemoteCluster(nodeId).getValue();
        Node node = null;
        try {
            node = cluster.getNodeById(nodeId);
        } catch(VoldemortException e) {
            Utils.croak("Can't find a node with id " + nodeId);
        }

        RoutingStrategy routingStrategy = new RoutingStrategyFactory().updateRoutingStrategy(storeDefinition,
                                                                                             cluster);
        try {
            new RequestFileFilter(storeDefinition,
                                  routingStrategy,
                                  inputFile,
                                  outputFile,
                                  node,
                                  stringKeys).filter();
        } catch(FileNotFoundException e) {
            Utils.croak(e.getMessage());
        }
    }
}
