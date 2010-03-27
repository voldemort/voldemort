/*
 * Copyright 2008-2010 LinkedIn, Inc
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

package voldemort;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.store.StoreDefinition;
import voldemort.utils.*;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.Iterator;

/**
 * Provides a command line interface to the {@link voldemort.client.protocol.admin.AdminClient}
 */
public class VoldemortAdminTool {
    public static void main (String [] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("help", "print help information");
        parser.accepts("url", "[REQUIRED] bootstrap URL")
              .withRequiredArg()
              .describedAs("bootstrap-url")
              .ofType(String.class);
        parser.accepts("node", "[REQUIRED] node id")
              .withRequiredArg()
              .describedAs("node-id")
              .ofType(Integer.class);
        parser.accepts("delete-partitions", "Delete partitions")
              .withRequiredArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("restore", "Restore from replication");
        parser.accepts("parallelism", "Parallelism")
              .withRequiredArg()
              .describedAs("parallelism")
              .ofType(Integer.class);
        parser.accepts("fetch-keys", "Fetch keys")
              .withRequiredArg()
              .describedAs("partition-ids")
              .withValuesSeparatedBy(',')
              .ofType(Integer.class);
        parser.accepts("outdir", "Output directory")
              .withRequiredArg()
              .describedAs("output-directory")
              .ofType(String.class);
        parser.accepts("stores", "Store names")
              .withRequiredArg()
              .describedAs("store-names")
              .withValuesSeparatedBy(',')
              .ofType(String.class);
        OptionSet options = parser.parse(args);

        if (options.has("help")) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        Set<String> missing = CmdUtils.missing(options,
                                               "url",
                                               "node");
        if (missing.size() > 0) {
            System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
            parser.printHelpOn(System.err);
            System.exit(1);
        }

        String url = (String) options.valueOf("url");
        Integer nodeId = (Integer) options.valueOf("node");
        Integer parallelism = CmdUtils.valueOf(options, "parallelism", 5);

        AdminClient adminClient = new AdminClient(url, new AdminClientConfig());

        String ops = "";
        if (options.has("delete-partitions")) {
            ops += "d";
        }
        if (options.has("fetch-keys")) {
            ops += "k";
        }
        if (options.has("restore")) {
            ops += "r";
        }
        if (ops.length() < 1) {
            Utils.croak("At least one of (delete-partitions, restore, add-node) must be specified");
        }

        List<String> storeNames = null;

        if (options.has("stores")) {
            // For some reason one can't just do @SuppressWarnings without identifier following it
            @SuppressWarnings("unchecked")
            List<String> temp = (List<String>) options.valuesOf("stores");
            storeNames = temp;
        }

        try {
            if (ops.contains("d")) {
                System.out.println("Starting delete-partitions");
                @SuppressWarnings("unchecked")
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("delete-partitions");
                executeDeletePartitions(nodeId,
                                        adminClient,
                                        partitionIdList,
                                        storeNames);
                System.out.println("Finished delete-partitions");
            }
            if (ops.contains("r")) {
                System.out.println("Starting restore");
                adminClient.restoreDataFromReplications(nodeId, parallelism);
                System.err.println("Finished restore");
            }
            if (ops.contains("k")) {
                if (!options.has("outdir")) {
                    Utils.croak("Directory name (outdir) must be specified for fetch-keys");
                }
                String outputDir = (String) options.valueOf("outdir");
                System.out.println("Starting fetch keys");
                @SuppressWarnings("unchecked")
                List<Integer> partitionIdList = (List<Integer>) options.valuesOf("fetch-keys");
                executeFetchKeys(nodeId,
                                 adminClient,
                                 partitionIdList,
                                 outputDir,
                                 storeNames);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Utils.croak(e.getMessage());
        }
    }

    public static void executeFetchKeys(Integer nodeId,
                                        AdminClient adminClient,
                                        List<Integer> partitionIdList,
                                        String outputDir,
                                        List<String> storeNames) throws IOException {
        File directory = new File(outputDir);
        if (directory.exists() || directory.mkdir()) {
            List<String> stores = storeNames;
            if (stores == null) {
                stores = Lists.newArrayList();
                List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
                for (StoreDefinition storeDefinition: storeDefinitionList) {
                    stores.add(storeDefinition.getName());
                }
            }
            for (String store: stores) {
                System.out.println("Fetching keys in partitions " + Joiner.on(", ").join(partitionIdList) + " of " + store);

                File outputFile = new File(directory, store + ".keys");
                DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile)));

                try {
                    Iterator<ByteArray> keyIterator = adminClient.fetchKeys(nodeId, store, partitionIdList, null);
                    while (keyIterator.hasNext()) {
                        byte[] keyBytes = keyIterator.next().get();
                        dos.writeInt(keyBytes.length);
                        dos.write(keyBytes);
                    }
                } finally {
                    dos.close();
                }
                System.out.println("Fetched keys from " + store + " to " + outputFile);
            }
        } else {
            Utils.croak("Can't find or create directory " + outputDir);
        }
    }

    public static void executeDeletePartitions(Integer nodeId,
                                               AdminClient adminClient,
                                               List<Integer> partitionIdList,
                                               List<String> storeNames) {
        List<String> stores = storeNames;
        if (stores == null) {
            stores = Lists.newArrayList();
            List<StoreDefinition> storeDefinitionList = adminClient.getRemoteStoreDefList(nodeId).getValue();
            for (StoreDefinition storeDefinition: storeDefinitionList) {
                stores.add(storeDefinition.getName());
            }
        }
        
        for (String store: stores) {
            System.out.println("Deleting partitions " + Joiner.on(", ").join(partitionIdList) + " of " + store);
            adminClient.deletePartitions(nodeId, store, partitionIdList, null);
        }
    }
}
