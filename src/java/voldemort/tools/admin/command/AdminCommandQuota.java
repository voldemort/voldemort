/*
 * Copyright 2008-2014 LinkedIn, Inc
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

package voldemort.tools.admin.command;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.VoldemortException;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.cluster.Node;
import voldemort.store.quota.QuotaUtils;
import voldemort.tools.admin.AdminParserUtils;
import voldemort.tools.admin.AdminUtils;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.base.Joiner;

/**
 * Implements all quota commands.
 */
public class AdminCommandQuota extends AbstractAdminCommand {

    /**
     * Parses command-line and directs to sub-commands.
     * 
     * @param args Command-line input
     * @throws Exception
     */
    public static void executeCommand(String[] args) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        args = AdminUtils.copyArrayCutFirst(args);
        if(subCmd.equals("get")) {
            SubCommandQuotaGet.executeCommand(args);
        } else if(subCmd.equals("set")) {
            SubCommandQuotaSet.executeCommand(args);
        } else if(subCmd.equals("reserve-memory")) {
            SubCommandQuotaReserveMemory.executeCommand(args);
        } else if(subCmd.equals("unset")) {
            SubCommandQuotaUnset.executeCommand(args);
        } else {
            printHelp(System.out);
        }
    }

    /**
     * Prints command-line help menu.
     */
    public static void printHelp(PrintStream stream) {
        stream.println();
        stream.println("Voldemort Admin Tool Quota Commands");
        stream.println("-----------------------------------");
        stream.println("get              Get quota values of stores.");
        stream.println("reserve-memory   Reserve memory for stores.");
        stream.println("set              Set quota values for stores.");
        stream.println("unset            Clear quota settings for stores.");
        stream.println();
        stream.println("To get more information on each command,");
        stream.println("please try \'help quota <command-name>\'.");
        stream.println();
    }

    /**
     * Parses command-line input and prints help menu.
     * 
     * @throws Exception
     */
    public static void executeHelp(String[] args, PrintStream stream) throws Exception {
        String subCmd = (args.length > 0) ? args[0] : "";
        if(subCmd.equals("get")) {
            SubCommandQuotaGet.printHelp(stream);
        } else if(subCmd.equals("set")) {
            SubCommandQuotaSet.printHelp(stream);
        } else if(subCmd.equals("reserve-memory")) {
            SubCommandQuotaReserveMemory.printHelp(stream);
        } else if(subCmd.equals("unset")) {
            SubCommandQuotaUnset.printHelp(stream);
        } else {
            printHelp(stream);
        }
    }

    /**
     * quota get command
     */
    public static class SubCommandQuotaGet extends AbstractAdminCommand {

        public static final String OPT_HEAD_QUOTA_GET = "quota-get";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_QUOTA_GET, "quota types to fetch")
                  .withOptionalArg()
                  .describedAs("quota-type-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsStoreMultiple(parser);
            AdminParserUtils.acceptsUrl(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  quota get - Get quota values of stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  quota get (<quota-type-list> | all) -s <store-name-list> -u <url>");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  Valid quota types are:");
            for(String quotaType: QuotaUtils.validQuotaTypes()) {
                stream.println("    " + quotaType);
            }
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and gets quota.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> quotaTypes = null;
            List<String> storeNames = null;
            String url = null;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_QUOTA_GET);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_QUOTA_GET);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            quotaTypes = AdminUtils.getQuotaTypes((List<String>) options.valuesOf(OPT_HEAD_QUOTA_GET));
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);

            // execute command
            AdminClient adminClient = AdminUtils.getAdminClient(url);

            doQuotaGet(adminClient, storeNames, quotaTypes);
        }

        /**
         * Gets quota for given quota types on given stores.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param storeNames List of stores to query quota
         * @param quotaType List of quota types to fetch
         */
        public static void doQuotaGet(AdminClient adminClient,
                                       List<String> storeNames,
                                       List<String> quotaTypes) {
            for(String storeName: storeNames) {
                if(!adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
                    System.out.println("Store " + storeName + " not in cluster.");
                } else {
                    System.out.println("Store " + storeName);
                    for(String quotaType: quotaTypes) {
                        Versioned<String> quotaVal = adminClient.quotaMgmtOps.getQuota(storeName,
                                                                                       quotaType);
                        if(quotaVal == null) {
                            System.out.println("No quota set for " + quotaType);
                        } else {
                            System.out.println("Quota value  for " + quotaType + " : "
                                               + quotaVal.getValue());
                        }
                    }
                }
            }
        }
    }

    /**
     * quota reserve-memory command
     */
    public static class SubCommandQuotaReserveMemory extends AbstractAdminCommand {

        public static final String OPT_HEAD_QUOTA_RESERVE_MEMORY = "quota-reserve-memory";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_QUOTA_RESERVE_MEMORY, "memory size in MB to be reserved")
                  .withOptionalArg()
                  .describedAs("memory-size")
                  .ofType(Integer.class);
            AdminParserUtils.acceptsStoreMultiple(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsNodeMultiple(parser); // either
                                                          // --node or
                                                          // --all-nodes
            AdminParserUtils.acceptsAllNodes(parser); // either --node or
                                                      // --all-nodes
            AdminParserUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  quota reserve-memory - Reserve memory for stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  quota reserve-memory <memory-size> -s <store-name-list> -u <url>");
            stream.println("                       [-n <node-id-list> | --all-nodes] [--confirm]");
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and reserves memory for given stores on given
         * nodes.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            long memoryMBSize = 0;
            List<String> storeNames = null;
            String url = null;
            List<Integer> nodeIds = null;
            Boolean allNodes = true;
            Boolean confirm = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_QUOTA_RESERVE_MEMORY);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_QUOTA_RESERVE_MEMORY);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);
            AdminParserUtils.checkOptional(options,
                                           AdminParserUtils.OPT_NODE,
                                           AdminParserUtils.OPT_ALL_NODES);

            // load parameters
            memoryMBSize = (Integer) options.valueOf(OPT_HEAD_QUOTA_RESERVE_MEMORY);
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_NODE)) {
                nodeIds = (List<Integer>) options.valuesOf(AdminParserUtils.OPT_NODE);
                allNodes = false;
            }
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Reserve memory for stores");
            System.out.println("Memory to reserve = " + memoryMBSize + " MBytes");
            System.out.println("Store:");
            System.out.println("  " + Joiner.on(", ").join(storeNames));
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            if(allNodes) {
                System.out.println("  node = all nodes");
            } else {
                System.out.println("  node = " + Joiner.on(", ").join(nodeIds));
            }

            // execute command
            if(!AdminUtils.askConfirm(confirm, "reserve memory"))
                return;

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Collection<Node> nodes = AdminUtils.getNodes(adminClient, nodeIds, allNodes);

            doQuotaReserveMemory(adminClient, nodes, storeNames, memoryMBSize);
        }

        /**
         * Reserves memory for given stores on given nodes.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param nodes List of nodes to reserve memory on
         * @param storeNames List of stores to reserve memory on
         * @param memoryMBSize Size of memory to be reserved
         * 
         */
        public static void doQuotaReserveMemory(AdminClient adminClient,
                                                 Collection<Node> nodes,
                                                 List<String> storeNames,
                                                 long memoryMBSize) {
            for(Node node: nodes) {
                adminClient.quotaMgmtOps.reserveMemory(node.getId(), storeNames, memoryMBSize);
            }
        }
    }

    /**
     * quota set command
     */
    public static class SubCommandQuotaSet extends AbstractAdminCommand {

        public static final String OPT_HEAD_QUOTA_SET = "quota-set";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_QUOTA_SET, "quota type-value pairs")
                  .withOptionalArg()
                  .describedAs("quota-type>=<quota-value")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsStoreMultiple(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  quota set - Set quota values for stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  quota set (<quota-type1>=<quota-value1>,...) -s <store-name-list> -u <url>");
            stream.println("            [--confirm]");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  Valid quota types are:");
            for(String quotaType: QuotaUtils.validQuotaTypes()) {
                stream.println("    " + quotaType);
            }
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and sets quota.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> quota = null;
            List<String> storeNames = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_QUOTA_SET);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_QUOTA_SET);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            quota = AdminUtils.getValueList((List<String>) options.valuesOf(OPT_HEAD_QUOTA_SET),
                                            "=");
            if(quota.size() % 2 != 0) {
                throw new VoldemortException("Invalid quota type-value pair.");
            }
            Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
            for(Integer i = 0; i < quota.size(); i += 2) {
                if(!validQuotaTypes.contains(quota.get(i))) {
                    Utils.croak("Invalid quota type: " + quota.get(i));
                }
            }

            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Set quota for stores");
            System.out.println("Quota:");
            for(Integer i = 0; i < quota.size(); i += 2) {
                System.out.println("  set " + quota.get(i) + " = " + quota.get(i + 1));
            }
            System.out.println("Store:");
            System.out.println("  " + Joiner.on(", ").join(storeNames));
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = all nodes");

            // execute command
            if(!AdminUtils.askConfirm(confirm, "set quota")) {
                return;
            }

            AdminClient adminClient = AdminUtils.getAdminClient(url);
            Map<String, String> quotaMap = AdminUtils.convertListToMap(quota);

            doQuotaSet(adminClient, storeNames, quotaMap);
        }

        /**
         * Sets quota for given quota types on given stores.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param storeNames The list of target stores to set quota
         * @param quotaMap Pairs of quota type-value to set
         * 
         */
        @SuppressWarnings({ "cast", "rawtypes" })
        public static void doQuotaSet(AdminClient adminClient,
                                       List<String> storeNames,
                                       Map<String, String> quotaMap) {
            for(String storeName: storeNames) {
                if(adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
                    Iterator<Entry<String, String>> iter = quotaMap.entrySet().iterator();
                    while(iter.hasNext()) {
                        Map.Entry entry = (Map.Entry) iter.next();
                        adminClient.quotaMgmtOps.setQuota(storeName,
                                                          (String) entry.getKey(),
                                                          (String) entry.getValue());
                    }
                } else {
                    System.err.println("Store " + storeName + " not in cluster.");
                }
            }
        }
    }

    /**
     * quota unset command
     */
    public static class SubCommandQuotaUnset extends AbstractAdminCommand {

        public static final String OPT_HEAD_QUOTA_UNSET = "quota-unset";

        /**
         * Initializes parser
         * 
         * @return OptionParser object with all available options
         */
        protected static OptionParser getParser() {
            OptionParser parser = new OptionParser();
            // help options
            AdminParserUtils.acceptsHelp(parser);
            // required options
            parser.accepts(OPT_HEAD_QUOTA_UNSET, "quota types to unset")
                  .withOptionalArg()
                  .describedAs("quota-type-list")
                  .withValuesSeparatedBy(',')
                  .ofType(String.class);
            AdminParserUtils.acceptsStoreMultiple(parser);
            AdminParserUtils.acceptsUrl(parser);
            // optional options
            AdminParserUtils.acceptsConfirm(parser);
            return parser;
        }

        /**
         * Prints help menu for command.
         * 
         * @param stream PrintStream object for output
         * @throws IOException
         */
        public static void printHelp(PrintStream stream) throws IOException {
            stream.println();
            stream.println("NAME");
            stream.println("  quota unset - Clear quota settings for stores");
            stream.println();
            stream.println("SYNOPSIS");
            stream.println("  quota unset (<quota-type-list> | all) -s <store-name-list> -u <url>");
            stream.println("              [--confirm]");
            stream.println();
            stream.println("COMMENTS");
            stream.println("  Valid quota types are:");
            for(String quotaType: QuotaUtils.validQuotaTypes()) {
                stream.println("    " + quotaType);
            }
            stream.println();
            getParser().printHelpOn(stream);
            stream.println();
        }

        /**
         * Parses command-line and unsets quota.
         * 
         * @param args Command-line input
         * @param printHelp Tells whether to print help only or execute command
         *        actually
         * @throws IOException
         * 
         */
        @SuppressWarnings("unchecked")
        public static void executeCommand(String[] args) throws IOException {

            OptionParser parser = getParser();

            // declare parameters
            List<String> quotaTypes = null;
            List<String> storeNames = null;
            String url = null;
            Boolean confirm = false;

            // parse command-line input
            args = AdminUtils.copyArrayAddFirst(args, "--" + OPT_HEAD_QUOTA_UNSET);
            OptionSet options = parser.parse(args);
            if(options.has(AdminParserUtils.OPT_HELP)) {
                printHelp(System.out);
                return;
            }

            // check required options and/or conflicting options
            AdminParserUtils.checkRequired(options, OPT_HEAD_QUOTA_UNSET);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_STORE);
            AdminParserUtils.checkRequired(options, AdminParserUtils.OPT_URL);

            // load parameters
            quotaTypes = AdminUtils.getQuotaTypes((List<String>) options.valuesOf(OPT_HEAD_QUOTA_UNSET));
            storeNames = (List<String>) options.valuesOf(AdminParserUtils.OPT_STORE);
            url = (String) options.valueOf(AdminParserUtils.OPT_URL);
            if(options.has(AdminParserUtils.OPT_CONFIRM)) {
                confirm = true;
            }

            // print summary
            System.out.println("Unset quota for stores");
            System.out.println("Quota:");
            System.out.println("  " + Joiner.on(", ").join(quotaTypes));
            System.out.println("Store:");
            System.out.println("  " + Joiner.on(", ").join(storeNames));
            System.out.println("Location:");
            System.out.println("  bootstrap url = " + url);
            System.out.println("  node = all nodes");

            // execute command
            if(!AdminUtils.askConfirm(confirm, "unset quota")) {
                return;
            }

            AdminClient adminClient = AdminUtils.getAdminClient(url);

            doQuotaUnset(adminClient, storeNames, quotaTypes);
        }

        /**
         * Unsets quota for given quota types on given stores.
         * 
         * @param adminClient An instance of AdminClient points to given cluster
         * @param storeNames The list of target stores to unset quota
         * @param quotaTypes Quota types to unset
         * 
         */
        public static void doQuotaUnset(AdminClient adminClient,
                                         List<String> storeNames,
                                         List<String> quotaTypes) {
            for(String storeName: storeNames) {
                if(adminClient.helperOps.checkStoreExistsInCluster(storeName)) {
                    for(String quotaType: quotaTypes) {
                        adminClient.quotaMgmtOps.unsetQuota(storeName, quotaType);
                    }
                } else {
                    System.err.println("Store " + storeName + " not in cluster.");
                }
            }
        }
    }
}
