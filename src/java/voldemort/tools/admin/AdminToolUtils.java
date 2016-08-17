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

package voldemort.tools.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.UnreachableStoreException;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.quota.QuotaType;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.Utils;
import voldemort.versioning.Versioned;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Utility class for AdminCommand
 * 
 */
public class AdminToolUtils {

    private static final String QUOTATYPE_ALL = "all";

    /**
     * Utility function that copies a string array except for the first element
     * 
     * @param arr Original array of strings
     * @return Copied array of strings
     */
    public static String[] copyArrayCutFirst(String[] arr) {
        if(arr.length > 1) {
            String[] arrCopy = new String[arr.length - 1];
            System.arraycopy(arr, 1, arrCopy, 0, arrCopy.length);
            return arrCopy;
        } else {
            return new String[0];
        }
    }

    /**
     * Utility function that copies a string array and add another string to
     * first
     * 
     * @param arr Original array of strings
     * @param add
     * @return Copied array of strings
     */
    public static String[] copyArrayAddFirst(String[] arr, String add) {
        String[] arrCopy = new String[arr.length + 1];
        arrCopy[0] = add;
        System.arraycopy(arr, 0, arrCopy, 1, arr.length);
        return arrCopy;
    }

    /**
     * Utility function that pauses and asks for confirmation on dangerous
     * operations.
     * 
     * @param confirm User has already confirmed in command-line input
     * @param opDesc Description of the dangerous operation
     * @throws IOException
     * @return True if user confirms the operation in either command-line input
     *         or here.
     * 
     */
    public static Boolean askConfirm(Boolean confirm, String opDesc) throws IOException {
        if(confirm) {
            System.out.println("Confirmed " + opDesc + " in command-line.");
            return true;
        } else {
            System.out.println("Are you sure you want to " + opDesc + "? (yes/no)");
            BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
            String text = buffer.readLine().toLowerCase(Locale.ENGLISH);
            boolean go = text.equals("yes") || text.equals("y");
            if (!go) {
                System.out.println("Did not confirm; " + opDesc + " aborted.");
            }
            return go;
        }
    }

    /**
     * Utility function that gives list of values from list of value-pair
     * strings.
     * 
     * @param valuePairs List of value-pair strings
     * @param delim Delimiter that separates the value pair
     * @returns The list of values; empty if no value-pair is present, The even
     *          elements are the first ones of the value pair, and the odd
     *          elements are the second ones. For example, if the list of
     *          value-pair is ["cluster.xml=file1", "stores.xml=file2"], and the
     *          pair delimiter is '=', we will then have the list of values in
     *          return: ["cluster.xml", "file1", "stores.xml", "file2"].
     */
    public static List<String> getValueList(List<String> valuePairs, String delim) {
        List<String> valueList = Lists.newArrayList();
        for(String valuePair: valuePairs) {
            String[] value = valuePair.split(delim, 2);
            if(value.length != 2)
                throw new VoldemortException("Invalid argument pair: " + valuePair);
            valueList.add(value[0]);
            valueList.add(value[1]);
        }
        return valueList;
    }

    /**
     * Utility function that converts a list to a map.
     * 
     * @param list The list in which even elements are keys and odd elements are
     *        values.
     * @rturn The map container that maps even elements to odd elements, e.g.
     *        0->1, 2->3, etc.
     */
    public static <V> Map<V, V> convertListToMap(List<V> list) {
        Map<V, V> map = new HashMap<V, V>();
        if(list.size() % 2 != 0)
            throw new VoldemortException("Failed to convert list to map.");
        for(int i = 0; i < list.size(); i += 2) {
            map.put(list.get(i), list.get(i + 1));
        }
        return map;
    }

    /**
     * Utility function that constructs AdminClient.
     * 
     * @param url URL pointing to the bootstrap node
     * @return Newly constructed AdminClient
     */
    public static AdminClient getAdminClient(String url) {
        ClientConfig config = new ClientConfig().setBootstrapUrls(url)
                                                .setConnectionTimeout(5, TimeUnit.SECONDS);

        AdminClientConfig adminConfig = new AdminClientConfig().setAdminSocketTimeoutSec(5);
        return new AdminClient(adminConfig, config);
    }

    /**
     * Utility function that fetches node ids.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @return Node ids in cluster
     */
    public static List<Integer> getAllNodeIds(AdminClient adminClient) {
        List<Integer> nodeIds = Lists.newArrayList();
        for(Integer nodeId: adminClient.getAdminClientCluster().getNodeIds()) {
            nodeIds.add(nodeId);
        }
        return nodeIds;
    }

    /**
     * Utility function that fetches all stores on a node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to fetch stores from
     * @return List of all store names
     */
    public static List<String> getAllUserStoreNamesOnNode(AdminClient adminClient, Integer nodeId) {
        List<String> storeNames = Lists.newArrayList();
        List<StoreDefinition> storeDefinitionList = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                               .getValue();

        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeNames.add(storeDefinition.getName());
        }
        return storeNames;
    }

    public static void validateStoreNameOnNode(AdminClient adminClient,
                                         Integer nodeId,
                                         List<String> storeNames) {

        List<String> userStores = new ArrayList<String>();

        for(String storeName: storeNames) {
            if(!SystemStoreConstants.isSystemStore(storeName)) {
                userStores.add(storeName);
            }
        }

        if(userStores.size() > 0) {
            validateUserStoreNamesOnNode(adminClient, nodeId, userStores);
        }
    }

    /**
     * Utility function that checks if store names are valid on a node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to fetch stores from
     * @param storeNames Store names to check
     */
    public static void validateUserStoreNamesOnNode(AdminClient adminClient,
                                                    Integer nodeId,
                                                    List<String> storeNames) {
        List<StoreDefinition> storeDefList = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                        .getValue();
        Map<String, Boolean> existingStoreNames = new HashMap<String, Boolean>();
        for(StoreDefinition storeDef: storeDefList) {
            existingStoreNames.put(storeDef.getName(), true);
        }
        for(String storeName: storeNames) {
            if(!Boolean.TRUE.equals(existingStoreNames.get(storeName))) {
                Utils.croak("Store " + storeName + " does not exist!");
            }
        }
    }

    /**
     * Utility function that fetches partitions.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @return all partitions on cluster
     */
    public static List<Integer> getAllPartitions(AdminClient adminClient) {
        List<Integer> partIds = Lists.newArrayList();
        partIds = Lists.newArrayList();
        for(Node node: adminClient.getAdminClientCluster().getNodes()) {
            partIds.addAll(node.getPartitionIds());
        }
        return partIds;
    }

    /**
     * Utility function that fetches quota types.
     */
    public static List<QuotaType> getQuotaTypes(List<String> strQuotaTypes) {
        if(strQuotaTypes.size() < 1) {
            throw new VoldemortException("Quota type not specified.");
        }
        List<QuotaType> quotaTypes;
        if(strQuotaTypes.size() == 1 && strQuotaTypes.get(0).equals(AdminToolUtils.QUOTATYPE_ALL)) {
            quotaTypes = Arrays.asList(QuotaType.values());
        } else {
            quotaTypes = new ArrayList<QuotaType>();
            for(String strQuotaType: strQuotaTypes) {
                QuotaType type = QuotaType.valueOf(strQuotaType);
                quotaTypes.add(type);
            }
        }
        return quotaTypes;
    }

    /**
     * Utility function that creates directory.
     * 
     * @param dir Directory path
     * @return File object of directory.
     */
    public static File createDir(String dir) {
        // create outdir
        File directory = null;
        if(dir != null) {
            directory = new File(dir);
            if(!(directory.exists() || directory.mkdir())) {
                Utils.croak("Can't find or create directory " + dir);
            }
        }
        return directory;
    }

    /**
     * Utility function that fetches system store definitions
     * 
     * @return The map container that maps store names to store definitions
     */
    public static Map<String, StoreDefinition> getSystemStoreDefMap() {
        Map<String, StoreDefinition> sysStoreDefMap = Maps.newHashMap();
        List<StoreDefinition> storesDefs = SystemStoreConstants.getAllSystemStoreDefs();
        for(StoreDefinition def: storesDefs) {
            sysStoreDefMap.put(def.getName(), def);
        }
        return sysStoreDefMap;
    }

    /**
     * Utility function that fetches user defined store definitions
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to fetch store definitions from
     * @return The map container that maps store names to store definitions
     */
    public static Map<String, StoreDefinition> getUserStoreDefMapOnNode(AdminClient adminClient,
                                                                        Integer nodeId) {
        List<StoreDefinition> storeDefinitionList = adminClient.metadataMgmtOps.getRemoteStoreDefList(nodeId)
                                                                               .getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for(StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }
        return storeDefinitionMap;
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if all nodes of the cluster are in normal state (
     * {@link VoldemortState#NORMAL_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @throws VoldemortException if any node is not in normal state
     */
    public static void assertServerInNormalState(AdminClient adminClient) {
        assertServerInNormalState(adminClient, adminClient.getAdminClientCluster().getNodeIds());
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if a node is in normal state (
     * {@link VoldemortState#NORMAL_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to be checked
     * @throws VoldemortException if any node is not in normal state
     */
    public static void assertServerInNormalState(AdminClient adminClient, Integer nodeId) {
        assertServerInNormalState(adminClient, Lists.newArrayList(new Integer[]{nodeId}));
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are in normal state (
     * {@link VoldemortState#NORMAL_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeIds List of node ids to be checked
     * @throws VoldemortException if any node is not in normal state
     */
    public static void assertServerInNormalState(AdminClient adminClient,
                                                 Collection<Integer> nodeIds) {
        assertServerState(adminClient, nodeIds, VoldemortState.NORMAL_SERVER, true);
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are not in offline state (
     * {@link VoldemortState#OFFLINE_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeIds List of node ids to be checked
     * @throws VoldemortException if any node is in offline state
     */
    public static void assertServerNotInOfflineState(AdminClient adminClient,
                                                     Collection<Integer> nodeIds) {
        assertServerState(adminClient, nodeIds, VoldemortState.OFFLINE_SERVER, false);
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are not in rebalancing state (
     * {@link VoldemortState#REBALANCING_MASTER_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @throws VoldemortException if any node is in rebalancing state
     */
    public static void assertServerNotInRebalancingState(AdminClient adminClient) {
        assertServerNotInRebalancingState(adminClient, adminClient.getAdminClientCluster()
                                                                  .getNodeIds());
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are not in rebalancing state (
     * {@link VoldemortState#REBALANCING_MASTER_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeId Node id to be checked
     * @throws VoldemortException if any node is in rebalancing state
     */
    public static void assertServerNotInRebalancingState(AdminClient adminClient, Integer nodeId) {
        assertServerNotInRebalancingState(adminClient, Lists.newArrayList(new Integer[]{nodeId}));
    }

    /**
     * Utility function that checks the execution state of the server by
     * checking the state of {@link VoldemortState} <br>
     * 
     * This function checks if the nodes are not in rebalancing state (
     * {@link VoldemortState#REBALANCING_MASTER_SERVER}).
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeIds List of node ids to be checked
     * @throws VoldemortException if any node is in rebalancing state
     */
    public static void assertServerNotInRebalancingState(AdminClient adminClient,
                                                         Collection<Integer> nodeIds) {
        assertServerState(adminClient, nodeIds, VoldemortState.REBALANCING_MASTER_SERVER, false);
    }

    /**
     * Checks if nodes are in a given {@link VoldemortState}. Can also be
     * used to ensure that nodes are NOT in a given {@link VoldemortState}.
     *
     * Either way, throws an exception if any node isn't as expected.
     *
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeIds List of node ids to be checked
     * @param stateToCheck state to be verified
     * @param serverMustBeInThisState - if true, function will throw if any
     *                                server is NOT in the stateToCheck
     *                                - if false, function will throw if any
     *                                server IS in the stateToCheck
     * @throws VoldemortException if any node doesn't conform to the required state
     */
    private static void assertServerState(AdminClient adminClient,
                                          Collection<Integer> nodeIds,
                                          VoldemortState stateToCheck,
                                          boolean serverMustBeInThisState) {
        for(Integer nodeId: nodeIds) {
            String nodeName = adminClient.getAdminClientCluster().getNodeById(nodeId).briefToString();
            try {
                Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                            MetadataStore.SERVER_STATE_KEY);
                VoldemortState state = VoldemortState.valueOf(versioned.getValue());
                if(state.equals(stateToCheck) != serverMustBeInThisState) {
                    throw new VoldemortException("Cannot execute admin operation: "
                                                 + nodeName + " is " + (serverMustBeInThisState ? "not in " : "in ")
                                                 + stateToCheck.name() + " state.");
                }
            } catch (UnreachableStoreException e) {
                System.err.println("Cannot verify the server state of " + nodeName + " because it is unreachable. Skipping.");
            }
        }
    }
}
