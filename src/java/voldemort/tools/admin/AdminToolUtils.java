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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.metadata.MetadataStore;
import voldemort.store.metadata.MetadataStore.VoldemortState;
import voldemort.store.quota.QuotaUtils;
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
            String text = buffer.readLine();
            return text.equals("yes");
        }
    }

    /**
     * Utility function that gives list of values from list of value-pair
     * strings.
     * 
     * @param valueList List of value-pair strings
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
                throw new VoldemortException("Invalid argument pair: " + value);
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
        return new AdminClient(url, new AdminClientConfig(), new ClientConfig());
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
        List<String> storeNameList = Lists.newArrayList();
        for(StoreDefinition storeDef: storeDefList) {
            storeNameList.add(storeDef.getName());
        }
        for(String storeName: storeNames) {
            if(!storeNameList.contains(storeName)) {
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
     * 
     * @param nodeIds List of IDs of nodes parsed from command-line input
     * @param allNodes Tells if all nodes are selected
     * @return Collection of node objects selected by user
     */
    public static List<String> getQuotaTypes(List<String> quotaTypes) {
        if(quotaTypes.size() < 1) {
            throw new VoldemortException("Quota type not specified.");
        }
        Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
        if(quotaTypes.size() == 1 && quotaTypes.get(0).equals(AdminToolUtils.QUOTATYPE_ALL)) {
            Iterator<String> iter = validQuotaTypes.iterator();
            quotaTypes = Lists.newArrayList();
            while(iter.hasNext()) {
                quotaTypes.add(iter.next());
            }
        } else {
            for(String quotaType: quotaTypes) {
                if(!validQuotaTypes.contains(quotaType)) {
                    Utils.croak("Specify a valid quota type from :" + validQuotaTypes);
                }
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
        assertServerInNormalState(adminClient, Lists.newArrayList(new Integer[] { nodeId }));
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
    public static void assertServerInNormalState(AdminClient adminClient, Collection<Integer> nodeIds) {
        for(Integer nodeId: nodeIds) {
            Versioned<String> versioned = adminClient.metadataMgmtOps.getRemoteMetadata(nodeId,
                                                                                        MetadataStore.SERVER_STATE_KEY);
            VoldemortState state = VoldemortState.valueOf(versioned.getValue());
            if(!state.equals(VoldemortState.NORMAL_SERVER)) {
                throw new VoldemortException("Cannot execute admin operation: "
                                             + nodeId
                                             + " ("
                                             + adminClient.getAdminClientCluster()
                                                          .getNodeById(nodeId)
                                                          .getHost()
                                             + ") is not in normal state, but in "
                                             + versioned.getValue());
            }
        }
    }
}
