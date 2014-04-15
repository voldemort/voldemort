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

import java.io.*;
import java.util.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import voldemort.VoldemortException;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;
import voldemort.store.StoreDefinition;
import voldemort.store.quota.QuotaUtils;
import voldemort.store.system.SystemStoreConstants;
import voldemort.utils.Utils;

/**
 * Utility class for AdminCommand
 * 
 */
public class AdminUtils {
    
    private static final String QUOTATYPE_ALL = "all";
    
    /**
     * Utility function copies a string array except for the first element
     * 
     * @param arr Original array of strings
     * @return Copied array of strings
     */
    public static String[] copyArrayCutFirst(String[] arr) {
    	if (arr.length > 1) {
	    	String[] arrCopy = new String[arr.length - 1];
	    	System.arraycopy(arr, 1, arrCopy, 0, arrCopy.length);
	    	return arrCopy;
    	} else {
    		return new String[0];
    	}
    }
    
    /**
     * Utility function copies a string array and add another string to first
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
     * Utility function pauses and asks for confirmation on dangerous operations.
     * 
     * @param confirm User has already confirmed in command-line input
     * @param opDesc Description of the dangerous operation
     * @throws IOException
     * @return True if user confirms the operation in either command-line input or here.
     *
     */
    public static Boolean askConfirm(Boolean confirm, String opDesc) throws IOException {
        if (confirm) {
            System.out.println("Confirmed " + opDesc + " in command-line.");
            return true;
        } else {
            System.out.println("Are you sure you want to " + opDesc + "? (yes/no)");
            BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));
            String text = buffer.readLine();
            return text.compareTo("yes") == 0;
        }
    }

    /**
     * Utility function gives list of values from list of value-pair strings.
     * 
     * @param valueList List of value-pair strings
     * @param delim Delimiter that separates the value pair
     * @returns The list of values; empty if no value-pair is present,
     *  The even elements are the first ones of the value pair, and the odd elements are the second ones.
     *  For example, if the list of value-pair is ["cluster.xml=file1", "stores.xml=file2"],
     *  and the pair delimiter is '=', we will then have the list of values in return:
     *  ["cluster.xml", "file1", "stores.xml", "file2"].
     */
    public static List<String> getValueList(List<String> valuePairs, String delim) {
        List<String> valueList = Lists.newArrayList();
        for (String valuePair: valuePairs) {
            String[] value = valuePair.split(delim, 2);
            if (value.length != 2) throw new VoldemortException("Invalid argument pair: " + value);
            valueList.add(value[0]);
            valueList.add(value[1]);            
        }
        return valueList;
    }
    
    /**
     * Utility function converts a list to a map.
     * 
     * @param list The list in which even elements are keys and odd elements are values.
     * @rturn The map container that maps even elements to odd elements, e.g. 0->1, 2->3, etc.
     */
    public static <V> Map<V, V> convertListToMap(List<V> list) {
        Map<V, V> map = new HashMap<V, V>();
        if (list.size() % 2 != 0) throw new VoldemortException("Failed to convert list to map.");
        for (int i = 0;i < list.size();i += 2) {
            map.put(list.get(i), list.get(i + 1));
        }
        return map;
    }

    /**
     * Utility function constructs AdminClient.
     * 
     * @param url URL pointing to the bootstrap node
     * @return Newly constructed AdminClient
     */
    public static AdminClient getAdminClient(String url) {
       return  new AdminClient(url,
                               new AdminClientConfig(),
                               new ClientConfig());
    }

    /**
     * Utility function fetches nodes.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param nodeIds List of node IDs parsed from command-line input
     * @param allNodes Tells if all nodes are selected
     * @return Collection of node objects selected by user
     */
    public static Collection<Node> getNodes(AdminClient adminClient, List<Integer> nodeIds,
                                              Boolean allNodes) {
        Collection<Node> nodes = null;
        if (allNodes) {
            nodes = adminClient.getAdminClientCluster().getNodes();
        } else {
            nodes = new ArrayList<Node>();
            for (Integer nodeId: nodeIds) {
                nodes.add(adminClient.getAdminClientCluster().getNodeById(nodeId));
            }
        }
        if (nodes.size() == 0) throw new VoldemortException("The cluster doesn't have any node.");
        return nodes;
    }

    /**
     * Utility function fetches stores on a node.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param node Node to fetch stores from
     * @param storeNames Stores read from command-line input
     * @param allStores Tells if all stores are selected
     * @return List of store names selected by user
     */
    public static List<String> getUserStoresOnNode(AdminClient adminClient, Node node, List<String> storeNames,
                                               Boolean allStores) {
        List<StoreDefinition> storeDefinitionList = adminClient.metadataMgmtOps
                                                               .getRemoteStoreDefList(node.getId())
                                                               .getValue();
        if (allStores) {
            storeNames = Lists.newArrayList();
            for (StoreDefinition storeDefinition: storeDefinitionList) {
                storeNames.add(storeDefinition.getName());
            }
        } else {
            for (String storeName: storeNames) {
                if (!storeDefinitionList.contains(storeName))
                    Utils.croak("Store " + storeName + " does not exist!");
            }
        }
        return storeNames;
    }

    /**
     * Utility function fetches partitions.
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param partIds List of partitions read from command-line input
     * @param allParts Tells if all partitions are selected
     * @return List of partitions selected by user
     */
    public static List<Integer> getPartitions(AdminClient adminClient, List<Integer> partIds, Boolean allParts) {
        if (allParts) {
            partIds = Lists.newArrayList();
            for (Node node: adminClient.getAdminClientCluster().getNodes()) {
                partIds.addAll(node.getPartitionIds());
            }
        }
        return partIds;
    }

    /**
     * Utility function fetches quota types.
     * 
     * @param nodeIds List of IDs of nodes parsed from command-line input
     * @param allNodes Tells if all nodes are selected
     * @return Collection of node objects selected by user
     */
    public static List<String> getQuotaTypes(List<String> quotaTypes) {
        if (quotaTypes.size() < 1) throw new VoldemortException("Quota type not specified.");
        Set<String> validQuotaTypes = QuotaUtils.validQuotaTypes();
        if (quotaTypes.size() == 1 && quotaTypes.get(0).compareTo(AdminUtils.QUOTATYPE_ALL) == 0) {
            Iterator<String> iter = validQuotaTypes.iterator();
            quotaTypes = Lists.newArrayList();
            while (iter.hasNext()) {
                quotaTypes.add(iter.next());
            }
        } else {
            for (String quotaType: quotaTypes) {
                if (!validQuotaTypes.contains(quotaType)) {
                    Utils.croak("Specify a valid quota type from :" + validQuotaTypes);
                }
            }
        }
        return quotaTypes;
    }
    
    /**
     * Utility function creates directory.
     * 
     * @param dir Directory path
     * @return File object of directory.
     */
    public static File createDir(String dir) {
        // create outdir
        File directory = null;
        if (dir != null) {
            directory = new File(dir);
            if (!(directory.exists() || directory.mkdir())) {
                Utils.croak("Can't find or create directory " + dir);
            }
        }
        return directory;
    }
    
    /** 
     * Utility function fetches system store definitions
     * 
     * @return The map container that maps store names to store definitions
     */
    public static Map<String, StoreDefinition> getSystemStoreDefs() {
        Map<String, StoreDefinition> sysStoreDefMap = Maps.newHashMap();
        List<StoreDefinition> storesDefs = SystemStoreConstants.getAllSystemStoreDefs();
        for(StoreDefinition def: storesDefs) {
            sysStoreDefMap.put(def.getName(), def);
        }
        return sysStoreDefMap;
    }

    /** 
     * Utility function fetches user defined store definitions
     * 
     * @param adminClient An instance of AdminClient points to given cluster
     * @param node Store definitions to fetch from
     * @return The map container that maps store names to store definitions
     */
    public static Map<String, StoreDefinition> getUserStoreDefs(AdminClient adminClient, Node node) {
        List<StoreDefinition> storeDefinitionList =
            adminClient.metadataMgmtOps.getRemoteStoreDefList(node.getId()).getValue();
        Map<String, StoreDefinition> storeDefinitionMap = Maps.newHashMap();
        for (StoreDefinition storeDefinition: storeDefinitionList) {
            storeDefinitionMap.put(storeDefinition.getName(), storeDefinition);
        }
        return storeDefinitionMap;
    }
}
