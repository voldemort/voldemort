/*
 * Copyright 2014 LinkedIn, Inc
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

package voldemort.tools;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import voldemort.ServerTestUtils;
import voldemort.client.ClientConfig;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Cluster;
import voldemort.server.VoldemortServer;
import voldemort.store.socket.SocketStoreFactory;
import voldemort.store.socket.clientrequest.ClientRequestExecutorPool;

import com.google.common.collect.Lists;

public class ReadOnlyReplicationHelperTest {

    private static String storesXmlfile = "test/common/voldemort/config/stores.xml";

    private SocketStoreFactory socketStoreFactory = new ClientRequestExecutorPool(2,
                                                                                  10000,
                                                                                  100000,
                                                                                  32 * 1024);

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
        cluster = ServerTestUtils.startVoldemortCluster(numServers,
                                                        servers,
                                                        partitionMap,
                                                        socketStoreFactory,
                                                        false,
                                                        null,
                                                        storesXmlfile,
                                                        serverProperties);

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

    private List<String> getSourceFileList(List<String> infoList,
                                           String storeName,
                                           Integer srcNodeId) {
        List<String> fileList = Lists.newArrayList();
        for(String info: infoList) {
            String[] infoSplit = info.split(",");
            String nodeId = infoSplit[1];
            String relPath = infoSplit[2];
            String[] pathSplit = relPath.split("/");
            String parsedStoreName = pathSplit[0];
            String parserFileName = pathSplit[2];
            if(storeName.equals(parsedStoreName) && srcNodeId.equals(Integer.parseInt(nodeId))) {
                fileList.add(parserFileName);
            }
        }
        return fileList;
    }

    private List<String> getDestFileList(List<String> infoList, String storeName, Integer srcNodeId) {
        List<String> fileList = Lists.newArrayList();
        for(String info: infoList) {
            String[] infoSplit = info.split(",");
            String nodeId = infoSplit[1];
            String relPath = infoSplit[3];
            String[] pathSplit = relPath.split("/");
            String parsedStoreName = pathSplit[0];
            String parserFileName = pathSplit[2];
            if(storeName.equals(parsedStoreName) && srcNodeId.equals(Integer.parseInt(nodeId))) {
                fileList.add(parserFileName);
            }
        }
        return fileList;
    }

    private Boolean compareStringLists(List<String> list1, List<String> list2) {
        if(list1 == null && list2 == null) {
            return true;
        }
        if(list1 == null || list2 == null || list1.size() != list2.size()) {
            return false;
        }
        for(String s1: list1) {
            if(!list2.contains(s1)) {
                return false;
            }
        }
        return true;
    }
    
    /**
     * Match string with wildcard pattern
     * 
     * @param s Input string
     * @param p Pattern string with wildcard (* and ?)
     * @param i Iterator of characters in s
     * @param j Iterator of characters in p
     * @return true if string can be matched by pattern
     */
    private Boolean isMatch(String s, String p, int i, int j) {

        while (i < s.length() && j < p.length() && p.charAt(j) != '*') {
            if(s.charAt(i) == p.charAt(j) || p.charAt(j) == '?') {
                i++;
                j++;
            } else {
                return false;
            }
        }
        if(j == p.length()) {
            return i == s.length();
        }
        if(i == s.length()) {
            while(j < p.length() && p.charAt(j) == '*') {
                j++;
            }
            return j == p.length();
        }
        return isMatch(s, p, i + 1, j) || isMatch(s, p, i, j + 1);
    }

    private Boolean isMatch(String s, String p) {
        return isMatch(s, p, 0, 0);
    }

    private Boolean compareWildcardStringLists(List<String> strings, List<String> patterns) {
        if(patterns == null) {
            return strings == null;
        } else if(strings == null) {
            return true;
        }
        for(String string: strings) {
            boolean match = false;
            for(String pattern: patterns) {
                if (isMatch(string, pattern)) {
                    match = true;
                    break;
                }
            }
            if (!match) return false;
        }
        return true;
    }

    @Test
    public void testGetROStorageFileListRemotely() {
        String storeName = "test-readonly-fetchfiles";
        // file list on node 0
        List<String> fileList0 = adminClient.readonlyOps.getROStorageFileList(0, storeName);
        // file list on node 1
        List<String> fileList1 = adminClient.readonlyOps.getROStorageFileList(1, storeName);
        // info list for node 0
        List<String> infoList0 = ReadOnlyReplicationHelperCLI.getReadOnlyReplicationInfo(adminClient,
                                                                                         0,
                                                                                         false);
        // info list for node 1
        List<String> infoList1 = ReadOnlyReplicationHelperCLI.getReadOnlyReplicationInfo(adminClient,
                                                                                         1,
                                                                                         false);
        for(String info: infoList0) {
            System.out.println(info);
        }
        // source list for node 0, i.e. file list on node 1
        List<String> srcList0 = getSourceFileList(infoList0, storeName, 1);
        // dest list for node 0, i.e. file list on node 0
        List<String> dstList0 = getDestFileList(infoList0, storeName, 1);
        // source list for node 1, i.e. file list on node 0
        List<String> srcList1 = getSourceFileList(infoList1, storeName, 0);
        // dest list for node 1, i.e. file list on node 1
        List<String> dstList1 = getDestFileList(infoList1, storeName, 0);
        // compare
        assertTrue(fileList0.size() > 0);
        assertTrue(fileList1.size() > 0);
        assertTrue(compareStringLists(srcList0, fileList1));
        assertTrue(compareStringLists(dstList0, fileList0));
        assertTrue(compareStringLists(srcList1, fileList0));
        assertTrue(compareStringLists(dstList1, fileList1));
    }

    @Test
    public void testGetROStorageFileListLocally() {
        String storeName = "test-readonly-fetchfiles";
        // file list on node 0
        List<String> fileList0 = adminClient.readonlyOps.getROStorageFileList(0, storeName);
        // file list on node 1
        List<String> fileList1 = adminClient.readonlyOps.getROStorageFileList(1, storeName);
        // info list for node 0
        List<String> infoList0 = ReadOnlyReplicationHelperCLI.getReadOnlyReplicationInfo(adminClient,
                                                                                         0,
                                                                                         true);
        // info list for node 1
        List<String> infoList1 = ReadOnlyReplicationHelperCLI.getReadOnlyReplicationInfo(adminClient,
                                                                                         1,
                                                                                         true);
        // source list for node 0, i.e. file list on node 1
        List<String> srcList0 = getSourceFileList(infoList0, storeName, 1);
        // dest list for node 0, i.e. file list on node 0
        List<String> dstList0 = getDestFileList(infoList0, storeName, 1);
        // source list for node 1, i.e. file list on node 0
        List<String> srcList1 = getSourceFileList(infoList1, storeName, 0);
        // dest list for node 1, i.e. file list on node 1
        List<String> dstList1 = getDestFileList(infoList1, storeName, 0);
        // compare
        assertTrue(fileList0.size() > 0);
        assertTrue(fileList1.size() > 0);

        assertTrue(isMatch("0_1_0", "0_1_*"));

        assertTrue(compareWildcardStringLists(fileList1, srcList0));
        assertTrue(compareWildcardStringLists(fileList0, dstList0));
        assertTrue(compareWildcardStringLists(fileList0, srcList1));
        assertTrue(compareWildcardStringLists(fileList1, dstList1));
    }

}
