/*
 * Copyright 2009 LinkedIn, Inc.
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

package voldemort.utils;

import static voldemort.utils.Ec2InstanceRemoteTestUtils.createInstances;
import static voldemort.utils.RemoteTestUtils.deploy;
import static voldemort.utils.RemoteTestUtils.executeRemoteTest;
import static voldemort.utils.RemoteTestUtils.generateClusterDescriptor;
import static voldemort.utils.RemoteTestUtils.startClusterAsync;
import static voldemort.utils.RemoteTestUtils.startClusterNode;
import static voldemort.utils.RemoteTestUtils.stopCluster;
import static voldemort.utils.RemoteTestUtils.stopClusterNode;
import static voldemort.utils.RemoteTestUtils.stopClusterQuiet;
import static voldemort.utils.RemoteTestUtils.toHostNames;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Ec2SmokeTest contains two examples that interact with EC2.
 * 
 * There are quite a few properties that are needed which are provided in a
 * *.properties file, the path of which is provided in the "ec2PropertiesFile"
 * System property. Below is a table of the properties:
 * 
 * <table>
 * <th>Name</th>
 * <th>Description</th>
 * <tr>
 * <td>ec2AccessId</td>
 * <td>EC2 access ID, provided by Amazon</td>
 * </tr>
 * <tr>
 * <td>ec2SecretKey</td>
 * <td>EC2 secret key, provided by Amazon</td>
 * </tr>
 * <tr>
 * <td>ec2Ami</td>
 * <td>ID of the EC2 AMI used for the instances that are started</td>
 * </tr>
 * <tr>
 * <td>ec2KeyPairId</td>
 * <td>Key pair ID</td>
 * </tr>
 * <tr>
 * <td>ec2SshPrivateKeyPath</td>
 * <td>SSH private key path to key used to connect to instances (optional)</td>
 * </tr>
 * <tr>
 * <td>ec2HostUserId</td>
 * <td>User ID on the hosts; for EC2 this is usually "root"</td>
 * </tr>
 * <tr>
 * <td>ec2VoldemortRootDirectory</td>
 * <td>Root directory on remote instances that points to the the Voldemort
 * "distribution" directory; relative to the host user ID's home directory. For
 * example, if the remote user's home directory is /root and the Voldemort
 * distribution directory is /root/voldemort, ec2VoldemortRootDirectory would be
 * "voldemort"</td>
 * </tr>
 * <tr>
 * <td>ec2VoldemortHomeDirectory</td>
 * <td>Home directory on remote instances that points to the configuration
 * directory, relative to the host user ID's home directory. For example, if the
 * remote user's home directory is /root and the Voldemort configuration
 * directory is /root/voldemort/config/single_node_cluster,
 * ec2VoldemortHomeDirectory would be "voldemort/config/single_node_cluster"</td>
 * </tr>
 * <tr>
 * <td>ec2SourceDirectory</td>
 * <td>Source directory on <b>local</b> machine from which to copy the Voldemort
 * "distribution" to the remote hosts; e.g. "/home/kirk/voldemortdev/voldemort"</td>
 * </tr>
 * <tr>
 * <td>ec2ParentDirectory</td>
 * <td>Parent directory on the <b>remote</b> machine into which to copy the
 * Voldemort "distribution". For example, if the remote user's home directory is
 * /root and the Voldemort distribution directory is /root/voldemort,
 * ec2ParentDirectory would be "." or "/root".</td>
 * </tr>
 * <tr>
 * <td>ec2ClusterXmlFile</td>
 * <td><b>Local</b> path to which cluster.xml will be written with EC2 hosts;
 * this needs to live under the ec2SourceDirectory's configuration directory
 * that is copied to the remote host.</td>
 * </tr>
 * <tr>
 * <td>ec2InstanceCount</td>
 * <td>The number of instances to create.</td>
 * </tr>
 * <tr>
 * <td>
 * ec2UseExternalHostNames</td>
 * <td>Set to "true" to use external/public host names, or "false" for
 * internal/private host names. Defaults to "true". Useful for when running the
 * tests from an instance of EC2 itself, for example.</td>
 * </tr>
 * </table>
 * 
 * @author Kirk True
 */

public class Ec2SmokeTest {

    private static String accessId;
    private static String secretKey;
    private static String ami;
    private static String keyPairId;
    private static String sshPrivateKeyPath;
    private static String hostUserId;
    private static File sshPrivateKey;
    private static String voldemortRootDirectory;
    private static String voldemortHomeDirectory;
    private static File sourceDirectory;
    private static String parentDirectory;
    private static File clusterXmlFile;
    private static List<HostNamePair> hostNamePairs;
    private static List<String> hostNames;
    private static Map<String, Integer> nodeIds;

    private static final Logger logger = Logger.getLogger(Ec2SmokeTest.class);

    @BeforeClass
    public static void setUpClass() throws Exception {
        Properties properties = getEc2Properties();
        accessId = properties.getProperty("ec2AccessId");
        secretKey = properties.getProperty("ec2SecretKey");
        ami = properties.getProperty("ec2Ami");
        keyPairId = properties.getProperty("ec2KeyPairId");
        sshPrivateKeyPath = properties.getProperty("ec2SshPrivateKeyPath");
        hostUserId = properties.getProperty("ec2HostUserId");
        sshPrivateKey = sshPrivateKeyPath != null ? new File(sshPrivateKeyPath) : null;
        voldemortRootDirectory = properties.getProperty("ec2VoldemortRootDirectory");
        voldemortHomeDirectory = properties.getProperty("ec2VoldemortHomeDirectory");
        sourceDirectory = new File(properties.getProperty("ec2SourceDirectory"));
        parentDirectory = properties.getProperty("ec2ParentDirectory");
        clusterXmlFile = new File(properties.getProperty("ec2ClusterXmlFile"));
        int ec2InstanceCount = Integer.parseInt(properties.getProperty("ec2InstanceCount"));

        hostNamePairs = createInstances(accessId, secretKey, ami, keyPairId, ec2InstanceCount);

        hostNames = toHostNames(hostNamePairs, properties.getProperty("ec2UseExternalHostNames",
                                                                      "true").equals("true"));

        nodeIds = generateClusterDescriptor(hostNamePairs, "test", clusterXmlFile);

        if(logger.isInfoEnabled())
            logger.info("Sleeping for 30 seconds to give EC2 instances some time to complete startup");

        Thread.sleep(30000);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    // if(hostNames != null)
    // destroyInstances(accessId, secretKey, hostNames);
    }

    @After
    public void tearDown() throws Exception {
        stopClusterQuiet(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
    }

    @Test
    public void testRemoteTest() throws Exception {
        deploy(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory);

        try {
            startClusterAsync(hostNames,
                              sshPrivateKey,
                              hostUserId,
                              voldemortRootDirectory,
                              voldemortHomeDirectory,
                              nodeIds);

            executeRemoteTest(hostNamePairs,
                              voldemortRootDirectory,
                              sshPrivateKey,
                              hostUserId,
                              10,
                              10,
                              100);
        } finally {
            stopCluster(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        }
    }

    @Test
    public void testTemporaryNodeOffline() throws Exception {
        deploy(hostNames, sshPrivateKey, hostUserId, sourceDirectory, parentDirectory);

        try {
            startClusterAsync(hostNames,
                              sshPrivateKey,
                              hostUserId,
                              voldemortRootDirectory,
                              voldemortHomeDirectory,
                              nodeIds);

            String offlineHostName = hostNames.get(0);

            stopClusterNode(offlineHostName, sshPrivateKey, hostUserId, voldemortRootDirectory);

            startClusterNode(offlineHostName,
                             sshPrivateKey,
                             hostUserId,
                             voldemortRootDirectory,
                             voldemortHomeDirectory,
                             nodeIds.get(hostNames.get(0)));
        } finally {
            stopCluster(hostNames, sshPrivateKey, hostUserId, voldemortRootDirectory);
        }
    }

    private static Properties getEc2Properties() throws Exception {
        String propertiesFileName = System.getProperty("ec2PropertiesFile");

        String[] requireds = { "ec2AccessId", "ec2SecretKey", "ec2Ami", "ec2KeyPairId",
                "ec2HostUserId", "ec2VoldemortRootDirectory", "ec2VoldemortHomeDirectory",
                "ec2SourceDirectory", "ec2ParentDirectory", "ec2ClusterXmlFile", "ec2InstanceCount" };

        if(propertiesFileName == null)
            throw new Exception("ec2PropertiesFile system property must be defined that "
                                + "provides the path to file containing the following "
                                + "required Ec2SmokeTest properties: "
                                + StringUtils.join(requireds, ", "));

        Properties properties = new Properties();
        Reader r = null;

        try {
            r = new FileReader(propertiesFileName);
            properties.load(r);
        } finally {
            IOUtils.closeQuietly(r);
        }

        for(String required: requireds)
            if(!properties.containsKey(required))
                throw new Exception("Required properties for Ec2SmokeTest: "
                                    + StringUtils.join(requireds, ", ") + "; missing " + required);

        return properties;
    }

}
