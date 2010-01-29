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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import voldemort.VoldemortException;

/**
 * Testing on EC2 requires quite a few configuration properties; these are
 * provided in a *.properties file, the path of which is provided in the
 * "ec2PropertiesFile" System property. Below is a table of the properties:
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
 * <td>ec2InstanceIdFile</td>
 * <td>File in which to list the instances that are created; used by external
 * tools to stop instances in case of catastrophic test failure</td>
 * </tr>
 * </table>
 * 
 * @author Kirk True
 */

public class Ec2RemoteTestConfig extends RemoteTestConfig {

    protected String accessId;

    protected String secretKey;

    protected String ami;

    protected String keyPairId;

    protected int instanceCount;

    protected File instanceIdFile;

    public Ec2RemoteTestConfig() {
        Properties properties = getEc2Properties();
        init(properties);
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getAmi() {
        return ami;
    }

    public void setAmi(String ami) {
        this.ami = ami;
    }

    public String getKeyPairId() {
        return keyPairId;
    }

    public void setKeyPairId(String keyPairId) {
        this.keyPairId = keyPairId;
    }

    public int getInstanceCount() {
        return instanceCount;
    }

    public void setInstanceCount(int instanceCount) {
        this.instanceCount = instanceCount;
    }

    public File getInstanceIdFile() {
        return instanceIdFile;
    }

    public void setInstanceIdFile(File instanceIdFile) {
        this.instanceIdFile = instanceIdFile;
    }

    protected void init(Properties properties) {
        setAccessId(properties.getProperty("ec2AccessId"));
        setSecretKey(properties.getProperty("ec2SecretKey"));
        setAmi(properties.getProperty("ec2Ami"));
        setKeyPairId(properties.getProperty("ec2KeyPairId"));
        setHostUserId(properties.getProperty("ec2HostUserId"));
        setSshPrivateKey(getFileProperty(properties, "ec2SshPrivateKeyPath"));
        setVoldemortRootDirectory(properties.getProperty("ec2VoldemortRootDirectory"));
        setVoldemortHomeDirectory(properties.getProperty("ec2VoldemortHomeDirectory"));
        setSourceDirectory(getFileProperty(properties, "ec2SourceDirectory"));
        setParentDirectory(properties.getProperty("ec2ParentDirectory"));
        setClusterXmlFile(getFileProperty(properties, "ec2ClusterXmlFile"));
        setInstanceCount(getIntProperty(properties, "ec2InstanceCount"));
        setInstanceIdFile(getFileProperty(properties, "ec2InstanceIdFile"));
    }

    protected File getFileProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        return value != null ? new File(value) : null;
    }

    protected int getIntProperty(Properties properties, String propertyName) {
        String value = properties.getProperty(propertyName);
        return value != null ? Integer.parseInt(value) : 0;
    }

    protected int getIntProperty(Properties properties, String propertyName, int defaultValue) {
        String value = properties.getProperty(propertyName);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    protected Properties getEc2Properties() {
        String propertiesFileName = System.getProperty("ec2PropertiesFile");

        List<String> requireds = getRequiredPropertyNames();

        if(propertiesFileName == null)
            throw new VoldemortException("ec2PropertiesFile system property must be defined that "
                                         + "provides the path to file containing the following "
                                         + "required properties: "
                                         + StringUtils.join(requireds, ", "));

        Properties properties = new Properties();
        InputStream in = null;

        try {
            in = new FileInputStream(propertiesFileName);
            properties.load(in);
        } catch(IOException e) {
            throw new VoldemortException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }

        for(String required: requireds) {
            // Allow system properties to override
            if(System.getProperties().containsKey(required))
                properties.put(required, System.getProperty(required));

            if(!properties.containsKey(required))
                throw new VoldemortException("Required properties: "
                                             + StringUtils.join(requireds, ", ") + "; missing "
                                             + required);
        }

        return properties;
    }

    protected List<String> getRequiredPropertyNames() {
        // There's a reason we're making two lists: Arrays.asList makes an
        // immutable list. However, we want to return a mutable list to the
        // caller as subclasses may modify the list "in place."
        return new ArrayList<String>(Arrays.asList("ec2AccessId",
                                                   "ec2SecretKey",
                                                   "ec2Ami",
                                                   "ec2KeyPairId",
                                                   "ec2HostUserId",
                                                   "ec2VoldemortRootDirectory",
                                                   "ec2VoldemortHomeDirectory",
                                                   "ec2SourceDirectory",
                                                   "ec2ParentDirectory",
                                                   "ec2ClusterXmlFile",
                                                   "ec2InstanceCount",
                                                   "ec2InstanceIdFile"));
    }

}
