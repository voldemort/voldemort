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

import java.util.List;

/**
 * Ec2Connection represents a connection to Amazon's EC2 API. The name of the
 * API doesn't denote some sort of persistent, long-lived connection. This API
 * simply serves as an abstraction of the actual underlying library/mechanism we
 * use to talk to EC2.
 * 
 * This API defines methods to create, delete, and retrieve a list of the remote
 * hosts from EC2.
 * 
 */

public interface Ec2Connection {

    public enum Ec2InstanceType {

        DEFAULT,
        LARGE,
        XLARGE,
        MEDIUM_HCPU,
        XLARGE_HCPU;

    }

    /**
     * Retrieve the host names (external/internal) of the instances launched by
     * the EC2 account. These are not filtered in any form but represent
     * <b>all</b> instances owned by this account, regardless of launch time,
     * instance size, etc.
     * 
     * @return List of HostNamePair instances
     * 
     * @throws Exception Thrown on errors
     */

    public List<HostNamePair> list() throws Exception;

    /**
     * Creates <code>instanceCount</code> number of instances of type
     * <code>instanceType</code> using the given AMI and keypair ID. The
     * implementation of this method must block until the instances are all
     * successfully started and in the <i>running</i> state (as reported by
     * EC2).
     * 
     * <p/>
     * 
     * Please note: the AMI and instance type must be compatible. For example,
     * attempting to launch a XLARGE_HCPU instance using a 32-bit AMI will
     * result in an error.
     * 
     * @param ami Amazon Machine Image; can be public, private, etc.
     * @param keypairId String representing keypair ID
     * @param instanceType Instance type to launch
     * @param instanceCount Number of instances to launch
     * 
     * @return List of HostNamePair instances
     * 
     * @throws Exception Thrown on errors
     */

    public List<HostNamePair> createInstances(String ami,
                                              String keypairId,
                                              Ec2InstanceType instanceType,
                                              int instanceCount) throws Exception;

    /**
     * Deletes the EC2 instances represented by the given external host names.
     * Like the create method, this method blocks until the instances
     * represented by the given host names have terminated.
     * 
     * @param hostNames External host names of EC2 instances to terminate
     * 
     * @throws Exception Thrown on errors
     */

    public void deleteInstancesByHostName(List<String> hostNames) throws Exception;

    /**
     * Deletes the EC2 instances represented by the given instance IDs. Like the
     * create method, this method blocks until the instances represented by the
     * given IDs have terminated.
     * 
     * @param instanceIds EC2 instance IDs to terminate
     * 
     * @throws Exception Thrown on errors
     */

    public void deleteInstancesByInstanceId(List<String> instanceIds) throws Exception;

}
