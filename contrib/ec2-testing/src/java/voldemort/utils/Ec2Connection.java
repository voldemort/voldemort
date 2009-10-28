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
import java.util.Map;

/**
 * Ec2Connection represents a connection to Amazon's EC2 API. The name of the
 * API doesn't denote some sort of persistent, long-lived connection. This API
 * simply serves as an abstraction of the actual underlying library/mechanism we
 * use to talk to EC2.
 * 
 * This API defines methods to create, delete, and retrieve a list of the remote
 * hosts from EC2.
 * 
 * @author Kirk True
 */

public interface Ec2Connection {

    public Map<String, String> getInstances() throws Exception;

    public Map<String, String> createInstances(String ami,
                                               String keypairId,
                                               String instanceSize,
                                               int instanceCount) throws Exception;

    public void deleteInstances(List<String> publicHostNames) throws Exception;

}