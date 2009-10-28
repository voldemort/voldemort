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

/**
 * The ClusterCleaner class represents the task of remotely cleaning the data
 * directory for a given Voldemort configuration directory. That is, given a
 * value <i>myconfig</i> for the <i>Voldemort home</i> configuration attribute,
 * the remote directory <i>myconfig/data</i> will be deleted. For example, if
 * the remote Voldemort home directory is
 * <i>voldemort/config/single_node_cluster</i>, the directory
 * <i>voldemort/config/single_node_cluster/data</i> will be deleted by this
 * operation.
 * 
 * <p/>
 * 
 * Implementation notes:
 * 
 * <ol>
 * <li>Implementations must provide a reasonable guarantee that the data was
 * actually deleted. An error should be raised, therefore, if the directory was
 * unable to be deleted, independent of cause (security permissions, for
 * example).
 * <li>In the event that the remote data directory was not already present, no
 * error should be thrown. The operation should simply return. A warning message
 * may be output but the application logic should continue.
 * <li>The deletion should happen recursively and thus include all child files
 * and sub-directories.
 * <li>No precaution or assumption need be made as to the state of the data
 * being deleted. For example, no checks are made to ensure that a Voldemort
 * server is not currently running on the given remote host, using this data
 * directory upon which this operation is being performed.
 * </ol>
 * 
 * @author Kirk True
 */

public interface ClusterCleaner extends RemoteOperation<Object> {

}