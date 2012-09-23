/*
 * Copyright 2008-2012 LinkedIn, Inc
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

package voldemort.client.scheduler;

import org.apache.log4j.Logger;

import voldemort.client.ClientInfo;
import voldemort.client.SystemStoreRepository;
import voldemort.versioning.ObsoleteVersionException;
import voldemort.versioning.Version;
import voldemort.versioning.Versioned;

/**
 * An async. job that keeps client registry refreshed while the client is
 * connected to the cluster
 * 
 */
public class ClientRegistryRefresher implements Runnable {

    private final Logger logger = Logger.getLogger(this.getClass());

    SystemStoreRepository systemStoreRepository;
    private ClientInfo clientInfo;
    private final String clientId;
    private Version lastVersion;
    private boolean hadConflict;

    public ClientRegistryRefresher(SystemStoreRepository sysRepository,
                                   String clientId,
                                   ClientInfo clientInfo,
                                   Version version) {
        this.systemStoreRepository = sysRepository;
        this.clientInfo = clientInfo;
        this.clientId = clientId;
        this.lastVersion = version;
        this.hadConflict = false;
        logger.info("Initial version obtained from client registry: " + version);
    }

    /*
     * Procedure to publish client registry info in the system store.
     */
    public synchronized void publishRegistry() {
        try {
            if(hadConflict) {
                /*
                 * if we previously had a conflict during update, we will try to
                 * get a newer version before update this time. This case shall
                 * not happen under regular circumstances. But it is just avoid
                 * update keeping failing when strange situations occur.
                 */
                lastVersion = this.systemStoreRepository.getClientRegistryStore()
                                                        .getSysStore(clientId)
                                                        .getVersion();
                hadConflict = false;
            }
            clientInfo.setUpdateTime(System.currentTimeMillis());
            logger.info("updating client registry with the following info for client: " + clientId
                        + "\n" + clientInfo);
            lastVersion = this.systemStoreRepository.getClientRegistryStore()
                                                    .putSysStore(clientId,
                                                                 new Versioned<String>(clientInfo.toString(),
                                                                                       lastVersion));
        } catch(ObsoleteVersionException e) {
            Versioned<String> existingValue = this.systemStoreRepository.getClientRegistryStore()
                                                                        .getSysStore(clientId);
            logger.warn("Multiple clients are updating the same client registry entry");
            logger.warn("  current value: " + clientInfo + " " + lastVersion);
            logger.warn("  existing value: " + existingValue.getValue() + " "
                        + existingValue.getVersion());
            hadConflict = true;
        } catch(Exception e) {
            logger.warn("encountered the following error while trying to update client registry: "
                        + e);
        }
    }

    public void run() {
        publishRegistry();
    }
}
