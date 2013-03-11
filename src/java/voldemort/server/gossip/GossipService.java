/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.server.gossip;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.common.service.AbstractService;
import voldemort.common.service.SchedulerService;
import voldemort.common.service.ServiceType;
import voldemort.server.VoldemortConfig;
import voldemort.store.metadata.MetadataStore;
import voldemort.utils.RebalanceUtils;

/**
 * This service runs a metadata Gossip protocol. See
 * {@link voldemort.server.gossip.Gossiper} for more details.
 */
@JmxManaged(description = "Gossip protocol for synchronizing state/configuration in a cluster.")
public class GossipService extends AbstractService {

    private final SchedulerService schedulerService;
    private final Gossiper gossiper;
    private final AdminClient adminClient;

    public GossipService(MetadataStore metadataStore,
                         SchedulerService service,
                         VoldemortConfig voldemortConfig) {
        super(ServiceType.GOSSIP);
        schedulerService = service;
        adminClient = RebalanceUtils.createTempAdminClient(voldemortConfig,
                                                           metadataStore.getCluster(),
                                                           4);
        gossiper = new Gossiper(metadataStore, adminClient, voldemortConfig.getGossipInterval());
    }

    @Override
    protected void startInner() {
        gossiper.start();
        schedulerService.scheduleNow(gossiper);
    }

    @Override
    protected void stopInner() {
        gossiper.stop();
        adminClient.close();
    }
}
