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

package voldemort.rest.coordinator.admin;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.ChannelPipelineFactory;

import voldemort.annotations.jmx.JmxManaged;
import voldemort.common.service.ServiceType;
import voldemort.rest.AbstractRestService;
import voldemort.rest.coordinator.CoordinatorConfig;
import voldemort.rest.coordinator.CoordinatorMetadata;

/**
 * Admin service that accepts REST requests from the Voldemort admin client and
 * does the operation on the coordinator.
 */
@JmxManaged(description = "Coordinator Admin Service")
public class CoordinatorAdminService extends AbstractRestService {

    private static final Logger logger = Logger.getLogger(CoordinatorAdminService.class);
    private CoordinatorConfig coordinatorConfig = null;
    private final CoordinatorMetadata coordinatorMetadata;

    public CoordinatorAdminService(CoordinatorConfig config) {
        super(ServiceType.COORDINATOR_ADMIN, config);
        this.coordinatorConfig = config;
        this.coordinatorMetadata = new CoordinatorMetadata();
    }

    @Override
    protected void initialize() {}

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected ChannelPipelineFactory getPipelineFactory() {
        return new CoordinatorAdminPipelineFactory(this.coordinatorMetadata, this.coordinatorConfig);
    }

    @Override
    protected int getServicePort() {
        return this.coordinatorConfig.getAdminPort();
    }

    @Override
    protected String getServiceName() {
        return ServiceType.COORDINATOR_ADMIN.getDisplayName();
    }
}
