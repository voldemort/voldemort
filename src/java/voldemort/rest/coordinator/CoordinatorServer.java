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

package voldemort.rest.coordinator;

import static voldemort.utils.Utils.croak;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.common.service.AbstractService;
import voldemort.common.service.ServiceType;
import voldemort.common.service.VoldemortService;
import voldemort.rest.coordinator.admin.CoordinatorAdminService;
import voldemort.rest.coordinator.config.CoordinatorConfig;
import voldemort.rest.coordinator.config.StoreClientConfigService;
import voldemort.utils.Utils;

import com.google.common.collect.ImmutableList;

/**
 * This is the main coordinator server, it bootstraps all the services. It can
 * be embedded or run directly via it's main method.
 */
public class CoordinatorServer extends AbstractService {

    private static final Logger logger = Logger.getLogger(CoordinatorServer.class.getName());
    private final List<VoldemortService> services;
    private final CoordinatorConfig config;

    public CoordinatorServer(CoordinatorConfig config) {
        super(ServiceType.COORDINATOR_SERVER);
        this.config = config;
        this.services = createServices();
    }

    private List<VoldemortService> createServices() {
        List<VoldemortService> services = new ArrayList<VoldemortService>();
        CoordinatorProxyService coordinator = new CoordinatorProxyService(config);
        StoreClientConfigService.initialize(config);
        services.add(coordinator);
        if (config.isAdminServiceEnabled()) {
            services.add(new CoordinatorAdminService(config));
        }
        return ImmutableList.copyOf(services);
    }

    @Override
    protected void startInner() throws VoldemortException {
        logger.info("Starting " + services.size() + " services.");
        long start = System.currentTimeMillis();
        for (VoldemortService service: services) {
            service.start();
        }
        long end = System.currentTimeMillis();
        logger.info("Coordinator startup completed in " + (end - start) + " ms.");
    }

    @Override
    protected void stopInner() throws VoldemortException {
        List<VoldemortException> exceptions = new ArrayList<VoldemortException>();
        /* Stop in reverse order */
        for (VoldemortService service: Utils.reversed(services)) {
            try {
                service.stop();
            } catch (VoldemortException e) {
                exceptions.add(e);
                logger.error(e);
            }
        }
        if (exceptions.size() > 0) {
            throw exceptions.get(0);
        }
    }

    public static void main(String[] args) throws Exception {
        CoordinatorConfig config = null;
        try {
            if (args.length != 1) {
                croak("USAGE: java " + CoordinatorProxyService.class.getName() + " <coordinator_config_file>");
                System.exit(-1);
            }
            config = new CoordinatorConfig(new File(args[0]));
        } catch (Exception e) {
            logger.error(e);
            Utils.croak("Error while loading configuration: " + e.getMessage());
        }
        final CoordinatorServer coordinatorServer = new CoordinatorServer(config);
        if (!coordinatorServer.isStarted()) {
            coordinatorServer.start();
        }
    }

    public List<VoldemortService> getServices() {
        return services;
    }

    public VoldemortService getService(ServiceType type) {
        for (VoldemortService service: services)
            if (service.getType().equals(type))
                return service;
        throw new IllegalStateException(type.getDisplayName() + " has not been initialized.");
    }

    public CoordinatorConfig getCoordinatorConfig() {
        return this.config;
    }
}
