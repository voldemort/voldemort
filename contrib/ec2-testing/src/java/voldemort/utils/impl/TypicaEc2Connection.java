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

package voldemort.utils.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import voldemort.utils.Ec2Connection;
import voldemort.utils.HostNamePair;

import com.xerox.amazonws.ec2.InstanceType;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.LaunchConfiguration;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

public class TypicaEc2Connection implements Ec2Connection {

    private static final int POLL_INTERVAL = 15;

    private final Jec2 ec2;

    private final Log logger = LogFactory.getLog(getClass());

    public TypicaEc2Connection(String accessId, String secretKey) {
        ec2 = new Jec2(accessId, secretKey);
    }

    public List<HostNamePair> list() throws Exception {
        List<HostNamePair> hostNamePairs = new ArrayList<HostNamePair>();

        for(ReservationDescription res: ec2.describeInstances(Collections.<String> emptyList())) {
            if(res.getInstances() != null) {
                for(Instance instance: res.getInstances()) {
                    HostNamePair hostNamePair = getHostNamePair(instance);

                    if(hostNamePair == null) {
                        if(logger.isWarnEnabled())
                            logger.warn("Instance "
                                        + instance.getInstanceId()
                                        + " present, but missing external and/or internal host name");

                        continue;
                    }

                    hostNamePairs.add(hostNamePair);
                }
            }
        }

        return hostNamePairs;
    }

    public List<HostNamePair> create(String ami,
                                     String keypairId,
                                     Ec2Connection.Ec2InstanceType instanceType,
                                     int instanceCount) throws Exception {
        LaunchConfiguration launchConfiguration = new LaunchConfiguration(ami);
        launchConfiguration.setInstanceType(InstanceType.valueOf(instanceType.name()));
        launchConfiguration.setKeyName(keypairId);
        launchConfiguration.setMinCount(instanceCount);
        launchConfiguration.setMaxCount(instanceCount);

        ReservationDescription reservationDescription = ec2.runInstances(launchConfiguration);

        List<String> instanceIds = new ArrayList<String>();

        for(ReservationDescription.Instance instance: reservationDescription.getInstances()) {
            if(logger.isInfoEnabled())
                logger.info("Instance " + instance.getInstanceId() + " launched");

            instanceIds.add(instance.getInstanceId());
        }

        List<HostNamePair> hostNamePairs = new ArrayList<HostNamePair>();

        while(!instanceIds.isEmpty()) {
            try {
                if(logger.isDebugEnabled())
                    logger.debug("Sleeping for " + POLL_INTERVAL + " seconds...");

                Thread.sleep(POLL_INTERVAL * 1000);
            } catch(InterruptedException e) {
                break;
            }

            for(ReservationDescription res: ec2.describeInstances(instanceIds)) {
                if(res.getInstances() != null) {
                    for(Instance instance: res.getInstances()) {
                        String state = String.valueOf(instance.getState()).toLowerCase();

                        if(!state.equals("running")) {
                            if(logger.isDebugEnabled())
                                logger.debug("Instance " + instance.getInstanceId() + " in state: "
                                             + state);

                            continue;
                        }

                        HostNamePair hostNamePair = getHostNamePair(instance);

                        if(hostNamePair == null) {
                            if(logger.isWarnEnabled())
                                logger.warn("Instance "
                                            + instance.getInstanceId()
                                            + " in running state, but missing external and/or internal host name");

                            continue;
                        }

                        hostNamePairs.add(hostNamePair);

                        if(logger.isInfoEnabled())
                            logger.info("Instance " + instance.getInstanceId()
                                        + " running with external host name: "
                                        + hostNamePair.getExternalHostName()
                                        + ", internal host name: "
                                        + hostNamePair.getInternalHostName());

                        instanceIds.remove(instance.getInstanceId());
                    }
                }
            }
        }

        return hostNamePairs;
    }

    public void delete(List<String> hostNames) throws Exception {
        if(logger.isDebugEnabled())
            logger.debug("Deleting instances for hosts: " + hostNames);

        List<String> instanceIds = new ArrayList<String>();

        for(ReservationDescription res: ec2.describeInstances(Collections.<String> emptyList())) {
            if(res.getInstances() != null) {
                for(Instance instance: res.getInstances()) {
                    String state = String.valueOf(instance.getState()).toLowerCase();

                    if(state.equals("shutting-down")) {
                        if(logger.isDebugEnabled())
                            logger.debug("Instance " + instance.getInstanceId()
                                         + " already shutting down");

                        continue;
                    } else if(state.equals("terminated")) {
                        if(logger.isDebugEnabled())
                            logger.debug("Instance " + instance.getInstanceId()
                                         + " already terminated");

                        continue;
                    }

                    String externalHostName = instance.getDnsName() != null ? instance.getDnsName()
                                                                                      .trim() : "";

                    if(hostNames.contains(externalHostName)) {
                        instanceIds.add(instance.getInstanceId());

                        if(logger.isInfoEnabled())
                            logger.info("Instance " + instance.getInstanceId() + " ("
                                        + externalHostName + ") to be terminated");
                    }
                }
            }
        }

        // Race condition - it's possible for another entity to have terminated
        // the instances as we're running, so simply return if there's nothing
        // for us to do.
        if(instanceIds.isEmpty())
            return;

        ec2.terminateInstances(instanceIds);

        while(!instanceIds.isEmpty()) {
            try {
                if(logger.isDebugEnabled())
                    logger.debug("Sleeping for " + POLL_INTERVAL + " seconds...");

                Thread.sleep(POLL_INTERVAL * 1000);
            } catch(InterruptedException e) {
                break;
            }

            for(ReservationDescription res: ec2.describeInstances(instanceIds)) {
                if(res.getInstances() != null) {
                    for(Instance instance: res.getInstances()) {
                        String state = String.valueOf(instance.getState()).toLowerCase();

                        if(!state.equals("terminated")) {
                            if(logger.isDebugEnabled())
                                logger.debug("Instance " + instance.getInstanceId() + " in state: "
                                             + state);

                            continue;
                        }

                        instanceIds.remove(instance.getInstanceId());
                    }
                }
            }
        }
    }

    private HostNamePair getHostNamePair(Instance instance) {
        String externalHostName = instance.getDnsName() != null ? instance.getDnsName().trim() : "";
        String internalHostName = instance.getPrivateDnsName() != null ? instance.getPrivateDnsName()
                                                                                 .trim()
                                                                      : "";

        if(externalHostName.length() == 0 || internalHostName.length() == 0)
            return null;

        return new HostNamePair(externalHostName, internalHostName);
    }

}