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
import voldemort.utils.Ec2ConnectionListener;
import voldemort.utils.HostNamePair;

import com.xerox.amazonws.ec2.InstanceType;
import com.xerox.amazonws.ec2.Jec2;
import com.xerox.amazonws.ec2.LaunchConfiguration;
import com.xerox.amazonws.ec2.ReservationDescription;
import com.xerox.amazonws.ec2.ReservationDescription.Instance;

/**
 * TypicaEc2Connection implements the Ec2Connection interface using the Typica
 * library (http://code.google.com/p/typica/) for EC2 access.
 * 
 */

public class TypicaEc2Connection implements Ec2Connection {

    private static final int POLL_INTERVAL = 15;

    private final Jec2 ec2;

    private final Ec2ConnectionListener listener;

    private final Log logger = LogFactory.getLog(getClass());

    public TypicaEc2Connection(String accessId, String secretKey) {
        this(accessId, secretKey, null, null);
    }

    public TypicaEc2Connection(String accessId,
                               String secretKey,
                               Ec2ConnectionListener listener,
                               String regionUrl) {
        ec2 = new Jec2(accessId, secretKey);
        this.listener = listener;

        if(regionUrl != null && ec2 != null)
            ec2.setRegionUrl(regionUrl);
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

    public List<HostNamePair> createInstances(String ami,
                                              String keypairId,
                                              Ec2Connection.Ec2InstanceType instanceType,
                                              int instanceCount,
                                              List<String> securityGroups) throws Exception {
        LaunchConfiguration launchConfiguration = new LaunchConfiguration(ami);
        launchConfiguration.setInstanceType(InstanceType.valueOf(instanceType.name()));
        launchConfiguration.setKeyName(keypairId);
        launchConfiguration.setMinCount(instanceCount);
        launchConfiguration.setMaxCount(instanceCount);
        if (securityGroups != null && securityGroups.size() > 0) {
            launchConfiguration.setSecurityGroup(securityGroups);
        }

        ReservationDescription reservationDescription = ec2.runInstances(launchConfiguration);

        List<String> instanceIds = new ArrayList<String>();

        for(ReservationDescription.Instance instance: reservationDescription.getInstances()) {
            String instanceId = instance.getInstanceId();

            if(logger.isInfoEnabled())
                logger.info("Instance " + instanceId + " launched");

            instanceIds.add(instanceId);

            if(listener != null)
                listener.instanceCreated(instanceId);
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

    public void deleteInstancesByHostName(List<String> hostNames) throws Exception {
        if(logger.isDebugEnabled())
            logger.debug("Deleting instances for hosts: " + hostNames);

        List<String> instanceIds = new ArrayList<String>();

        for(ReservationDescription res: ec2.describeInstances(Collections.<String> emptyList())) {
            if(res.getInstances() != null) {
                for(Instance instance: res.getInstances()) {
                    String externalHostName = instance.getDnsName() != null ? instance.getDnsName()
                                                                                      .trim() : "";

                    // We're only in charge of terminating the instances that
                    // were passed in via hostNames, so if the host name we find
                    // isn't on that list then ignore it.
                    if(!hostNames.contains(externalHostName))
                        continue;

                    String state = String.valueOf(instance.getState()).toLowerCase();
                    String instanceId = instance.getInstanceId();

                    if(state.equals("shutting-down") || state.equals("terminated")) {
                        // We expect that this code is terminating the
                        // instances. If they're not in the expected state, log
                        // it and ignore the instance.
                        if(logger.isWarnEnabled())
                            logger.warn("Instance " + instanceId + " in state \""
                                        + instance.getState() + "\" - ignoring");

                        continue;
                    }

                    instanceIds.add(instanceId);

                    if(logger.isInfoEnabled())
                        logger.info("Instance " + instanceId + " (" + externalHostName
                                    + ") to be terminated");
                }
            }
        }

        deleteInstancesByInstanceId(instanceIds);
    }

    public void deleteInstancesByInstanceId(List<String> instanceIds) throws Exception {
        // Race condition - it's possible for another entity to have terminated
        // the instances as we're running, so simply return if there's nothing
        // for us to do.
        if(instanceIds.isEmpty())
            return;

        if(logger.isDebugEnabled())
            logger.debug("Deleting instances: " + instanceIds);

        ec2.terminateInstances(instanceIds);
        int count = instanceIds.size();

        while(count > 0) {
            try {
                if(logger.isDebugEnabled())
                    logger.debug("Sleeping for " + POLL_INTERVAL + " seconds...");

                Thread.sleep(POLL_INTERVAL * 1000);
            } catch(InterruptedException e) {
                break;
            }

            for(ReservationDescription res: ec2.describeInstances(instanceIds)) {
                if(res.getInstances() == null)
                    break;

                for(Instance instance: res.getInstances()) {
                    String state = String.valueOf(instance.getState()).toLowerCase();
                    String instanceId = instance.getInstanceId();

                    if(!state.equals("terminated")) {
                        if(logger.isDebugEnabled())
                            logger.debug("Instance " + instanceId + " in state: " + state);

                        continue;
                    }

                    count--;

                    if(listener != null)
                        listener.instanceDestroyed(instanceId);
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
