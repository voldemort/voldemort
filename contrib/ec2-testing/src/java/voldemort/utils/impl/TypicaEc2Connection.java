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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import voldemort.utils.Ec2Connection;

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

    public Map<String, String> list() throws Exception {
        List<String> dummyInstanceIds = new ArrayList<String>();
        Map<String, String> hostNameMap = new HashMap<String, String>();
        waitForInstances(dummyInstanceIds, hostNameMap);
        return hostNameMap;
    }

    public Map<String, String> create(String ami,
                                      String keypairId,
                                      String instanceSize,
                                      int instanceCount) throws Exception {
        List<String> instanceIds = launch(ami, keypairId, instanceSize, instanceCount);
        Map<String, String> hostNameMap = new HashMap<String, String>();

        while(!instanceIds.isEmpty()) {
            try {
                if(logger.isDebugEnabled())
                    logger.debug("Sleeping for " + POLL_INTERVAL + " seconds...");

                Thread.sleep(POLL_INTERVAL * 1000);
            } catch(InterruptedException e) {
                break;
            }

            waitForInstances(instanceIds, hostNameMap);
        }

        return hostNameMap;
    }

    public void delete(List<String> hostNames) throws Exception {
        if(logger.isDebugEnabled())
            logger.debug("Deleting instances for hosts: " + hostNames);

        List<String> instanceIds = new ArrayList<String>();

        for(ReservationDescription res: ec2.describeInstances(new ArrayList<String>())) {
            if(res.getInstances() != null) {
                for(Instance instance: res.getInstances()) {
                    String state = String.valueOf(instance.getState()).toLowerCase();

                    if(state.equals("pending")) {
                        if(logger.isDebugEnabled())
                            logger.debug("Instance " + instance.getInstanceId()
                                         + " in pending state");

                        continue;
                    }

                    if(!state.equals("running")) {
                        if(logger.isWarnEnabled())
                            logger.warn("Instance " + instance.getInstanceId()
                                        + " in unexpected state: " + state + ", code: "
                                        + instance.getStateCode());

                        continue;
                    }

                    String publicDnsName = instance.getDnsName() != null ? instance.getDnsName()
                                                                                   .trim() : "";

                    if(hostNames.contains(publicDnsName)) {
                        instanceIds.add(instance.getInstanceId());

                        if(logger.isInfoEnabled())
                            logger.info("Instance " + instance.getInstanceId() + " )"
                                        + instance.getDnsName() + ") to be terminated");
                    }
                }
            }
        }

        ec2.terminateInstances(instanceIds);
    }

    private List<String> launch(String ami, String keypairId, String instanceSize, int instanceCount)
            throws Exception {
        LaunchConfiguration launchConfiguration = new LaunchConfiguration(ami);
        launchConfiguration.setInstanceType(instanceSize == null ? InstanceType.DEFAULT
                                                                : InstanceType.valueOf(instanceSize));
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

        return instanceIds;
    }

    private void waitForInstances(List<String> instanceIds, Map<String, String> hostNameMap)
            throws Exception {
        if(logger.isDebugEnabled())
            logger.debug("Waiting for instances: " + instanceIds);

        for(ReservationDescription res: ec2.describeInstances(instanceIds)) {
            if(res.getInstances() != null) {
                for(Instance instance: res.getInstances()) {
                    String state = String.valueOf(instance.getState()).toLowerCase();

                    if(state.equals("pending")) {
                        if(logger.isDebugEnabled())
                            logger.debug("Instance " + instance.getInstanceId()
                                         + " in pending state");

                        continue;
                    }

                    if(!state.equals("running")) {
                        if(logger.isWarnEnabled())
                            logger.warn("Instance " + instance.getInstanceId()
                                        + " in unexpected state: " + state + ", code: "
                                        + instance.getStateCode());

                        continue;
                    }

                    String publicDnsName = instance.getDnsName() != null ? instance.getDnsName()
                                                                                   .trim() : "";
                    String privateDnsName = instance.getPrivateDnsName() != null ? instance.getPrivateDnsName()
                                                                                           .trim()
                                                                                : "";

                    if(publicDnsName.length() > 0 && privateDnsName.length() > 0) {
                        hostNameMap.put(instance.getDnsName(), instance.getPrivateDnsName());
                        instanceIds.remove(instance.getInstanceId());

                        if(logger.isInfoEnabled())
                            logger.info("Instance " + instance.getInstanceId()
                                        + " running with public DNS: " + instance.getDnsName()
                                        + ", private DNS: " + instance.getPrivateDnsName());
                    } else {
                        if(logger.isWarnEnabled())
                            logger.warn("Instance "
                                        + instance.getInstanceId()
                                        + " in running state, but missing public and/or private DNS name");
                    }
                }
            }
        }
    }

}