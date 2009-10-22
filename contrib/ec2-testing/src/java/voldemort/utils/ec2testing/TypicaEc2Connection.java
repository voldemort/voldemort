package voldemort.utils.ec2testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.xerox.amazonws.ec2.EC2Exception;
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

    public Map<String, String> createInstances(String ami,
                                               String keypairId,
                                               String instanceSize,
                                               int instanceCount,
                                               long timeout) throws Exception {
        List<String> instanceIds = launch(ami, keypairId, instanceSize, instanceCount);
        Map<String, String> hostNameMap = new HashMap<String, String>();

        long startTime = System.currentTimeMillis();

        while(!instanceIds.isEmpty()) {
            if(System.currentTimeMillis() > (startTime + timeout))
                throw new Error("Rollback creation NYI");

            try {
                if(logger.isInfoEnabled())
                    logger.info("Sleeping for " + POLL_INTERVAL + " seconds...");

                Thread.sleep(POLL_INTERVAL * 1000);
            } catch(InterruptedException e) {
                break;
            }

            waitForInstances(instanceIds, hostNameMap);

            if(instanceIds.isEmpty())
                break;
        }

        return hostNameMap;
    }

    private List<String> launch(String ami, String keypairId, String instanceSize, int instanceCount)
            throws EC2Exception {
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
        if(logger.isInfoEnabled())
            logger.info("Waiting for instances: " + instanceIds);

        try {
            for(ReservationDescription res: ec2.describeInstances(instanceIds)) {
                if(res.getInstances() != null) {
                    for(Instance instance: res.getInstances()) {
                        String state = String.valueOf(instance.getState()).toLowerCase();

                        if(state.equals("pending")) {
                            if(logger.isInfoEnabled())
                                logger.info("Instance " + instance.getInstanceId()
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
                                                                                       .trim()
                                                                            : "";
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
        } catch(EC2Exception e) {

        }
    }

}