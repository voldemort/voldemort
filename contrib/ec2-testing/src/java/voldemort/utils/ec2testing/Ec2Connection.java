package voldemort.utils.ec2testing;

import java.util.Map;

public interface Ec2Connection {

    public Map<String, String> createInstances(String ami,
                                               String keypairId,
                                               String instanceSize,
                                               int instanceCount,
                                               long timeout) throws Exception;

}