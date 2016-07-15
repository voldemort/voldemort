package voldemort.server;

import java.util.List;

import voldemort.VoldemortApplicationException;

public class MockHostMatcher extends PosixHostMatcher {

    private final String mockHostName;
    private final String mockFQDN;

    public MockHostMatcher(List<String> types, String mockHostName, String mockFQDN) {
        super(types);
        this.mockFQDN = mockFQDN;
        this.mockHostName = mockHostName;
    }

    @Override
    protected String getHost(String type) {
        if(type.equals(HostMatcher.HOSTNAME)) {
            return mockHostName;
        } else if(type.equals(HostMatcher.FQDN)) {
            return mockFQDN;
        } else {
            throw new VoldemortApplicationException("Unrecognized type " + type);
        }
    }
}
