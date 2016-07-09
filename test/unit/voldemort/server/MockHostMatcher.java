package voldemort.server;

import io.tehuti.utils.Utils;

public class MockHostMatcher extends HostMatcher {
    private final String mockFQDN;

    public MockHostMatcher(String mockFQDN) {
        super();
        this.mockFQDN = Utils.notNull(mockFQDN);
    }

    @Override
    protected String getHost() {
        return mockFQDN;
    }
}
