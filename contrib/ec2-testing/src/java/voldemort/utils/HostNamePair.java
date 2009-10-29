package voldemort.utils;

public class HostNamePair {

    private final String externalHostName;

    private final String internalHostName;

    public HostNamePair(String externalHostName, String internalHostName) {
        this.externalHostName = externalHostName;
        this.internalHostName = internalHostName;
    }

    public String getExternalHostName() {
        return externalHostName;
    }

    public String getInternalHostName() {
        return internalHostName;
    }

}
