package voldemort.utils;

import java.util.List;

public class RemoteTestResult {

    private String hostName;

    private List<RemoteTestIteration> remoteTestIterations;

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public List<RemoteTestIteration> getRemoteTestIterations() {
        return remoteTestIterations;
    }

    public void setRemoteTestIterations(List<RemoteTestIteration> remoteTestIterations) {
        this.remoteTestIterations = remoteTestIterations;
    }

}
