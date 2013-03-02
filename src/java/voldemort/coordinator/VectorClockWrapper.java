package voldemort.coordinator;

import java.util.ArrayList;
import java.util.List;

import voldemort.versioning.ClockEntry;
import voldemort.versioning.VectorClock;

public class VectorClockWrapper {

    private List<ClockEntry> versions;
    private long timestamp;

    public VectorClockWrapper() {
        this.versions = new ArrayList<ClockEntry>();
    }

    public VectorClockWrapper(VectorClock vc) {
        this.versions = vc.getEntries();
        this.setTimestamp(vc.getTimestamp());
    }

    public List<ClockEntry> getVersions() {
        return versions;
    }

    public void setVersions(List<ClockEntry> vectorClock) {
        versions = vectorClock;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
