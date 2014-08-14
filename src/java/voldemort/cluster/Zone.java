package voldemort.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Zone implements Serializable, Comparable<Zone> {

    private static final long serialVersionUID = 1;
    public static final int DEFAULT_ZONE_ID = 0;
    public static final int UNSET_ZONE_ID = -1;

    private int zoneId;
    private List<Integer> proximityList;

    public Zone(int zoneId, List<Integer> proximityList) {
        this.zoneId = zoneId;
        this.proximityList = new ArrayList<Integer>(proximityList);
    }

    public Zone() {
        this.zoneId = DEFAULT_ZONE_ID;
        this.proximityList = new LinkedList<Integer>();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Zone Id " + this.zoneId + " - ");
        builder.append("Proximity List - [" + proximityList + "]");
        return builder.toString();
    }

    @Override
    public boolean equals(Object object) {
        if(this == object)
            return true;
        if(!(object instanceof Zone))
            return false;

        Zone zone = (Zone) object;
        if(getId() != zone.getId()) {
            return false;
        }
        if(this.getProximityList().size() != zone.getProximityList().size()) {
            return false;
        }
        Iterator<Integer> proxListFirst = this.getProximityList().iterator();
        Iterator<Integer> proxListSecond = zone.getProximityList().iterator();
        while(proxListFirst.hasNext() && proxListSecond.hasNext()) {
            Integer first = proxListFirst.next();
            Integer second = proxListSecond.next();
            if(!first.equals(second)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int compareTo(Zone other) {
        return Integer.valueOf(this.zoneId).compareTo(other.getId());
    }

    @Override
    public int hashCode() {
        return getId() ^ getProximityList().size();
    }

    public int getId() {
        return this.zoneId;
    }

    public List<Integer> getProximityList() {
        return proximityList;
    }
}
