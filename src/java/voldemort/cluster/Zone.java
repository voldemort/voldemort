package voldemort.cluster;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;

public class Zone implements Serializable {

    private static final long serialVersionUID = 1;
    public static final int DEFAULT_ZONE_ID = 0;

    private int zoneId;
    private LinkedList<Integer> proximityList;

    public Zone(int zoneId, LinkedList<Integer> proximityList) {
        this.zoneId = zoneId;
        this.proximityList = proximityList;
    }

    public Zone() {
        this.zoneId = 0;
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
    public int hashCode() {
        return getId() ^ getProximityList().size();
    }

    public int getId() {
        return this.zoneId;
    }

    public LinkedList<Integer> getProximityList() {
        return proximityList;
    }
}
