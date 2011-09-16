package voldemort.collections;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import voldemort.serialization.Serializer;

/**
 * Keeps tracking of a single page and its associated first entry.
 */
public class VPageIndexEntry<LK extends Comparable<LK>> {

    private static final String PAGE_ID = "pageId";
    private static final String LAST_INDEX = "lastIndex";

    private final Map<String, Object> _map;

    private final Serializer<LK> _serializer;

    public VPageIndexEntry(int pageId, LK lastIndex, Serializer<LK> serializer) {
        this(pageId, serializer.toBytes(lastIndex), serializer);
    }

    public VPageIndexEntry(int pageId, byte[] lastIndexBytes, Serializer<LK> serializer) {
        Map<String, Object> tempMap = new HashMap<String, Object>();
        tempMap.put(PAGE_ID, pageId);
        tempMap.put(LAST_INDEX, lastIndexBytes);
        _map = Collections.unmodifiableMap(tempMap);
        _serializer = serializer;
    }

    public static <LK extends Comparable<LK>> VPageIndexEntry<LK> valueOf(Map<String, Object> map,
                                                                          Serializer<LK> serializer) {
        byte[] lastIndexBytes = (byte[]) map.get(LAST_INDEX);
        LK lastIndex = serializer.toObject(lastIndexBytes);
        return new VPageIndexEntry<LK>((Integer) map.get(PAGE_ID), lastIndex, serializer);
    }

    public Integer getPageId() {
        return (Integer) _map.get(PAGE_ID);
    }

    public LK getLastIndex() {
        return _serializer.toObject((byte[]) _map.get(LAST_INDEX));
    }

    public Map<String, Object> mapValue() {
        return _map;
    }

    // copy to tmpMap to get the pretty version of LK instead of bytes
    public String toString() {
        Map<String, Object> tmpMap = new HashMap<String, Object>();
        tmpMap.put(PAGE_ID, getPageId());
        tmpMap.put(LAST_INDEX, getLastIndex().toString());
        return tmpMap.toString();
    }

}
