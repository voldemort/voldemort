package voldemort.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import voldemort.client.StoreClient;
import voldemort.serialization.Serializer;
import voldemort.versioning.Versioned;

/**
 * VLinkedPagedList is a voldemort-store backed list that keeps track of a value
 * in relation to its position in a sorted key. Items are placed in the
 * LinkedPagedList in descending order. New elements are added to the front of
 * the list.
 * 
 * The voldemort store configuration must use JSON serialization. For the
 * stackClient, the key must be defined as a JSON object: {"value" : <JSON
 * serialization format of I>, "id" : "int32"} The value must be defined as the
 * following: {"id" : "int32", "nextId" : "int32", "previousId" : "int32",
 * "stable" : "boolean", "value" : ["bytes"]}
 * 
 * For the pageIndex, the JSON serialization format of the key is the same as
 * the JSON serialization format of I. The JSON serialization format of the
 * value is: [{"pageId" : "int32", "lastIndex" : "bytes"}]
 * 
 * @param <I> type used to identify this list
 * @param <LK> value to be stored
 */
public class VLinkedPagedList<I, LK extends Comparable<LK>> implements Iterable<LK> {

    private final I _identifier;
    // stack of an array of LK
    private final VStack<I, List<byte[]>> _index;

    private final StoreClient<I, List<Map<String, Object>>> _pageIndex;
    private final IndexEntryComparator<LK> _pageIndexComparator;

    private final Serializer<LK> _serializer;
    private final int _maxSize;
    private final int _fixedSize;

    /**
     * 
     * @param identifier name of the list
     * @param indexStack VStack to store the list of LK entries
     * @param pageIndexClient voldemort store for the page index
     * @param serializer serializes LK to/from bytes
     * @param maxSize maximum size of each list in the VStack
     * @param fixedSize once the maxSize is reached, truncate down to a standard
     *        fixedSize
     * @param iteratorSize maximum size of list to be stored within an iterator.
     *        Should comfortably fit in memory
     */
    public VLinkedPagedList(I identifier,
                            StoreClient<Map<String, Object>, Map<String, Object>> stackClient,
                            StoreClient<I, List<Map<String, Object>>> pageIndexClient,
                            Serializer<LK> serializer,
                            int maxSize,
                            int fixedSize) {
        if(identifier == null || stackClient == null || pageIndexClient == null)
            throw new NullPointerException();
        if(maxSize == 0 || fixedSize == 0)
            throw new IllegalArgumentException("maxSize and fixedSize must be greater than 0");
        if(fixedSize > maxSize)
            throw new IllegalArgumentException("fixedSize must be less than or equal to maxSize");

        _identifier = identifier;
        _index = new VStack<I, List<byte[]>>(identifier, stackClient);
        _pageIndex = pageIndexClient;
        _serializer = serializer;
        _pageIndexComparator = new IndexEntryComparator<LK>(serializer);
        _maxSize = maxSize;
        _fixedSize = fixedSize;
    }

    public void add(LK linearKey) {
        if(linearKey == null)
            throw new NullPointerException();

        Versioned<List<byte[]>> keyList = _index.peekVersioned();
        if(keyList == null) {
            // start a brand new list
            List<byte[]> keyListValue = new ArrayList<byte[]>(1);
            keyListValue.add(0, _serializer.toBytes(linearKey));
            _index.add(keyListValue);
            VPageIndexEntry<LK> indexEntry = new VPageIndexEntry<LK>(0, linearKey, _serializer);
            ArrayList<Map<String, Object>> pageIndexList = new ArrayList<Map<String, Object>>(1);
            pageIndexList.add(indexEntry.mapValue());
            _pageIndex.put(_identifier, pageIndexList);
        } else {
            List<byte[]> keyListValue = keyList.getValue();
            if(keyListValue.size() >= _maxSize) {
                // split up the list into 2 nodes
                keyListValue.add(0, _serializer.toBytes(linearKey));
                List<byte[]> fixedList = keyListValue.subList(_maxSize - _fixedSize + 1,
                                                              keyListValue.size());
                List<byte[]> truncList = keyListValue.subList(0, _maxSize - _fixedSize + 1);

                _index.setById(0, new Versioned<List<byte[]>>(fixedList, keyList.getVersion()));
                _index.add(truncList);

                LK lastIndex = _serializer.toObject(truncList.get(truncList.size() - 1));
                _pageIndex.applyUpdate(new UpdatePageIndex<I, LK>(_identifier,
                                                                  lastIndex,
                                                                  _serializer));
            } else {
                // add to existing node
                keyListValue.add(0, _serializer.toBytes(linearKey));
                _index.setById(0, keyList);
            }
        }
    }

    public LK peek() {
        List<byte[]> indexList = _index.peek();
        LK key = _serializer.toObject(indexList.get(0));
        return key;
    }

    /**
     * Return a ListIterator<K> that starts from the beginning of the list.
     * 
     * @return
     */
    public MappedListIterator<VLinkedPagedKey, LK> listIterator() {
        return listIterator(null, true);
    }

    /**
     * Return a listIterator<K>. Uses the page index to figure out what page to
     * start iterating from.
     * 
     * Runtime: 2 - 3 gets.
     * 
     * @param linearKey key to position the cursor.
     * @param next true if the cursor is to be placed directly before the first
     *        key that is less than linearKey false if the cursor is to be
     *        placed directly after the last key that is greater than linearKey
     * @return a ListIterator of <K> ids.
     */
    public MappedListIterator<VLinkedPagedKey, LK> listIterator(LK linearKey, boolean next) {
        int indexPage = 0;
        if(linearKey == null) {
            if(next) {
                indexPage = 0;
            } else {
                List<Map<String, Object>> indexList = _pageIndex.getValue(_identifier);
                if(indexList != null) {
                    VPageIndexEntry<LK> entry = VPageIndexEntry.valueOf(indexList.get(indexList.size()),
                                                                        _serializer);
                    indexPage = entry.getPageId();
                }
            }
        } else {
            List<Map<String, Object>> indexList = _pageIndex.getValue(_identifier);
            if(indexList != null) {
                Map<String, Object> searchKey = new VPageIndexEntry<LK>(0, linearKey, _serializer).mapValue();
                int position = Collections.binarySearch(indexList, searchKey, _pageIndexComparator);
                if(position < 0) {
                    position = -1 * (position + 1);
                }
                indexPage = VPageIndexEntry.valueOf(indexList.get(position), _serializer)
                                           .getPageId();
            }
        }

        return listIterator(linearKey, next, indexPage);
    }

    /**
     * Similar to listIterator(LK linearKey, boolean next) except with the
     * addition of the hint "pageId" which specifies the first page to look for
     * the key.
     * 
     * Runtime: up to O(n) gets, unless a good hint page is provided, then it
     * could be 1 - 2 gets.
     * 
     * @param linearKey
     * @param next
     * @param pageId
     * @return
     */
    public MappedListIterator<VLinkedPagedKey, LK> listIterator(LK linearKey,
                                                                boolean next,
                                                                Integer pageId) {
        return new VLinkedPagedListIterator<I, LK>(_index, _serializer, linearKey, next, pageId);
    }

    public LK getById(VLinkedPagedKey key) {
        byte[] bytes = _index.getById(key.getPageId()).get(key.getArrayIndex());
        return _serializer.toObject(bytes);
    }

    public Iterator<LK> iterator() {
        return listIterator();
    }

    /**
     * Reverse order comparison of VPageIndexEntry objects. Only compares the
     * lastIndex component.
     * 
     * @param <LK>
     */
    private static class IndexEntryComparator<LK extends Comparable<LK>> implements
            Comparator<Map<String, Object>> {

        private final Serializer<LK> _serializer;

        public IndexEntryComparator(Serializer<LK> serializer) {
            _serializer = serializer;
        }

        public int compare(Map<String, Object> arg0, Map<String, Object> arg1) {
            VPageIndexEntry<LK> entry0 = VPageIndexEntry.valueOf(arg0, _serializer);
            VPageIndexEntry<LK> entry1 = VPageIndexEntry.valueOf(arg1, _serializer);

            return entry1.getLastIndex().compareTo(entry0.getLastIndex());
        }
    }
}
