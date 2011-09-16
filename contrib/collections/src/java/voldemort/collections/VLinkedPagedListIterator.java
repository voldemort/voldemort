package voldemort.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import voldemort.serialization.Serializer;

/**
 * @author jko
 * 
 */
public class VLinkedPagedListIterator<I, LK extends Comparable<LK>> implements
        MappedListIterator<VLinkedPagedKey, LK> {

    private final Serializer<LK> _serializer;

    /*
     * Internal iterator to track the position inside the index
     */
    private ListIterator<byte[]> _keyIterator;
    private VListIterator<List<byte[]>> _indexIterator;
    private int _currentIndexId = VStack.NULL_ID;
    private VLinkedPagedKey _lastId = null;

    VLinkedPagedListIterator(VStack<I, List<byte[]>> index,
                             Serializer<LK> serializer,
                             LK linearKey,
                             boolean next,
                             int pageId) {
        _serializer = serializer;
        _currentIndexId = pageId;
        if(next) {
            _indexIterator = index.listIterator(pageId);
            if(!_indexIterator.hasNext()) {
                _keyIterator = new ArrayList<byte[]>(0).listIterator();
            } else {
                List<byte[]> byteList = _indexIterator.next().getValue();
                if(linearKey == null) {
                    _keyIterator = byteList.listIterator();
                } else {
                    boolean found = false;
                    int position = 0;
                    while(!found) {
                        position = Collections.binarySearch(byteList,
                                                            _serializer.toBytes(linearKey),
                                                            new LKByteReverseComparator<LK>(_serializer));
                        if(position < 0) {
                            position = -1 * (position + 1);
                        }
                        if(position < byteList.size()) {
                            found = true;
                        } else {
                            byteList = _indexIterator.next().getValue();
                        }
                    }
                    _keyIterator = byteList.listIterator(position);
                }
            }
        } else {
            _indexIterator = index.previousListIterator(pageId);
            if(!_indexIterator.hasPrevious()) {
                _keyIterator = new ArrayList<byte[]>(0).listIterator();
            } else {
                List<byte[]> byteList = _indexIterator.previous().getValue();
                if(linearKey == null) {
                    _keyIterator = byteList.listIterator(byteList.size());
                } else {
                    boolean found = false;
                    int position = 0;
                    while(!found) {
                        position = Collections.binarySearch(byteList,
                                                            serializer.toBytes(linearKey),
                                                            new LKByteReverseComparator<LK>(_serializer));
                        if(position < 0) {
                            position = -1 * (position + 1);
                        }
                        if(position > 0) {
                            found = true;
                        } else {
                            byteList = _indexIterator.previous().getValue();
                        }
                    }
                    // position should be no larger than byteList.size()
                    _keyIterator = byteList.listIterator(Math.min(position + 1, byteList.size()));
                }
            }
        }
    }

    public void add(LK e) {
        throw new UnsupportedOperationException();
    }

    public boolean hasNext() {
        // put the cursor in the right position
        if(_indexIterator.nextId() != null && _indexIterator.lastId() != null) {
            if(_indexIterator.nextId().equals(_indexIterator.lastId())) {
                _indexIterator.next();
            }
        }

        while(!_keyIterator.hasNext()) {
            if(_indexIterator.hasNext()) {
                _currentIndexId = _indexIterator.nextId();
                _keyIterator = _indexIterator.next().getValue().listIterator();
            } else {
                break;
            }
        }

        return _keyIterator.hasNext();
    }

    public boolean hasPrevious() {
        // put the cursor in the right position
        if(_indexIterator.previousId() != null && _indexIterator.lastId() != null) {
            if(_indexIterator.previousId().equals(_indexIterator.lastId())) {
                _indexIterator.previous();
            }
        }

        while(!_keyIterator.hasPrevious()) {
            if(_indexIterator.hasPrevious()) {
                _currentIndexId = _indexIterator.previousId();
                List<byte[]> byteList = _indexIterator.previous().getValue();
                _keyIterator = byteList.listIterator(byteList.size());
            } else {
                break;
            }
        }

        return _keyIterator.hasPrevious();
    }

    public LK next() {
        if(hasNext()) {
            _lastId = new VLinkedPagedKey(_currentIndexId, _keyIterator.nextIndex());
            return _serializer.toObject(_keyIterator.next());
        } else {
            throw new NoSuchElementException();
        }
    }

    public VLinkedPagedKey nextId() {
        if(hasNext())
            return new VLinkedPagedKey(_currentIndexId, _keyIterator.nextIndex());
        else
            return null;
    }

    public LK previous() {
        if(hasPrevious()) {
            _lastId = new VLinkedPagedKey(_currentIndexId, _keyIterator.previousIndex());
            return _serializer.toObject((_keyIterator.previous()));
        } else {
            throw new NoSuchElementException();
        }
    }

    public VLinkedPagedKey previousId() {
        if(hasPrevious())
            return new VLinkedPagedKey(_currentIndexId, _keyIterator.previousIndex());
        else
            return null;
    }

    public VLinkedPagedKey lastId() {
        return _lastId;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void set(LK e) {
        throw new UnsupportedOperationException();
    }

    /**
     * Comparator that does a reverse comparison of serialized LK objects. Used
     * because elements in the List<byte[]> in the index are sorted in
     * descending order.
     * 
     * @param <LK>
     */
    private static class LKByteReverseComparator<LK extends Comparable<LK>> implements
            Comparator<byte[]> {

        private final Serializer<LK> _serializer;

        private LKByteReverseComparator(Serializer<LK> serializer) {
            _serializer = serializer;
        }

        public int compare(byte[] arg0, byte[] arg1) {
            LK lk0 = _serializer.toObject(arg0);
            LK lk1 = _serializer.toObject(arg1);
            return lk1.compareTo(lk0);
        }

    }

}
