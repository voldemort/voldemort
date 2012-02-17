package voldemort.collections;

/**
 * Key into a VLinkedPagedList. Specifies the page to look into, and the index
 * of the array within the page.
 */
public class VLinkedPagedKey {

    private final int _pageId;
    private final int _arrayIndex;

    public VLinkedPagedKey(int pageId, int arrayIndex) {
        _pageId = pageId;
        _arrayIndex = arrayIndex;
    }

    public int getPageId() {
        return _pageId;
    }

    public int getArrayIndex() {
        return _arrayIndex;
    }

    public String toString() {
        return "PageId " + _pageId + ", arrayIndex " + _arrayIndex;
    }
}
