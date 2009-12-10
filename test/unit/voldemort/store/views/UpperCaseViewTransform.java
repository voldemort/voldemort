package voldemort.store.views;

import voldemort.store.Store;

/**
 * A simple test view that translates all reads into uppercase
 * 
 * @author jay
 * 
 */
public class UpperCaseViewTransform implements ViewTransformation<String, String, String> {

    public String fromStoreToView(Store<String, String> store, String k, String s) {
        return s.toUpperCase();
    }

    public String fromViewToStore(Store<String, String> store, String k, String v) {
        throw new UnsupportedViewOperationException("View not writable");
    }

}
