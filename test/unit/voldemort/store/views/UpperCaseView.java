package voldemort.store.views;

import voldemort.store.Store;

/**
 * A simple test view that translates all reads into uppercase
 * 
 * 
 */
public class UpperCaseView implements View<String, String, String, String> {

    public String storeToView(Store<String, String, String> store, String k, String s, String t) {
        if(t == null)
            return s;
        if(t.compareTo("concat") == 0)
            return s.concat(s);
        if(t.compareTo("concat-upper") == 0)
            return s.concat(s).toUpperCase();
        throw new UnsupportedViewOperationException("Transform should be either concat or concat-upper");
    }

    public String viewToStore(Store<String, String, String> store, String k, String v, String t) {
        if(t == null)
            return v;
        if(t.compareTo("concat") == 0)
            return v.concat(v);
        if(t.compareTo("concat-upper") == 0)
            return v.concat(v).toUpperCase();
        throw new UnsupportedViewOperationException("Transform should be either concat or concat-upper");
    }
}
