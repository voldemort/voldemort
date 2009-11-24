package voldemort.store.views;

/**
 * A simple test view that translates all reads into uppercase
 * 
 * @author jay
 * 
 */
public class UpperCaseViewTransform implements ViewTransformation<String, String> {

    public String fromStore(String s) {
        return s.toUpperCase();
    }

    public String fromView(String v) {
        throw new UnsupportedOperationException("View not writable");
    }

}
