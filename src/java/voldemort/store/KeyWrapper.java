package voldemort.store;

import com.google.common.base.Objects;

/**
 * Wrapper class that handles arrays as keys properly.
 * 
 * @author jay
 * 
 */
public class KeyWrapper {

    private final Object k;

    public KeyWrapper(Object k) {
        this.k = k;
    }

    public Object get() {
        return k;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof KeyWrapper))
            return false;

        KeyWrapper key = (KeyWrapper) o;
        return Objects.deepEquals(k, key.get());
    }

    @Override
    public int hashCode() {
        return Objects.deepHashCode(k);
    }

    @Override
    public String toString() {
        return "Key(" + k.toString() + ")";
    }

}