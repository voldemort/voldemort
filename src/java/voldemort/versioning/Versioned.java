package voldemort.versioning;

import java.io.Serializable;
import java.util.Comparator;

import com.google.common.base.Objects;

/**
 * A wrapper for an object that adds a Version.
 * 
 * @author jay
 * 
 */
public final class Versioned<T> implements Serializable {

    private static final long serialVersionUID = 1;

    private volatile VectorClock version;
    private volatile T object;

    public Versioned(T object) {
        this(object, new VectorClock());
    }

    public Versioned(T object, Version version) {
        this.version = version == null ? new VectorClock() : (VectorClock) version;
        this.object = object;
    }

    public Version getVersion() {
        return version;
    }

    public T getValue() {
        return object;
    }

    public void setObject(T object) {
        this.object = object;
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object object) {
        if (this == null)
            return true;
        else if (object == null)
            return false;
        else if (!object.getClass().equals(Versioned.class))
            return false;

        Versioned versioned = (Versioned) object;
        return Objects.equal(getVersion(), versioned.getVersion())
               && Objects.deepEquals(getValue(), versioned.getValue());
    }

    @Override
    public int hashCode() {
        return 31 + version.hashCode() + 31 * object.hashCode();
    }

    @Override
    public String toString() {
        return "[" + object + ", " + version + "]";
    }

    /**
     * Create a clone of this Versioned object such that the object pointed to
     * is the same, but the VectorClock and Versioned wrapper is a shallow copy.
     */
    public Versioned<T> cloneVersioned() {
        return new Versioned<T>(this.getValue(), this.version.clone());
    }

    public static final class HappenedBeforeComparator<S> implements Comparator<Versioned<S>> {

        public int compare(Versioned<S> v1, Versioned<S> v2) {
            Occured occured = v1.getVersion().compare(v2.getVersion());
            if (occured == Occured.BEFORE)
                return -1;
            else if (occured == Occured.AFTER)
                return 1;
            else
                return 0;
        }
    }

}
