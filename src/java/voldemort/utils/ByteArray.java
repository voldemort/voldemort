package voldemort.utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * A byte array container that provides an equals and hashCode pair based on the
 * contents of the byte array. This is useful as a key for Maps.
 */
public final class ByteArray implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final ByteArray EMPTY = new ByteArray();

    private final byte[] underlying;

    public ByteArray(byte... underlying) {
        this.underlying = Utils.notNull(underlying, "underlying");
    }

    public byte[] get() {
        return underlying;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(underlying);
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof ByteArray))
            return false;
        ByteArray other = (ByteArray) obj;
        return Arrays.equals(underlying, other.underlying);
    }

    @Override
    public String toString() {
        return Arrays.toString(underlying);
    }

    /**
     * Translate the each ByteArray in an iterable into a hexidecimal string
     * 
     * @param arrays The array of bytes to translate
     * @return An iterable of converted strings
     */
    public static Iterable<String> toHexStrings(Iterable<ByteArray> arrays) {
        ArrayList<String> ret = new ArrayList<String>();
        for(ByteArray array: arrays)
            ret.add(ByteUtils.toHexString(array.get()));
        return ret;
    }

    public int length() {
        return underlying.length;
    }
}
