package voldemort.utils;

import java.io.Serializable;

import com.google.common.base.Function;
import com.google.common.base.Objects;

/**
 * Represents a pair of items.
 */
public final class Pair<F, S> implements Serializable, Function<F, S> {

    private static final long serialVersionUID = 1L;

    private final F first;

    private final S second;

    /**
     * Static factory method that, unlike the constructor, performs generic
     * inference saving some typing. Use in the following way (for a pair of
     * Strings):
     * 
     * <p>
     * <code>
     * Pair<String, String> pair = Pair.create("first", "second");
     * </code>
     * </p>
     * 
     * @param <F> The type of the first thing.
     * @param <S> The type of the second thing
     * @param first The first thing
     * @param second The second thing
     * @return The pair (first,second)
     */
    public static final <F, S> Pair<F, S> create(F first, S second) {
        return new Pair<F, S>(first, second);
    }

    /**
     * Use the static factory method {@link #create(Object, Object)} instead of
     * this where possible.
     * 
     * @param first
     * @param second
     */
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public S apply(F from) {
        if(from == null ? first == null : from.equals(first))
            return second;
        return null;
    }

    public final F getFirst() {
        return first;
    }

    public final S getSecond() {
        return second;
    }

    @Override
    public final int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((first == null) ? 0 : first.hashCode());
        result = PRIME * result + ((second == null) ? 0 : second.hashCode());
        return result;
    }

    @Override
    public final boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof Pair<?, ?>))
            return false;

        final Pair<?, ?> other = (Pair<?, ?>) (obj);
        return Objects.equal(first, other.first) && Objects.equal(second, other.second);
    }

    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[ " + first + ", " + second + " ]");
        return builder.toString();
    }
}
