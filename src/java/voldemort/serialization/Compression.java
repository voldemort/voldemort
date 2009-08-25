package voldemort.serialization;

import com.google.common.base.Preconditions;

public final class Compression {

    private final String type;

    private final String options;

    public Compression(String type, String options) {
        Preconditions.checkNotNull(type);
        this.type = type;
        this.options = options;
    }

    public String getType() {
        return type;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime * 1 + type.hashCode();
        return prime * result + (options == null ? 0 : options.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(!(obj instanceof Compression))
            return false;
        Compression other = (Compression) obj;
        return type.equals(other.type)
               && (options == null ? other.options == null : options.equals(other.options));
    }

    public String getOptions() {
        return options;
    }
}
