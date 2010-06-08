package voldemort.store.db4o;

public class Db4oKeyValuePair<Key, Value> {

    private Key key;
    private Value value;

    public Db4oKeyValuePair(Key key, Value value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public Key getKey() {
        return key;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    public Value getValue() {
        return value;
    }
}
