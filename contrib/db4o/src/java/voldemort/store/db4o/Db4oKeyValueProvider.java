package voldemort.store.db4o;

import java.util.ArrayList;
import java.util.List;

import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.query.Query;

public class Db4oKeyValueProvider<Key, Value> {

    private ObjectContainer container;

    public static <Key, Value> Db4oKeyValueProvider<Key, Value> createDb4oKeyValueProvider(ObjectContainer container) {
        return new Db4oKeyValueProvider<Key, Value>(container);
    }

    private Db4oKeyValueProvider(ObjectContainer container) {
        this.container = container;
    }

    private void setContainer(ObjectContainer container) {
        this.container = container;
    }

    private ObjectContainer getContainer() {
        return container;
    }

    public List<Key> getKeys() {
        List<Key> keys = new ArrayList<Key>();
        Query query = getContainer().query();
        query.constrain(Db4oKeyValuePair.class);
        ObjectSet<Db4oKeyValuePair<Key, Value>> result = query.execute();
        while(result.hasNext()) {
            Db4oKeyValuePair<Key, Value> pair = result.next();
            keys.add(pair.getKey());
        }
        return keys;
    }

    public ObjectSet<Db4oKeyValuePair<Key, Value>> get(Key key) {
        // return getContainer().queryByExample(new
        // Db4oKeyValuePair<Key,Value>(key, null));
        Query query = getContainer().query();
        query.constrain(Db4oKeyValuePair.class);
        query.descend("key").constrain(key);
        return query.execute();
    }

    public void delete(Key key) {
        ObjectSet<Db4oKeyValuePair<Key, Value>> candidates = get(key);
        while(candidates.hasNext()) {
            getContainer().delete(candidates.next());
        }
    }

    public void set(Key key, Value value) {
        Db4oKeyValuePair<Key, Value> pair = new Db4oKeyValuePair<Key, Value>(key, value);
        getContainer().store(pair);
    }

}
