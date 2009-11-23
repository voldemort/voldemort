package voldemort.store;

public class ForceFailStore<K, V> extends DelegatingStore<K, V> {

    public ForceFailStore(Store<K, V> innerStore) {
        super(innerStore);
    }

}
