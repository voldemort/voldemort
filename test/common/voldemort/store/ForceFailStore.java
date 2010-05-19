package voldemort.store;

public class ForceFailStore<K, V, T> extends DelegatingStore<K, V, T> {

    public ForceFailStore(Store<K, V, T> innerStore) {
        super(innerStore);
    }

}
