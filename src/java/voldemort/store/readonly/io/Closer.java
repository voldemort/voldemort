package voldemort.store.readonly.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public class Closer extends BaseCloser<Closeable> implements Closeable {

    public Closer() {}

    public Closer(List delegates) {
        this.delegates = (List<Closeable>) delegates;
    }

    public Closer(Closeable... delegates) {
        add(delegates);
    }

    @Override
    public void close() throws IOException {
        exec();
    }

    public boolean closed() {
        return executed();
    }

    public boolean isClosed() {
        return executed();
    }

    protected void onDelegate(Closeable delegate) throws IOException {
        delegate.close();
    }

}
