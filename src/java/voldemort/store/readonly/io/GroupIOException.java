package voldemort.store.readonly.io;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class GroupIOException extends IOException {

    private static final long serialVersionUID = 1L;
    List<Throwable> suppressed = new ArrayList<Throwable>();

    public GroupIOException(Throwable cause) {
        suppressed.add(cause);
    }

    public void addSuppressed(Throwable t) {
        suppressed.add(t);
    }

    @Override
    public void printStackTrace(PrintStream out) {

        for(Throwable current: suppressed) {
            current.printStackTrace(out);
        }

        // this will print ourselves AND the cause...
        super.printStackTrace(out);

    }

    @Override
    public void printStackTrace(PrintWriter out) {

        for(Throwable current: suppressed) {
            current.printStackTrace(out);
        }

        // this will print ourselves AND the cause...
        super.printStackTrace(out);

    }

}
