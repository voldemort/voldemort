package voldemort.performance.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import voldemort.store.bdb.BdbStorageConfiguration;
import voldemort.store.views.View;
import voldemort.utils.Props;
import voldemort.utils.ReflectUtils;
import voldemort.utils.Utils;

/**
 * @author nnarkhed
 * 
 */
public class BenchmarkViews {

    private Benchmark benchmark;

    public BenchmarkViews() {}

    public static View<?, ?, ?, ?> loadTransformation(String className) {
        if(className == null)
            return null;
        Class<?> transClass = ReflectUtils.loadClass(className.trim());
        return (View<?, ?, ?, ?>) ReflectUtils.callConstructor(transClass, new Object[] {});
    }

    public static List<String> getTransforms() {
        List<String> transforms = new ArrayList<String>();
        transforms.add("concat");
        transforms.add("concat-upper");
        return transforms;
    }

    public void runBenchmark(Props props) throws Exception {
        benchmark = new Benchmark();
        benchmark.initialize(props);
        benchmark.warmUpAndRun();
        benchmark.close();
    }

    public static void main(String[] args) throws IOException {
        OptionParser parser = new OptionParser();
        parser.accepts(Benchmark.STORE_NAME, "store name on the remote URL")
              .withRequiredArg()
              .ofType(String.class);
        parser.accepts(Benchmark.RECORD_COUNT, "number of records inserted during warmup phase")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(Benchmark.OPS_COUNT, "number of operations to do")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts(Benchmark.URL, "url on which to run remote tests")
              .withOptionalArg()
              .ofType(String.class);
        parser.accepts(Benchmark.HELP);

        for(String arg: args) {
            System.out.println("Arguments = " + arg);
        }
        OptionSet options = parser.parse(args);

        if(options.has(Benchmark.HELP)) {
            parser.printHelpOn(System.out);
            System.exit(0);
        }

        if(!options.has(Benchmark.STORE_NAME) || !options.has(Benchmark.RECORD_COUNT)
           || !options.has(Benchmark.OPS_COUNT)) {
            parser.printHelpOn(System.out);
            Utils.croak("Missing params");
            System.exit(0);
        }

        Props props = new Props();

        props.put(Benchmark.STORE_NAME, (String) options.valueOf(Benchmark.STORE_NAME));
        props.put(Benchmark.RECORD_COUNT, (Integer) options.valueOf(Benchmark.RECORD_COUNT));
        props.put(Benchmark.OPS_COUNT, (Integer) options.valueOf(Benchmark.OPS_COUNT));
        props.put(Benchmark.STORAGE_ENGINE_TYPE, BdbStorageConfiguration.TYPE_NAME);
        props.put(Benchmark.STORE_TYPE, "view");
        props.put(Benchmark.VIEW_CLASS, "voldemort.store.views.UpperCaseView");
        props.put(Benchmark.HAS_TRANSFORMS, "true");

        BenchmarkViews benchmark = null;
        try {
            benchmark = new BenchmarkViews();
            benchmark.runBenchmark(props);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
