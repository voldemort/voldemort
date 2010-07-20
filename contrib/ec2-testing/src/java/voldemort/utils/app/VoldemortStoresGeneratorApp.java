package voldemort.utils.app;

import java.util.List;

import joptsimple.OptionSet;
import voldemort.routing.RoutingStrategyType;
import voldemort.utils.CmdUtils;
import voldemort.utils.StoresGenerator;

public class VoldemortStoresGeneratorApp extends VoldemortApp {

    public static void main(String[] args) throws Exception {
        new VoldemortStoresGeneratorApp().run(args);
    }

    @Override
    protected String getScriptName() {
        return "voldemort-storesgenerator.sh";
    }

    @Override
    public void run(String[] args) throws Exception {
        parser.accepts("help", "Prints this help");
        parser.accepts("logging",
                       "Options are \"debug\", \"info\" (default), \"warn\", \"error\", or \"off\"")
              .withRequiredArg();
        parser.accepts("storename", "Store name; defaults to test").withRequiredArg();
        parser.accepts("zonerepfactor",
                       "Zone specific replication factor as a list of comma separated integers")
              .withRequiredArg()
              .ofType(Integer.class)
              .withValuesSeparatedBy(',');
        parser.accepts("routing-strategy",
                       "Routing Strategy (" + RoutingStrategyType.CONSISTENT_STRATEGY
                               + " (default), " + RoutingStrategyType.ZONE_STRATEGY + ", "
                               + RoutingStrategyType.CONSISTENT_STRATEGY + ")").withRequiredArg();
        parser.accepts("required-reads", "Required number of reads")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("required-writes", "Required number of writes")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("zone-count-read", "Number of zones to read from; default 0 ")
              .withRequiredArg()
              .ofType(Integer.class);
        parser.accepts("zone-count-write", "Number of zones to write from; default 0 ")
              .withRequiredArg()
              .ofType(Integer.class);

        OptionSet options = parse(args);
        String storeName = CmdUtils.valueOf(options, "storename", "test");

        List<Integer> zoneRepFactor = getRequiredListIntegers(options, "zonerepfactor");
        int requiredReads = getRequiredInt(options, "required-reads");
        int requiredWrites = getRequiredInt(options, "required-writes");
        int zoneCountRead = CmdUtils.valueOf(options, "zone-count-read", 0);
        int zoneCountWrite = CmdUtils.valueOf(options, "zone-count-write", 0);
        String routingStrategy = CmdUtils.valueOf(options,
                                                  "routing-strategy",
                                                  RoutingStrategyType.CONSISTENT_STRATEGY);
        String storesXml = new StoresGenerator().createStoreDescriptor(storeName,
                                                                       zoneRepFactor,
                                                                       requiredReads,
                                                                       requiredWrites,
                                                                       zoneCountRead,
                                                                       zoneCountWrite,
                                                                       routingStrategy);

        System.out.print(storesXml);
    }
}
