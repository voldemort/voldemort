package voldemort.store.leveldb.leveldbjni;
import java.util.Random;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class LevelDBJNITestMain{

  public static void main(String[] args) throws Exception{
    OptionParser parser = new OptionParser();
    parser.accepts("help", "print help information");
    parser.accepts("path", "db path")
      .withOptionalArg()
      .describedAs("data path")
      .ofType(String.class);
    parser.accepts("winsert", "number of data to insert in the db initialy")
      .withOptionalArg()
      .describedAs("initial rows")
      .ofType(String.class);
    parser.accepts("threads", "number of client threads")
      .withOptionalArg()
      .describedAs("threads")
      .ofType(String.class);
    parser.accepts("ops", "number of operations per thread")
      .withOptionalArg()
      .describedAs("ops-number")
      .ofType(String.class);
    parser.accepts("iterations", "number of iterations")
      .withOptionalArg()
      .describedAs("iterations")
      .ofType(String.class);
    parser.accepts("poolsize", "size of the pool or 0 for no pool")
      .withOptionalArg()
      .describedAs("poolsize")
      .ofType(String.class);
    parser.accepts("datasize", "datasize in bytes")
      .withOptionalArg()
      .describedAs("datasize")
      .ofType(String.class);
    parser.accepts("blocksize", "blocksize in Kb")
      .withOptionalArg()
      .describedAs("blocksize")
      .ofType(String.class);
    parser.accepts("cachesize", "cache size in MB")
      .withOptionalArg()
      .describedAs("cachesize")
      .ofType(String.class);
    parser.accepts("writebuffer", "writebuffersize in MB")
      .withOptionalArg()
      .describedAs("writebuffer-size")
      .ofType(String.class);
    parser.accepts("rwr", "read write ratio")
      .withOptionalArg()
      .describedAs("read write ratio default 1.0 (ro)")
      .ofType(String.class);
    parser.accepts("dbstats", "prints db stats");
    parser.accepts("compact", "Compact the database first");
    parser.accepts("prepare", "insert data first");
    parser.accepts("sync", "sync to disk");
    parser.accepts("iterator", "use iterator to get instead of native function"+
      ", this option requires pooled!");
    parser.accepts("cleanFirst", "remove all data before starting");
    final OptionSet options = parser.parse(args);


    if(args.length == 0 || options.has("help")) {
       parser.printHelpOn(System.out);
      System.exit(0);
    }

    DbOptions dbo = new DbOptions();
    dbo.setCreateIfMissing(true);

    int writebuffer = 100;
    if(options.has("writebuffer"))
      writebuffer = Integer.parseInt((String)options.valueOf("writebuffer"));
    dbo.setWriteBufferSize(1024l*1024l*writebuffer);
    int cachesize = 900;
    if(options.has("cachesize"))
      cachesize = Integer.parseInt((String)options.valueOf("cachesize"));
    dbo.setLRUBlockCache(1024l*1024l*cachesize);
    int blocksize = 4;
    if(options.has("blocksize"))
      blocksize = Integer.parseInt((String)options.valueOf("blocksize"));
    dbo.setBlockSize(1024*blocksize);

    dbo.setMaxOpenFile(100);

    int _datasize = 100;
    if(options.has("datasize"))
      _datasize = Integer.parseInt((String)options.valueOf("datasize"));
    double _rwr = 1.0d;
    if(options.has("rwr"))
      _rwr = Double.parseDouble((String)options.valueOf("rwr"));
    final double rwr = _rwr;
    final int datasize = _datasize;

    String dbname = "testdb";
    if(options.has("path"))
      dbname = (String)options.valueOf("path");
    
    int _poolsize = 1;
    if(options.has("poolsize"))
      _poolsize = Integer.parseInt((String)options.valueOf("poolsize"));

    if(options.has("cleanFirst"))
      DB.destroyDB(dbo,dbname);

    final DB db = new DB(dbo,dbname,_poolsize,options.has("iterator"));

    long i = 0;
    long s = System.currentTimeMillis();
    final String data = "aaaaaaaaaaakdkeaaaaaaaaaaakdkeaaaaaaaaaaakdke";
    final WriteOptions wo = new WriteOptions();
    wo.setSync(options.has("sync"));

    
    //insert
    int _wcount = 100000;
    if(options.has("winsert"))
      _wcount = Integer.parseInt((String)options.valueOf("winsert"));
    final int wcount = _wcount;
    // boolean insert = false;
    if(options.has("prepare")){
      System.out.println("Prepare " + wcount);
      Random randomGenerator = new Random();
      long cts = System.currentTimeMillis();
      for(int j = 0; j < wcount; j++){
        if((j+1) % 10000 == 0){
          long d = System.currentTimeMillis() - cts;
          System.out.println("... "+(j+1)+" in " + (10000l*1000/d) + "tps");
          cts = System.currentTimeMillis();
        }
        byte[] ba = new byte[datasize];
        randomGenerator.nextBytes(ba);
        db.put(wo,"mykey"+j,data + new String(ba));
      }
      long delta = System.currentTimeMillis()-s;
      double tps = wcount*1000l/delta;
      System.out.println(String.format("put -> %s tps : %s MB/s",
        wcount*1000l/delta,tps * datasize / 1024 / 1024));
    }

    if(options.has("dbstats")){
      System.out.println("Stats ...");
      DbIterator it = db.newIterator(new ReadOptions());
      int count = 0;
      long ksize = 0l;
      long vsize = 0l;
      for (it.seekToFirst(); it.hasNext(); it.next()) {
        count++;
        ksize += it.key().length;
        vsize += it.value().length;
      }
      System.out.println(String.format("Num keys : %s",count));
      System.out.println(String.format("Key size : %s Mb",
        ((double)ksize) / 1024 / 1024));
      System.out.println(String.format("value size : %s Mb",
        ((double)vsize) / 1024 / 1024));
      System.out.println(db.getProperty("leveldb.stats"));

    }

    if(options.has("compact")){
      long st = System.currentTimeMillis();
      System.out.println("Compaction ...");
      db.compactDB();
      System.out.println("DB Compacted in " + 
        (System.currentTimeMillis()-st) + "ms \n" +
        db.getProperty("leveldb.stats"));
    }

    int cumulw = 0;
    int iterations=0;
    if(options.has("iterations"))
      iterations = Integer.parseInt((String)options.valueOf("iterations"));
    System.out.println("Run");

    for(int ii = 0; ii<iterations; ii++){
      // Thread.sleep(10000l);
      long istart= System.currentTimeMillis();
      int tcount = 1;
      if(options.has("threads"))
        tcount = Integer.parseInt((String)options.valueOf("threads"));
      Thread[] ta = new Thread[tcount];
      final int[] tc = new int[tcount];
      final int[] rc = new int[tcount];
      final int[] wc = new int[tcount];
      final long[] rct = new long[tcount];
      final long[] wct = new long[tcount];
      for(int t = 0 ; t< tcount; t++){
        final int ti = t;
        ta[t] = new Thread(new Runnable(){
          public void run(){
            Random randomGenerator = new Random();
            long s = System.currentTimeMillis();
            int rcount = 100000;
            int diff = 0;
            if(options.has("ops"))
              rcount = Integer.parseInt((String)options.valueOf("ops"));
            for(int j = 0; j < rcount; j++){
              long opsstart = System.currentTimeMillis();
              int k = randomGenerator.nextInt(wcount);
              if(ti!=0 && randomGenerator.nextDouble()>rwr){
                wc[ti]++;
                byte[] ba = new byte[datasize];
                randomGenerator.nextBytes(ba);
                // System.out.println("put = " + 
                  db.put(wo,("mykey"+k),data + new String(ba))
                // )
                ;
                wct[ti]+= System.currentTimeMillis() - opsstart; 
              }else{
                rc[ti]++;
                String d = db.get("mykey"+k);
                if(!d.equals(data+k))
                  diff++;
                rct[ti]+= System.currentTimeMillis() - opsstart; 
              }
            }
            long delta = System.currentTimeMillis()-s;
            // System.out.println(String.format(rcount +" get in "+delta+
            // "ms -> %s tps, diff=%s",rcount*1000l/delta,diff));
            if(delta == 0) delta =1 ;
            tc[ti] =(int) (rcount*1000l/delta);
          }
        });
        ta[t].start();
      }
      for(int t = 0 ; t< tcount; t++){
        ta[t].join();
      }
      int totaltps = 0;
      int totalw = 0;
      int totalr = 0;
      long totallat = 0l;
      for(int t = 0 ; t< tcount; t++){
        totaltps+=tc[t];
        totalw+=wc[t];
        totalr+=rc[t];
        totallat+= wct[t]+rct[t];
      }
      cumulw+=totalw;
      long idelta = System.currentTimeMillis()-istart;
      System.out.println(String.format(
        "Iteration %3d in %8d ms => tps=%6d,write=%6.2fMB/s, cumul=%6.2fMB,"+
        " readThread[%s tps,%f ms], avglatency : %f",
        ii,idelta,totaltps,
        (((double)totalw) * datasize / 1024 / 1024),//write
        (((double)cumulw) * datasize / 1024 / 1024),//cumul
        rc[0]*1000l/idelta, // readtps
        ((double)rct[0])/rc[0], // avg latency
        ((double)totallat)/(totalw+totalr) // total avg latency
        ));
      if(options.has("dbstats") && totaltps < 1500)
        System.out.println(db.getProperty("leveldb.stats"));
    }

    db.close();
    return;
  }


}