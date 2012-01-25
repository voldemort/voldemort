package voldemort.store.compress;

import voldemort.serialization.Compression;

public class CompressionStrategyFactory {

    public CompressionStrategy get(Compression compression) {
        if(compression == null)
            return new NoopCompressionStrategy();
        if(compression.getType().equals("gzip"))
            return new GzipCompressionStrategy();
        if(compression.getType().equals("lzf"))
            return new LzfCompressionStrategy();
        if(compression.getType().equals("snappy"))
            return new SnappyCompressionStrategy();
        throw new IllegalArgumentException("Unsupported compression algorithm: "
                                           + compression.getType());
    }
}
