package voldemort.store.compress;

import voldemort.serialization.Compression;

public class CompressionStrategyFactory {

    public CompressionStrategy get(Compression compression) {
        if(compression == null)
            return new NoopCompressionStrategy();
        if(compression.getType().equals("gzip"))
            return new GzipCompressionStrategy();
        throw new IllegalArgumentException("Unsupported compression algorithm: "
                                           + compression.getType());
    }
}
