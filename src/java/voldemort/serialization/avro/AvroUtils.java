package voldemort.serialization.avro;

import java.io.IOException;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.log4j.Logger;

final class AvroUtils {

    private static final Logger logger = Logger.getLogger(AvroUtils.class);

    private AvroUtils() {}

    static void close(DataFileStream<?> stream) {
        if(stream != null) {
            try {
                stream.close();
            } catch(IOException e) {
                logger.error("Failed to close stream", e);
            }
        }
    }

    static void close(DataFileWriter<?> stream) {
        if(stream != null) {
            try {
                stream.close();
            } catch(IOException e) {
                logger.error("Failed to close stream", e);
            }
        }
    }
}
