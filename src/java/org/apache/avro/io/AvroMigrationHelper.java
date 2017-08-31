package org.apache.avro.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;


/**
 * this is a helper class thats intended to work with either avro 1.4 or
 * a more modern version (1.7) on the classpath. this is meant to be used
 * for a short period of time to enable seamless migration out of avro 1.4
 * @deprecated after avro migration completes please stop using this class
 * in favor of avro 1.7. see individual method docs for details.
 */
@Deprecated
public class AvroMigrationHelper {
    private static final boolean IS_AVRO_14;
    private static final AvroFactory FACTORY;

    static {
        boolean avro14;
        AvroFactory toUse;
        try {
            toUse = new ModernAvroFactory();
            avro14 = false;
        } catch (Exception e) {
            //nope, try 1.4
            try {
                toUse = new Avro14Factory(); //will throw if fails
                avro14 = true;
            } catch (Exception e2) {
                IllegalStateException exception = new IllegalStateException("could not initialize any avro factory");
                exception.addSuppressed(e);
                exception.addSuppressed(e2);
                throw exception;
            }
        }
        IS_AVRO_14 = avro14;
        FACTORY = toUse;
    }

    private AvroMigrationHelper() {
        //this is a util class. dont build one yourself
    }

    public static BinaryEncoder newBinaryEncoder(OutputStream out) {
        return FACTORY.newBinaryEncoder(out);
    }

    public static JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException {
        return FACTORY.newJsonEncoder(schema, out);
    }

    public static JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException {
        return FACTORY.newJsonDecoder(schema, input);
    }

    public static JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException {
        return FACTORY.newJsonDecoder(schema, input);
    }

    public static GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
        return FACTORY.newEnumSymbol(avroSchema, enumValue);
    }

    public static GenericData.Fixed newFixedField(Schema ofType) {
        return FACTORY.newFixedField(ofType);
    }

    public static GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
        return FACTORY.newFixedField(ofType, contents);
    }

    /**
     * generated java code (specific classes) for a given set of Schemas. this method fails if any failure occurs
     * during code generation
     * @param toCompile set of schema objects
     * @param avro14Compatible if true will generate java code that will successfully compile and run under avro 1.4
     * @return a collection of generated java file descriptors, each with a relative path and proposed contents
     */
    public static Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, boolean avro14Compatible) {
        return FACTORY.compile(toCompile, avro14Compatible);
    }

    public static boolean isAvro14() {
        return IS_AVRO_14;
    }
}