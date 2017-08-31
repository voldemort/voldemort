package org.apache.avro.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

//package-private ON PURPOSE
interface AvroFactory {

    BinaryEncoder newBinaryEncoder(OutputStream out);

    default JsonEncoder newJsonEncoder(Schema schema, OutputStream out) throws IOException {
        return new JsonEncoder(schema, out); //was made package-private in 1.7
    }

    default JsonDecoder newJsonDecoder(Schema schema, InputStream input) throws IOException {
        return new JsonDecoder(schema, input); //was made package-private in 1.7
    }

    default JsonDecoder newJsonDecoder(Schema schema, String input) throws IOException {
        return new JsonDecoder(schema, input); //was made package-private in 1.7
    }

    GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue);

    default GenericData.Fixed newFixedField(Schema ofType) {
        byte[] emptyDataArray = new byte[ofType.getFixedSize()]; //null is probably unsafe
        return newFixedField(ofType, emptyDataArray);
    }

    GenericData.Fixed newFixedField(Schema ofType, byte[] contents);

    Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, boolean avro14Compatible);
}