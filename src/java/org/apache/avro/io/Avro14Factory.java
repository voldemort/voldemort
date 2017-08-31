package org.apache.avro.io;

import java.io.OutputStream;
import java.lang.reflect.Constructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

//package private ON PURPOSE
class Avro14Factory extends AbstractAvroFactory  {
    private final Constructor _binaryEncoderCtr;

    Avro14Factory() throws Exception {
        super(
                GenericData.EnumSymbol.class.getConstructor(String.class),
                GenericData.Fixed.class.getConstructor(byte[].class)
        );
        _binaryEncoderCtr = BinaryEncoder.class.getConstructor(OutputStream.class);
        Class<?> compilerClass = Class.forName("org.apache.avro.specific.SpecificCompiler");
        _specificCompilerCtr = compilerClass.getConstructor(Schema.class);
        _compilerEnqueueMethod = compilerClass.getDeclaredMethod("enqueue", Schema.class);
        _compilerEnqueueMethod.setAccessible(true); //its normally private
        _compilerCompileMethod = compilerClass.getDeclaredMethod("compile");
        _compilerCompileMethod.setAccessible(true); //package-protected
        Class<?> outputFileClass = Class.forName("org.apache.avro.specific.SpecificCompiler$OutputFile");
        _outputFilePathField = outputFileClass.getDeclaredField("path");
        _outputFilePathField.setAccessible(true);
        _outputFileContentsField = outputFileClass.getDeclaredField("contents");
        _outputFileContentsField.setAccessible(true);
    }

    @Override
    public BinaryEncoder newBinaryEncoder(OutputStream out) {
        try {
            return (BinaryEncoder) _binaryEncoderCtr.newInstance(out);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
        try {
            return (GenericData.EnumSymbol) _enumSymbolCtr.newInstance(enumValue);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
        try {
            return (GenericData.Fixed) _fixedCtr.newInstance((Object) contents);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}