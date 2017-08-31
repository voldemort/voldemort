package org.apache.avro.io;

import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

//package private ON PURPOSE
class ModernAvroFactory extends AbstractAvroFactory  {
    private static final String PARSER_INVOCATION_START = "new org.apache.avro.Schema.Parser().parse(";
    private static final Pattern PARSER_INVOCATION_PATTERN = Pattern.compile(Pattern.quote(PARSER_INVOCATION_START) + "\"(.*)\"\\);([\r\n]+)");
    private static final String CSV_SEPARATOR = "(?<!\\\\)\",\""; //require the 1st " not be preceded by a \
    private static final Pattern BUILDER_START_PATTERN = Pattern.compile("/\\*\\* Creates a new \\w+ RecordBuilder \\*/");

    private final Object _encoderFactory;
    private final Method _encoderFactoryBinaryEncoderMethod;

    //compiler-related
    private boolean _compilerSupported; //defaults to false
    private Object _publicFieldVisibilityEnumInstance;
    private Method _setFieldVisibilityMethod;

    ModernAvroFactory() throws Exception {
        super(
                GenericData.EnumSymbol.class.getConstructor(Schema.class, String.class),
                GenericData.Fixed.class.getConstructor(Schema.class, byte[].class)
        );
        _encoderFactory = Class.forName("org.apache.avro.io.EncoderFactory").getDeclaredMethod("get").invoke(null);
        _encoderFactoryBinaryEncoderMethod = _encoderFactory.getClass().getMethod("binaryEncoder", OutputStream.class, BinaryEncoder.class);
        tryInitializeCompilerFields();
    }

    private void tryInitializeCompilerFields() throws Exception {
        //compiler was moved out into a separate jar in modern avro, so compiler functionality is optional
        try {
            Class<?> compilerClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler");
            _specificCompilerCtr = compilerClass.getConstructor(Schema.class);
            _compilerEnqueueMethod = compilerClass.getDeclaredMethod("enqueue", Schema.class);
            _compilerEnqueueMethod.setAccessible(true); //its normally private
            _compilerCompileMethod = compilerClass.getDeclaredMethod("compile");
            _compilerCompileMethod.setAccessible(true); //package-protected
            Class<?> outputFileClass = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler$OutputFile");
            _outputFilePathField = outputFileClass.getDeclaredField("path");
            _outputFilePathField.setAccessible(true);
            _outputFileContentsField = outputFileClass.getDeclaredField("contents");
            _outputFileContentsField.setAccessible(true);
            Class<?> fieldVisibilityEnum = Class.forName("org.apache.avro.compiler.specific.SpecificCompiler$FieldVisibility");
            Field publicVisibilityField = fieldVisibilityEnum.getDeclaredField("PUBLIC");
            _publicFieldVisibilityEnumInstance = publicVisibilityField.get(null);
            _setFieldVisibilityMethod = compilerClass.getDeclaredMethod("setFieldVisibility", fieldVisibilityEnum);
            _compilerSupported = true;
        } catch (Exception e) {
            _compilerSupported = false;
            //ignore
        }
    }

    @Override
    public BinaryEncoder newBinaryEncoder(OutputStream out) {
        try {
            return (BinaryEncoder) _encoderFactoryBinaryEncoderMethod.invoke(_encoderFactory, out, null);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public GenericData.EnumSymbol newEnumSymbol(Schema avroSchema, String enumValue) {
        try {
            return (GenericData.EnumSymbol) _enumSymbolCtr.newInstance(avroSchema, enumValue);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public GenericData.Fixed newFixedField(Schema ofType, byte[] contents) {
        try {
            return (GenericData.Fixed) _fixedCtr.newInstance(ofType, contents);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, boolean avro14Compatible) {
        if (!_compilerSupported) {
            throw new UnsupportedOperationException("avro compiler jar was not found on classpath (was made an independent jar in modern avro)");
        }
        return super.compile(toCompile, avro14Compatible);
    }

    @Override
    protected Object getCompilerInstance(Schema firstSchema) throws Exception {
        Object instance = super.getCompilerInstance(firstSchema);
        //configure compiler to be as avro-1.4 compliant as possible within the avro compiler API
        _setFieldVisibilityMethod.invoke(instance, _publicFieldVisibilityEnumInstance); //make fields public
        return instance;
    }

    @Override
    protected List<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroCodegenOutput, boolean avro14Compatible) {
        //maybe make avro-1.4 compatible, always fix known avro bugs
        return avroCodegenOutput.stream()
                .map(file -> avro14Compatible ? new AvroGeneratedSourceCode(file.getPath(), make14Compatible(file.getContents())) : file)
                .map(file -> new AvroGeneratedSourceCode(file.getPath(), fixKnownIssues(file.getContents())))
                .collect(Collectors.toList());
    }

    /**
     * given a java class generated by modern avro, make the code compatible with an avro 1.4 runtime.
     * @param avroGenerated a class generated by modern avro
     * @return avro-1.4 compatible version of the input class
     */
    private static String make14Compatible(String avroGenerated) {
        String result = avroGenerated;

        //comment out annotation that only exists in avro 1.7
        result = result.replace("@org.apache.avro.specific.AvroGenerated", "// @org.apache.avro.specific.AvroGenerated");

        //issues we have with avro's generated SCHEMA$ field:
        //1. modern avro uses Schema.Parser().parse() whereas 1.4 doesnt have it. 1.7 still has Schema.parse() (just deprecated)
        //   so we resort to using that for compatibility
        //2. the java language has a limitation where string constants cannot be over 64K long (this is way less than 64k
        //   characters for complicated unicode) - see AVRO-1316. modern avro has a vararg parse(String...) to get around this
        //   we will need to convert that into new StringBuilder().append().append()...toString()
        //group 1 would be the args to parse(), group 2 would be some line break
        Matcher matcher = PARSER_INVOCATION_PATTERN.matcher(result);
        if (matcher.find()) {
            String argsStr = result.substring(matcher.start(1), matcher.end(1));
            String lineBreak = matcher.group(2);
            String[] varArgs = argsStr.split(CSV_SEPARATOR);
            String singleArg;
            if (varArgs.length > 1) {
                StringBuilder argBuilder = new StringBuilder(argsStr.length());
                argBuilder.append("new StringBuilder()");
                Arrays.stream(varArgs).forEach(literal -> {
                    argBuilder.append(".append(\"").append(literal).append("\")");
                });
                argBuilder.append(".toString()");
                singleArg = argBuilder.toString();
            } else {
                singleArg = "\"" + varArgs[0] + "\"";
            }
            result = matcher.replaceFirst(Matcher.quoteReplacement("org.apache.avro.Schema.parse(" + singleArg + ");") + lineBreak);
        }

        //look for builder-related code (should be at the end of a file) and kill it with fire
        //(the builder base class does not exist in avro 1.4)
        matcher = BUILDER_START_PATTERN.matcher(result);
        if (matcher.find()) {
            result = result.substring(0, matcher.start()) + "\n}";
        }
        //classes for type fixed dont have a byte[]-accepting constructor in 1.4
        result = result.replaceAll("super\\(bytes\\);", "super();\n    bytes(bytes);");
        return result;
    }

    /**
     * given a java class generated by modern avro, fix known issues
     * @param avroGenerated java code generated by modern avro
     * @return a version of the input code, with any known issues fixed.
     */
    private static String fixKnownIssues(String avroGenerated) {
        //because who in their right minds would call their class Exception, right? :-(
        //also, this bug has been fixed in upstream avro, but there's been no 1.7.* release with the fix yet
        //see AVRO-1901
        return avroGenerated.replaceAll("catch (Exception e)", "catch (java.lang.Exception e)");
    }
}