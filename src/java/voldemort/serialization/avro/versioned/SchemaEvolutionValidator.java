/*
 * Copyright 2011 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.serialization.avro.versioned;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.log4j.Level;
import org.codehaus.jackson.JsonNode;

import voldemort.VoldemortException;
import voldemort.serialization.SerializerDefinition;

/**
 * Provides methods to compare schemas for schema evolution and indicate any
 * potential problems.
 * 
 * @author Jemiah Westerman <jwesterman@linkedin.com>
 * 
 * @version $Revision$
 */
public class SchemaEvolutionValidator {

    private static final Schema NULL_TYPE_SCHEMA = Schema.create(Schema.Type.NULL);
    private final List<String> _recordStack = new ArrayList<String>();

    /**
     * This main method provides an easy command line tool to compare two
     * schemas.
     */
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println("Usage: SchemaEvolutionValidator pathToOldSchema pathToNewSchema");
            return;
        }

        Schema oldSchema;
        Schema newSchema;

        try {
            oldSchema = Schema.parse(new File(args[0]));
        } catch(Exception ex) {
            oldSchema = null;
            System.out.println("Could not open or parse the old schema (" + args[0] + ") due to "
                               + ex);
        }

        try {
            newSchema = Schema.parse(new File(args[1]));
        } catch(Exception ex) {
            newSchema = null;
            System.out.println("Could not open or parse the new schema (" + args[1] + ") due to "
                               + ex);
        }

        if(oldSchema == null || newSchema == null) {
            return;
        }

        System.out.println("Comparing: ");
        System.out.println("\t" + args[0]);
        System.out.println("\t" + args[1]);

        List<Message> messages = SchemaEvolutionValidator.checkBackwardCompatability(oldSchema,
                                                                                     newSchema,
                                                                                     oldSchema.getName());
        Level maxLevel = Level.ALL;
        for(Message message: messages) {
            System.out.println(message.getLevel() + ": " + message.getMessage());
            if(message.getLevel().isGreaterOrEqual(maxLevel)) {
                maxLevel = message.getLevel();
            }
        }

        if(maxLevel.isGreaterOrEqual(Level.ERROR)) {
            System.out.println(Level.ERROR
                               + ": The schema is not backward compatible. New clients will not be able to read existing data.");
        } else if(maxLevel.isGreaterOrEqual(Level.WARN)) {
            System.out.println(Level.WARN
                               + ": The schema is partially backward compatible, but old clients will not be able to read data serialized in the new format.");
        } else {
            System.out.println(Level.INFO
                               + ": The schema is backward compatible. Old and new clients will be able to read records serialized by one another.");
        }
    }

    /**
     * Compare two schemas to see if they are backward compatible. Returns a
     * list of validation messages. <li>ERROR messages indicate the schemas are
     * not backward compatible and the new schema should not be allowed. If an
     * ERROR schema is uploaded, clients will not be able to read existing data.
     * <li>WARN messages indicate that the new schemas may cause problems for
     * existing clients. However, once all clients are updated to this version
     * of the schema they should be able to read new and existing data. <li>INFO
     * messages note changes to the schema, basically providing a friendly list
     * of what has changed from one version to the next. This includes changes
     * like the addition of fields, changes to default values, etc.
     * 
     * @param oldSchema the original schema
     * @param newSchema the new schema
     * @param name the schema name
     * @return list of messages about the schema evolution
     */
    public static List<Message> checkBackwardCompatability(Schema oldSchema,
                                                           Schema newSchema,
                                                           String name) {
        SchemaEvolutionValidator validator = new SchemaEvolutionValidator();
        List<Message> messages = new ArrayList<Message>();
        validator.compareTypes(oldSchema, newSchema, messages, name);
        return messages;
    }

    /* package private */void compareTypes(Schema oldSchema,
                                           Schema newSchema,
                                           List<Message> messages,
                                           String name) {
        oldSchema = stripOptionalTypeUnion(oldSchema);
        newSchema = stripOptionalTypeUnion(newSchema);
        Schema.Type oldType = oldSchema.getType();
        Schema.Type newType = newSchema.getType();

        if(oldType != Type.UNION && newType == Type.UNION) {
            boolean compatibleTypeFound = false;
            for(Schema s: newSchema.getTypes()) {
                if((oldType != Type.RECORD && oldType == s.getType())
                   || (oldType == Type.RECORD && s.getType() == Type.RECORD && oldSchema.getName()
                                                                                        .equals(s.getName()))) {
                    compareTypes(oldSchema, s, messages, name);
                    compatibleTypeFound = true;
                    break;
                }
            }

            if(compatibleTypeFound) {
                messages.add(new Message(Level.INFO,
                                         "Type change from " + oldType + " to " + newType
                                                 + " for field " + name
                                                 + ". The new union includes the original type."));
            } else {
                messages.add(new Message(Level.ERROR,
                                         "Incompatible type change from "
                                                 + oldType
                                                 + " to "
                                                 + newType
                                                 + " for field "
                                                 + name
                                                 + ". The new union does not include the original type."));
            }
        } else if(oldType == Type.RECORD) {
            if(!_recordStack.contains(oldSchema.getName())) {
                _recordStack.add(oldSchema.getName());
                compareRecordTypes(oldSchema, newSchema, messages, name);
                _recordStack.remove(oldSchema.getName());
            }
        } else if(oldType == Type.ENUM) {
            compareEnumTypes(oldSchema, newSchema, messages, name);
        } else if(oldType == Type.ARRAY) {
            compareArrayTypes(oldSchema, newSchema, messages, name);
        } else if(oldType == Type.MAP) {
            compareMapTypes(oldSchema, newSchema, messages, name);
        } else if(oldType == Type.UNION) {
            compareUnionTypes(oldSchema, newSchema, messages, name);
        } else if(oldType == Type.FIXED) {
            compareFixedTypes(oldSchema, newSchema, messages, name);
        } else {
            comparePrimitiveTypes(oldSchema, newSchema, messages, name);
        }
    }

    /* package private */void compareRecordTypes(Schema oldSchema,
                                                 Schema newSchema,
                                                 List<Message> messages,
                                                 String name) {
        if(oldSchema == null || newSchema == null || oldSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Old schema must be RECORD type. Name=" + name
                                               + ". Type=" + oldSchema);
        }

        if(newSchema.getType() != Schema.Type.RECORD) {
            messages.add(new Message(Level.ERROR, "Illegal type change from " + oldSchema.getType()
                                                  + " to " + newSchema.getType() + " for field "
                                                  + name));
            return;
        }

        // Check all of the fields in the new schema
        for(Field newField: newSchema.getFields()) {
            String fieldName = newField.name();
            Field oldField = oldSchema.getField(fieldName);

            if(oldField == null) {
                // This is a new field that did not exist in the original
                // schema.
                // Check if it is optional or has a default value.
                if(isOptional(newField)) {
                    if(newField.defaultValue() == null) {
                        messages.add(new Message(Level.INFO, "Added optional field " + name + "."
                                                             + fieldName
                                                             + " with no default value."));
                    } else {
                        messages.add(new Message(Level.INFO, "Added optional field " + name + "."
                                                             + fieldName + " with default value: "
                                                             + newField.defaultValue()));
                    }
                } else {
                    if(newField.defaultValue() == null) {
                        messages.add(new Message(Level.ERROR, "Added required field " + name + "."
                                                              + fieldName
                                                              + " with no default value."));
                    } else {
                        messages.add(new Message(Level.INFO, "Added required field " + name + "."
                                                             + fieldName + " with default value: "
                                                             + newField.defaultValue()));
                    }
                }
            } else {
                // This is a field that existed in the original schema.

                // Check if the field was changed from optional to required or
                // vice versa.
                boolean newFieldIsOptional = isOptional(newField);
                boolean oldFieldIsOptional = isOptional(oldField);

                if(oldFieldIsOptional != newFieldIsOptional) {
                    if(oldFieldIsOptional) {
                        messages.add(new Message(Level.ERROR,
                                                 "Existing field " + name + "." + fieldName
                                                         + " was optional and is now required."));
                    } else {
                        messages.add(new Message(Level.WARN, "Existing field " + name + "."
                                                             + fieldName
                                                             + " was required and is now optional."));
                    }
                }

                // Recursively compare the nested field types
                compareTypes(oldField.schema(), newField.schema(), messages, name + "." + fieldName);

                // Check if the default value has been changed
                if(newField.defaultValue() == null) {
                    if(oldField.defaultValue() != null) {
                        messages.add(new Message(Level.WARN,
                                                 "Removed default value for existing field " + name
                                                         + "." + fieldName
                                                         + ". The old default was: "
                                                         + oldField.defaultValue()));
                    }
                } else // newField.defaultValue() != null
                {
                    if(oldField.defaultValue() == null) {
                        messages.add(new Message(Level.WARN,
                                                 "Added a default value for existing field " + name
                                                         + "." + fieldName
                                                         + ". The new default is: "
                                                         + newField.defaultValue()));
                    } else if(!newField.defaultValue().equals(oldField.defaultValue())) {
                        messages.add(new Message(Level.INFO,
                                                 "Changed the default value for existing field "
                                                         + name + "." + fieldName
                                                         + ". The old default was: "
                                                         + oldField.defaultValue()
                                                         + ". The new default is: "
                                                         + newField.defaultValue()));
                    }
                }
            }

            // For all fields in the new schema (whether or not it existed in
            // the old schema), if there is a default value for this field, make
            // sure it is legal.
            if(newField.defaultValue() != null) {
                checkDefaultValueIsLegal(newField, messages, name + "." + fieldName);
            }
        }

        // Check if any fields were removed.
        for(Field oldField: newSchema.getFields()) {
            String fieldName = oldField.name();
            Field newField = newSchema.getField(fieldName);

            if(newField == null) {
                if(isOptional(oldField)) {
                    messages.add(new Message(Level.INFO, "Removed optional field " + name + "."
                                                         + fieldName));
                } else {
                    messages.add(new Message(Level.WARN, "Removed required field " + name + "."
                                                         + fieldName));
                }
            }
        }

        // Check if indexing was modified or added to any old fields.
        for(Field oldField: oldSchema.getFields()) {
            if(newSchema.getField(oldField.name()) != null) {
                String oldIndexType = oldField.getProp("indexType");
                String newIndexType = newSchema.getField(oldField.name()).getProp("indexType");

                // Check if added indexing.
                if(oldIndexType == null && newIndexType != null) {
                    messages.add(new Message(Level.ERROR,
                                             "Cannot add indexing to "
                                                     + oldField.name()
                                                     + ". Adding indexing to fields created without indexing is not supported."));
                }

                // Check if changed indexing
                if(oldIndexType != null && !oldIndexType.equals(newIndexType)) {
                    messages.add(new Message(Level.ERROR,
                                             "Cannot change indexing from "
                                                     + oldIndexType
                                                     + " to "
                                                     + newIndexType
                                                     + " for "
                                                     + oldField.name()
                                                     + ". Changing indexing method is not supported."));
                }
            }
        }
    }

    /* package private */void compareUnionTypes(Schema oldSchema,
                                                Schema newSchema,
                                                List<Message> messages,
                                                String name) {
        if(oldSchema == null || newSchema == null || oldSchema.getType() != Schema.Type.UNION) {
            throw new IllegalArgumentException("Old and new schema must both be UNION types. Name="
                                               + name + ". Type=" + oldSchema);
        }

        // Build a list of type names, ignoring nulls which are checked
        // separately as optional/required fields
        List<Schema> newTypes = new ArrayList<Schema>();
        List<String> newTypeNames = new ArrayList<String>();
        List<Schema> oldTypes = new ArrayList<Schema>();
        List<String> oldTypeNames = new ArrayList<String>();

        if(newSchema.getType() == Type.UNION) {
            for(Schema schema: newSchema.getTypes()) {
                if(schema.getType() != Schema.Type.NULL) {
                    newTypes.add(schema);
                    newTypeNames.add(schema.getName());
                }
            }
        } else {
            newTypes.add(newSchema);
            newTypeNames.add(newSchema.getName());
        }

        for(Schema schema: oldSchema.getTypes()) {
            if(schema.getType() != Schema.Type.NULL) {
                oldTypes.add(schema);
                oldTypeNames.add(schema.getName());
            }
        }

        if(!newTypeNames.containsAll(oldTypeNames)) {
            messages.add(new Message(Level.ERROR,
                                     "Existing UNION field " + name
                                             + " had one or more types removed. The old types are:"
                                             + oldTypeNames + ". The new types are: "
                                             + newTypeNames));
        }
        if(!oldTypeNames.containsAll(newTypeNames)) {
            messages.add(new Message(Level.INFO,
                                     "Existing UNION field " + name
                                             + " had one or more types added. The old types are:"
                                             + oldTypeNames + ". The new types are: "
                                             + newTypeNames));
        }
        if(newTypeNames.containsAll(oldTypeNames) && oldTypeNames.containsAll(newTypeNames)
           && !newTypeNames.equals(oldTypeNames)) {
            messages.add(new Message(Level.INFO,
                                     "Existing UNION field "
                                             + name
                                             + " had one or more types reordered. The old types are:"
                                             + oldTypeNames + ". The new types are: "
                                             + newTypeNames));
        }

        for(int i = 0; i < newTypeNames.size(); i++) {
            String typeName = newTypeNames.get(i);
            int oldIndex = oldTypeNames.indexOf(typeName);
            if(oldIndex != -1) {
                compareTypes(oldTypes.get(oldIndex),
                             newTypes.get(i),
                             messages,
                             name + "." + oldTypes.get(oldIndex).getName());
            }
        }
    }

    /* package private */void compareEnumTypes(Schema oldSchema,
                                               Schema newSchema,
                                               List<Message> messages,
                                               String name) {
        if(oldSchema == null || newSchema == null || oldSchema.getType() != Schema.Type.ENUM) {
            throw new IllegalArgumentException("Old schema must be ENUM type. Name=" + name
                                               + ". Type=" + oldSchema);
        }

        if(newSchema.getType() != Schema.Type.ENUM) {
            messages.add(new Message(Level.ERROR, "Illegal type change from " + oldSchema.getType()
                                                  + " to " + newSchema.getType() + " for field "
                                                  + name));
            return;
        }

        List<String> newEnumSymbols = newSchema.getEnumSymbols();
        List<String> oldEnumSymbols = oldSchema.getEnumSymbols();

        // Check if enum types were added or removed
        if(!newEnumSymbols.containsAll(oldEnumSymbols)) {
            messages.add(new Message(Level.ERROR,
                                     "Existing ENUM field "
                                             + name
                                             + " had one or more enum symbols removed. The old symbols are:"
                                             + oldEnumSymbols + ". The new symbols are: "
                                             + newEnumSymbols));
        }
        if(!oldEnumSymbols.containsAll(newEnumSymbols)) {
            messages.add(new Message(Level.INFO,
                                     "Existing ENUM field "
                                             + name
                                             + " had one or more enum symbols added. The old symbols are:"
                                             + oldEnumSymbols + ". The new symbols are: "
                                             + newEnumSymbols));
        }

        // Check if enum types were reordered.
        if(newEnumSymbols.containsAll(oldEnumSymbols)) {
            for(int i = 0; i < oldEnumSymbols.size(); i++) {
                if(!oldEnumSymbols.get(i).equals(newEnumSymbols.get(i))) {
                    messages.add(new Message(Level.WARN,
                                             "Existing ENUM field "
                                                     + name
                                                     + " had one or more enum symbols reordered. The old symbols are:"
                                                     + oldEnumSymbols + ". The new symbols are: "
                                                     + newEnumSymbols));
                    break;
                }
            }
        }
    }

    /* package private */void compareArrayTypes(Schema oldSchema,
                                                Schema newSchema,
                                                List<Message> messages,
                                                String name) {
        if(oldSchema == null || newSchema == null || oldSchema.getType() != Schema.Type.ARRAY) {
            throw new IllegalArgumentException("Old schema must be ARRAY type. Name=" + name
                                               + ". Type=" + oldSchema);
        }

        if(newSchema.getType() != Schema.Type.ARRAY) {
            messages.add(new Message(Level.ERROR, "Illegal type change from " + oldSchema.getType()
                                                  + " to " + newSchema.getType() + " for field "
                                                  + name));
            return;
        }

        // Compare the array element types
        compareTypes(oldSchema.getElementType(),
                     newSchema.getElementType(),
                     messages,
                     name + ".<array element>");
    }

    /* package private */void compareMapTypes(Schema oldSchema,
                                              Schema newSchema,
                                              List<Message> messages,
                                              String name) {
        if(oldSchema == null || newSchema == null || oldSchema.getType() != Schema.Type.MAP) {
            throw new IllegalArgumentException("Old schema must be MAP type. Name=" + name
                                               + ". Type=" + oldSchema);
        }

        if(newSchema.getType() != Schema.Type.MAP) {
            messages.add(new Message(Level.ERROR, "Illegal type change from " + oldSchema.getType()
                                                  + " to " + newSchema.getType() + " for field "
                                                  + name));
            return;
        }

        // Compare the array element types
        compareTypes(oldSchema.getValueType(),
                     newSchema.getValueType(),
                     messages,
                     name + ".<map element>");
    }

    /* package private */void compareFixedTypes(Schema oldSchema,
                                                Schema newSchema,
                                                List<Message> messages,
                                                String name) {
        if(oldSchema == null || newSchema == null || oldSchema.getType() != Schema.Type.FIXED) {
            throw new IllegalArgumentException("Old schema must be FIXED type. Name=" + name
                                               + ". Type=" + oldSchema);
        }

        if(newSchema.getType() != Schema.Type.FIXED) {
            messages.add(new Message(Level.ERROR, "Illegal type change from " + oldSchema.getType()
                                                  + " to " + newSchema.getType() + " for field "
                                                  + name));
            return;
        }

        if(newSchema.getFixedSize() != oldSchema.getFixedSize()) {
            messages.add(new Message(Level.ERROR, "Illegal size change for fixed type field "
                                                  + name));
        }
    }

    /* package private */void comparePrimitiveTypes(Schema oldSchema,
                                                    Schema newSchema,
                                                    List<Message> messages,
                                                    String name) {
        if(oldSchema == null || newSchema == null) {
            throw new IllegalArgumentException("Old schema must both be a primitive type. Name="
                                               + name + ". Type=" + oldSchema);
        }

        Schema.Type oldType = oldSchema.getType();
        Schema.Type newType = newSchema.getType();

        if(oldType != newType) {
            if(((oldType == Schema.Type.INT && (newType == Schema.Type.LONG
                                                || newType == Schema.Type.FLOAT || newType == Schema.Type.DOUBLE))
                || (oldType == Schema.Type.LONG && (newType == Schema.Type.FLOAT || newType == Schema.Type.DOUBLE)) || (oldType == Schema.Type.FLOAT && (newType == Schema.Type.DOUBLE)))) {
                messages.add(new Message(Level.INFO, "Type change from " + oldSchema.getType()
                                                     + " to " + newSchema.getType() + " for field "
                                                     + name));
            } else {
                messages.add(new Message(Level.ERROR, "Illegal type change from "
                                                      + oldSchema.getType() + " to "
                                                      + newSchema.getType() + " for field " + name));
            }
        }
    }

    /**
     * Returns true if this field is optional. Optional fields are represented
     * as a type union containing the null type.
     * 
     * @param field
     * @return
     */
    /* package private */boolean isOptional(Field field) {
        if(field.schema().getType() == Type.UNION) {
            for(Schema nestedType: field.schema().getTypes()) {
                if(nestedType.getType() == Type.NULL) {
                    return true;
                }
            }
        }
        return false;
    }

    /* package private */Schema stripOptionalTypeUnion(Schema schema) {
        if(schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2
           && schema.getTypes().contains(NULL_TYPE_SCHEMA)) {
            return schema.getTypes().get(0).equals(NULL_TYPE_SCHEMA) ? schema.getTypes().get(1)
                                                                    : schema.getTypes().get(0);
        }
        return schema;
    }

    public static class Message {

        private final Level _level;
        private final String _message;

        public Message(Level level, String message) {
            super();
            _level = level;
            _message = message;
        }

        public Level getLevel() {
            return _level;
        }

        public String getMessage() {
            return _message;
        }

        @Override
        public String toString() {
            return _level + ": " + _message;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((_level == null) ? 0 : _level.toString().hashCode());
            result = prime * result + ((_message == null) ? 0 : _message.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if(this == obj)
                return true;
            if(obj == null)
                return false;
            if(getClass() != obj.getClass())
                return false;
            Message other = (Message) obj;
            if(_level == null) {
                if(other._level != null)
                    return false;
            } else if(!_level.equals(other._level))
                return false;
            if(_message == null) {
                if(other._message != null)
                    return false;
            } else if(!_message.equals(other._message))
                return false;
            return true;
        }
    }

    /**
     * Makes sure the default value is good
     * 
     * @param parent
     * @param field
     */
    /* package private */static void checkDefaultValueIsLegal(Field field,
                                                              List<Message> messages,
                                                              String name) {
        if(field == null) {
            throw new IllegalArgumentException("Field must be non-null. Name=" + name);
        }

        if(field.defaultValue() != null) {
            // Get the type schema. If this is a UNION, the default must be of
            // the leading type
            Schema fieldSchema = field.schema();
            if(fieldSchema.getType() == Schema.Type.UNION) {
                fieldSchema = fieldSchema.getTypes().get(0);
            }

            // Get the default value
            JsonNode defaultJson = field.defaultValue();

            String expectedVal = checkDefaultJson(defaultJson, field.schema());

            if(expectedVal != null) {
                messages.add(new Message(Level.ERROR, "Illegal default value for field " + name
                                                      + ". The default must be of type "
                                                      + expectedVal + "."));
            }
        }
    }

    /**
     * Check that the default json node is a valid default value. If not, return
     * the expected type as a String.
     */
    /* package private */static String checkDefaultJson(JsonNode defaultJson, Schema schema) {
        Schema.Type fieldType = schema.getType();
        String expectedVal = null;
        switch(fieldType) {
            case NULL:
                if(!defaultJson.isNull()) {
                    expectedVal = "null";
                }

                break;
            case BOOLEAN:
                if(!defaultJson.isBoolean()) {
                    expectedVal = "boolean";
                }
                break;
            case INT:
                if(!defaultJson.isInt()) {
                    expectedVal = "int";
                }
                break;
            case LONG:
                if(!defaultJson.isInt() && !defaultJson.isLong()) {
                    expectedVal = "long";
                }
                break;
            case FLOAT:
            case DOUBLE:
                if(!defaultJson.isNumber()) {
                    expectedVal = "number";
                }
                break;
            case BYTES:
                if(defaultJson.isTextual()) {
                    break;
                }
                expectedVal = "bytes (ex. \"\\u00FF\")";
                break;
            case STRING:
                if(!defaultJson.isTextual()) {
                    expectedVal = "string";
                }
                break;
            case ENUM:
                if(defaultJson.isTextual()) {
                    if(schema.hasEnumSymbol(defaultJson.getTextValue())) {
                        break;
                    }
                }
                expectedVal = "valid enum";
                break;
            case FIXED:
                if(defaultJson.isTextual()) {
                    byte[] fixed = defaultJson.getValueAsText().getBytes();
                    if(fixed.length == schema.getFixedSize()) {
                        break;
                    }
                    expectedVal = "fixed size incorrect. Expected size: " + schema.getFixedSize()
                                  + " got size " + fixed.length;
                    break;
                }
                expectedVal = "fixed (ex. \"\\u00FF\")";
                break;
            case ARRAY:
                if(defaultJson.isArray()) {
                    // Check all array variables
                    boolean isGood = true;
                    for(JsonNode node: defaultJson) {
                        String val = checkDefaultJson(node, schema.getElementType());
                        if(val == null) {
                            continue;
                        } else {
                            isGood = false;
                            break;
                        }
                    }

                    if(isGood) {
                        break;
                    }
                }
                expectedVal = "array of type " + schema.getElementType().toString();
                break;
            case MAP:
                if(defaultJson.isObject()) {
                    boolean isGood = true;
                    for(JsonNode node: defaultJson) {
                        String val = checkDefaultJson(node, schema.getValueType());
                        if(val == null) {
                            continue;
                        } else {
                            isGood = false;
                            break;
                        }
                    }

                    if(isGood) {
                        break;
                    }
                }

                expectedVal = "map of type " + schema.getValueType().toString();
                break;
            case RECORD:
                if(defaultJson.isObject()) {
                    boolean isGood = true;
                    for(Field field: schema.getFields()) {
                        JsonNode jsonNode = defaultJson.get(field.name());

                        if(jsonNode == null) {
                            jsonNode = field.defaultValue();
                            if(jsonNode == null) {
                                isGood = false;
                                break;
                            }
                        }

                        String val = checkDefaultJson(jsonNode, field.schema());
                        if(val != null) {
                            isGood = false;
                            break;
                        }
                    }

                    if(isGood) {
                        break;
                    }
                }

                expectedVal = "record of type " + schema.toString();
                break;
            case UNION:
                // Avro spec states we only need to match with the first item
                expectedVal = checkDefaultJson(defaultJson, schema.getTypes().get(0));
                break;
        }

        return expectedVal;
    }

    public static void checkSchemaCompatibility(SerializerDefinition serDef) {

        Map<Integer, String> schemaVersions = serDef.getAllSchemaInfoVersions();

        Iterator schemaIterator = schemaVersions.entrySet().iterator();

        Schema firstSchema = null;
        Schema secondSchema = null;

        String firstSchemaStr;
        String secondSchemaStr;

        if(!schemaIterator.hasNext())
            throw new VoldemortException("No schema specified");

        Map.Entry schemaPair = (Map.Entry) schemaIterator.next();

        firstSchemaStr = (String) schemaPair.getValue();

        while(schemaIterator.hasNext()) {

            schemaPair = (Map.Entry) schemaIterator.next();

            secondSchemaStr = (String) schemaPair.getValue();
            Schema oldSchema = Schema.parse(firstSchemaStr);
            Schema newSchema = Schema.parse(secondSchemaStr);
            List<Message> messages = SchemaEvolutionValidator.checkBackwardCompatability(oldSchema,
                                                                                         newSchema,
                                                                                         oldSchema.getName());
            Level maxLevel = Level.ALL;
            for(Message message: messages) {
                System.out.println(message.getLevel() + ": " + message.getMessage());
                if(message.getLevel().isGreaterOrEqual(maxLevel)) {
                    maxLevel = message.getLevel();
                }
            }

            if(maxLevel.isGreaterOrEqual(Level.ERROR)) {
                System.out.println(Level.ERROR
                                   + ": The schema is not backward compatible. New clients will not be able to read existing data.");
                throw new VoldemortException(" The schema is not backward compatible. New clients will not be able to read existing data.");
            } else if(maxLevel.isGreaterOrEqual(Level.WARN)) {
                System.out.println(Level.WARN
                                   + ": The schema is partially backward compatible, but old clients will not be able to read data serialized in the new format.");
                throw new VoldemortException("The schema is partially backward compatible, but old clients will not be able to read data serialized in the new format.");
            } else {
                System.out.println(Level.INFO
                                   + ": The schema is backward compatible. Old and new clients will be able to read records serialized by one another.");
            }

            firstSchemaStr = secondSchemaStr;

        }
    }
}