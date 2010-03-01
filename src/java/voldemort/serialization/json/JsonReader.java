/*
 * Copyright 2008-2009 LinkedIn, Inc
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

package voldemort.serialization.json;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import voldemort.serialization.SerializationException;

/**
 * Read in JSON objects from a java.io.Reader
 * 
 * 
 */
public class JsonReader {

    // The java.io.Reader to use to get characters
    private final Reader reader;

    // the character to use to count line breaks
    private final char lineBreak;

    // the current line number
    private int line;

    // the index into the current line
    private int lineOffset;

    // the number of characters read
    private int charsRead;

    // the current character
    private int current;

    // a circular buffer of recent characters for error message context
    private final char[] contextBuffer;

    // the offset into the contextBuffer where we are at currently.
    private int contextOffset;

    public JsonReader(Reader reader) {
        this(reader, 50);
    }

    public JsonReader(Reader reader, int contextBufferSize) {
        this.reader = reader;
        String newline = System.getProperty("line.separator");
        if(newline.contains("\n"))
            lineBreak = '\n';
        else
            lineBreak = '\r';
        this.line = 1;
        this.lineOffset = 0;
        this.charsRead = 0;
        // initialize contextBuffer to all whitespace
        this.contextBuffer = new char[contextBufferSize];
        for(int i = 0; i < contextBufferSize; i++)
            this.contextBuffer[i] = ' ';
        next();
    }

    public boolean hasMore() {
        return current() != -1;
    }

    public Object read() {
        skipWhitespace();
        Object o = null;
        switch(current()) {
            case '{':
                o = readObject();
                break;
            case '[':
                o = readArray();
                break;
            case '"':
            case '\'':
                o = readString();
                break;
            case 't':
            case 'f':
                o = readBoolean();
                break;
            case 'n':
                o = readNull();
                break;
            case -1:
                throw new EndOfFileException();
            default:
                if(Character.isDigit(current()) || current() == '-') {
                    o = readNumber();
                } else {
                    throw new SerializationException("Unacceptable initial character "
                                                     + currentChar()
                                                     + " found when parsing object at line "
                                                     + getCurrentLineNumber() + " character "
                                                     + getCurrentLineOffset());
                }
        }
        skipWhitespace();
        return o;
    }

    public Map<String, ?> readObject() {
        skip('{');
        skipWhitespace();
        Map<String, Object> values = new HashMap<String, Object>();
        while(current() != '}') {
            skipWhitespace();
            String key = readString();
            skipWhitespace();
            skip(':');
            skipWhitespace();
            Object value = read();
            values.put(key, value);
            skipWhitespace();
            if(current() == ',') {
                next();
                skipWhitespace();
            } else if(current() == '}') {
                break;
            } else {
                throw new SerializationException("Unexpected character '"
                                                 + currentChar()
                                                 + "' in object definition, expected '}' or ',' but found: "
                                                 + getCurrentContext());
            }
        }
        skip('}');
        return values;
    }

    public List<?> readArray() {
        skip('[');
        skipWhitespace();
        List<Object> l = new ArrayList<Object>();
        while(current() != ']' && hasMore()) {
            l.add(read());
            skipWhitespace();
            if(current() == ',') {
                next();
                skipWhitespace();
            }
        }
        skip(']');
        return l;
    }

    public Object readNull() {
        skip("null");
        return null;
    }

    public Boolean readBoolean() {
        if(current() == 't') {
            skip("true");
            return Boolean.TRUE;
        } else {
            skip("false");
            return Boolean.FALSE;
        }
    }

    public String readString() {
        int quote = current();
        StringBuilder buffer = new StringBuilder();
        next();
        while(current() != quote && hasMore()) {
            if(current() == '\\')
                appendControlSequence(buffer);
            else
                buffer.append(currentChar());
            next();
        }
        skip(quote);
        return buffer.toString();
    }

    private void appendControlSequence(StringBuilder buffer) {
        skip('\\');
        switch(current()) {
            case '"':
            case '\\':
            case '/':
                buffer.append(currentChar());
                break;
            case 'b':
                buffer.append('\b');
                break;
            case 'f':
                buffer.append('\f');
                break;
            case 'n':
                buffer.append('\n');
                break;
            case 'r':
                buffer.append('\r');
                break;
            case 't':
                buffer.append('\t');
                break;
            case 'u':
                buffer.append(readUnicodeLiteral());
                break;
            default:
                throw new SerializationException("Unrecognized control sequence on line "
                                                 + getCurrentLineNumber() + ": '\\" + currentChar()
                                                 + "'.");
        }
    }

    private char readUnicodeLiteral() {
        int value = 0;
        for(int i = 0; i < 4; i++) {
            next();
            value <<= 4;
            if(Character.isDigit(current()))
                value += current() - '0';
            else if('a' <= current() && 'f' >= current())
                value += 10 + current() - 'a';
            else if('A' <= current() && 'F' >= current())
                value += 10 + current() - 'A';
            else
                throw new SerializationException("Invalid character in unicode sequence on line "
                                                 + getCurrentLineNumber() + ": " + currentChar());
        }

        return (char) value;
    }

    public Number readNumber() {
        skipWhitespace();
        int intPiece = readInt();

        // if int is all we have, return it
        if(isTerminator(current()))
            return intPiece;

        // okay its a double, check for exponent
        double doublePiece = intPiece;
        if(current() == '.')
            doublePiece += readFraction();
        if(current() == 'e' || current() == 'E') {
            next();
            skipIf('+');
            int frac = readInt();
            doublePiece *= Math.pow(10, frac);
        }
        if(isTerminator(current()))
            return doublePiece;
        else
            throw new SerializationException("Invalid number format for number on line "
                                             + lineOffset + ": " + getCurrentContext());
    }

    private boolean isTerminator(int ch) {
        return Character.isWhitespace(ch) || ch == '{' || ch == '}' || ch == '[' || ch == ']'
               || ch == ',' || ch == -1;
    }

    public int readInt() {
        skipWhitespace();
        int val = 0;
        boolean isPositive;
        if(current() == '-') {
            isPositive = false;
            next();
        } else if(current() == '+') {
            isPositive = true;
            next();
        } else {
            isPositive = true;
        }
        skipWhitespace();
        if(!Character.isDigit(current()))
            throw new SerializationException("Expected a digit while trying to parse number, but got '"
                                             + currentChar()
                                             + "' at line "
                                             + getCurrentLineNumber()
                                             + " character "
                                             + getCurrentLineOffset() + ": " + getCurrentContext());
        while(Character.isDigit(current())) {
            val *= 10;
            val += (current() - '0');
            next();
        }
        if(!isPositive)
            val = -val;
        return val;
    }

    public double readFraction() {
        skip('.');
        double position = 0.1;
        double val = 0;
        while(Character.isDigit(current())) {
            val += position * (current() - '0');
            position *= 0.1;
            next();
        }
        return val;
    }

    private int current() {
        return this.current;
    }

    private char currentChar() {
        return (char) this.current;
    }

    private int next() {
        try {
            // read a character
            this.current = this.reader.read();

            // increment the character count and maybe line number
            this.charsRead++;
            this.lineOffset++;
            if(this.current == this.lineBreak) {
                this.line++;
                this.lineOffset = 1;
            }
            // add to context buffer
            this.contextBuffer[this.contextOffset] = (char) this.current;
            this.contextOffset = (contextOffset + 1) % this.contextBuffer.length;

            return this.current;
        } catch(IOException e) {
            throw new SerializationException("Error reading from JSON stream.", e);
        }
    }

    private void skipIf(char c) {
        if(current() == c)
            next();
    }

    private void skip(String s) {
        for(int i = 0; i < s.length(); i++)
            skip(s.charAt(i));
    }

    private void skipWhitespace() {
        while(Character.isWhitespace(current()))
            next();
    }

    private void skip(int c) {
        if(current() != c)
            throw new SerializationException("Expected '" + ((char) c)
                                             + "' but current character is '" + currentChar()
                                             + "' on line " + line + " character " + lineOffset
                                             + ": " + getCurrentContext());
        next();
    }

    public int getCurrentLineNumber() {
        return this.line;
    }

    public int getCurrentLineOffset() {
        return this.lineOffset;
    }

    public String getCurrentContext() {
        StringBuilder builder = new StringBuilder(this.contextBuffer.length);
        for(int i = this.contextOffset; i < this.contextBuffer.length; i++)
            builder.append(this.contextBuffer[i]);
        for(int i = 0; i < contextOffset; i++)
            builder.append(this.contextBuffer[i]);
        if(this.charsRead < this.contextBuffer.length)
            return builder.toString().substring(this.contextBuffer.length - this.charsRead,
                                                this.contextBuffer.length);
        else
            return builder.toString();
    }
}
