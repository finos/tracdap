/*
 * Copyright 2022 Accenture Global Solutions Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.codec.json;

import org.finos.tracdap.common.exception.EUnexpected;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.async.ByteArrayFeeder;

import java.io.IOException;


class JsonStreamParser {

    enum ParseStateType {
        ROOT,
        OBJECT,
        ARRAY,
        FIELD
    }

    static class ParseState {

        ParseStateType stateType;
        String fieldName;
    }

    interface Handler {

        void handlePushArray(JsonParser lexer, ParseState state, ParseState parent, int depth) throws IOException;
        void handlePopArray(JsonParser lexer, ParseState state, ParseState parent, int depth) throws IOException;
        void handlePushObject(JsonParser lexer, ParseState state, ParseState parent, int depth) throws IOException;
        void handlePopObject(JsonParser lexer, ParseState state, ParseState parent, int depth) throws IOException;
        void handleFieldName(JsonParser lexer, ParseState state, ParseState parent, int depth) throws IOException;
        void handleFieldValue(JsonParser lexer, ParseState state, int depth) throws IOException;
        void handleArrayValue(JsonParser lexer, ParseState state, int depth) throws IOException;

        void close() throws IOException;
    }

    private static final int MAX_PARSE_DEPTH = 4;

    private final JsonParser lexer;
    private final ByteArrayFeeder feeder;
    private final Handler handler;

    private final ParseState[] parseStack;
    private int parseDepth;


    JsonStreamParser(JsonFactory factory, Handler handler) throws IOException {

        this.lexer = factory.createNonBlockingByteArrayParser();
        this.feeder = (ByteArrayFeeder) lexer.getNonBlockingInputFeeder();
        this.handler = handler;

        this.parseStack = new ParseState[MAX_PARSE_DEPTH];

        for (int i = 0; i < MAX_PARSE_DEPTH; i++)
            parseStack[i] = new ParseState();

        parseStack[0].stateType = ParseStateType.ROOT;
        parseDepth = 0;
    }

    public void close() throws IOException {

        lexer.close();
        handler.close();
    }

    public void feedInput(byte[] data, int offset, int end) throws IOException {

        feeder.feedInput(data, offset, end);
    }

    public JsonToken nextToken() throws IOException {

        return lexer.nextToken();
    }

    public void acceptToken(JsonToken token) throws IOException {

        var stateType = parseStack[parseDepth].stateType;

        switch (stateType) {

            case FIELD:
                acceptFieldValue(token);
                break;

            case OBJECT:
                acceptObjectContinue(token);
                break;

            case ARRAY:
                acceptArrayContinue(token);
                break;

            case ROOT:
                acceptRootToken(token);
                break;

            default:
                throw new EUnexpected();
        }
    }

    private void acceptRootToken(JsonToken token) throws IOException {

        switch (token) {

            case START_OBJECT:
                acceptStartObject();
                break;

            case START_ARRAY:
                acceptStartArray();
                break;

            default:

                var msg = String.format("Invalid JSON: Expected an object or array, got '%s'", token.name());
                throw new JsonParseException(lexer, msg, lexer.currentLocation());
        }
    }

    private void acceptStartArray() throws IOException {

        parseDepth++;
        parseStack[parseDepth].stateType = ParseStateType.ARRAY;

        handler.handlePushArray(lexer, parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);
    }

    private void acceptEndArray() throws IOException {

        handler.handlePopArray(lexer, parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);

        parseDepth--;

        if (parseStack[parseDepth].stateType == ParseStateType.FIELD)
            parseDepth--;
    }

    private void acceptArrayContinue(JsonToken token) throws IOException {

        var parseState = parseStack[parseDepth];

        if (token.isScalarValue()) {
            handler.handleArrayValue(lexer, parseState, parseDepth);
            return;
        }

        switch (token) {

            case START_OBJECT:
                acceptStartObject();
                break;

            case START_ARRAY:
                acceptStartArray();
                break;

            case END_ARRAY:
                acceptEndArray();
                break;

            default:

                var msg = String.format("Invalid JSON: Expected an array value, got '%s'", token.name());
                throw new JsonParseException(lexer, msg, lexer.currentLocation());
        }
    }

    private void acceptStartObject() throws IOException {

        parseDepth++;
        parseStack[parseDepth].stateType = ParseStateType.OBJECT;

        handler.handlePushObject(lexer, parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);
    }

    private void acceptEndObject() throws IOException {

        handler.handlePopObject(lexer, parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);

        parseDepth--;

        if (parseStack[parseDepth].stateType == ParseStateType.FIELD)
            parseDepth--;
    }

    private void acceptObjectContinue(JsonToken token) throws IOException {

        switch (token) {

            case FIELD_NAME:
                acceptFieldName();
                break;

            case END_OBJECT:
                acceptEndObject();
                break;

            default:

                var msg = String.format("Invalid JSON: Expected a field name, got '%s'", token.name());
                throw new JsonParseException(lexer, msg, lexer.currentLocation());
        }
    }

    private void acceptFieldName() throws IOException {

        parseDepth++;
        parseStack[parseDepth].stateType = ParseStateType.FIELD;
        parseStack[parseDepth].fieldName = lexer.currentName();

        handler.handleFieldName(lexer, parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);
    }

    private void acceptFieldValue(JsonToken token) throws IOException {

        var parseState = parseStack[parseDepth];

        if (token.isScalarValue()) {

            handler.handleFieldValue(lexer, parseState, parseDepth);
        }
        else { switch (token) {

            case START_OBJECT:
                acceptStartObject();
                break;

            case START_ARRAY:
                acceptStartArray();
                break;

            default:

                var msg = String.format("Invalid JSON: Expected a field value, got '%s'", token.name());
                throw new JsonParseException(lexer, msg, lexer.currentLocation());

        }}

        if (parseStack[parseDepth].stateType == ParseStateType.FIELD)
            parseDepth--;
    }
}
