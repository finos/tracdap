/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.codec.json;

import com.accenture.trac.common.exception.EUnexpected;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;


abstract class JsonParserBase {

    private static final int MAX_PARSE_DEPTH = 4;

    private final JsonParser lexer;

    private final ParseState[] parseStack;
    private int parseDepth;

    abstract void handlePushArray(ParseState state, ParseState parent, int depth) throws IOException;
    abstract void handlePopArray(ParseState state, ParseState parent, int depth) throws IOException;
    abstract void handlePushObject(ParseState state, ParseState parent, int depth) throws IOException;
    abstract void handlePopObject(ParseState state, ParseState parent, int depth) throws IOException;
    abstract void handleFieldName(ParseState state, ParseState parent, int depth) throws IOException;
    abstract void handleFieldValue(ParseState state, int depth) throws IOException;
    abstract void handleArrayValue(ParseState state, int depth) throws IOException;

    enum ParseStateType {
        ROOT,
        OBJECT,
        ARRAY,
        FIELD
    }

    JsonParserBase(JsonParser lexer) {

        this.lexer = lexer;

        this.parseStack = new ParseState[MAX_PARSE_DEPTH];
        this.parseDepth = 0;
    }

    public void acceptToken(JsonToken token) throws IOException {

        var stateType = parseDepth > 0
                ? parseStack[parseDepth].stateType
                : ParseStateType.ROOT;

        switch (stateType) {

            case FIELD: acceptFieldValue(token); break;
            case OBJECT: acceptObjectContinue(token); break;
            case ARRAY: acceptArrayContinue(token); break;
            case ROOT: acceptRootToken(token); break;

            default:
                throw new EUnexpected();
        }
    }

    private void acceptRootToken(JsonToken token) throws IOException {

        switch (token) {

            case START_OBJECT: acceptStartObject();
            case START_ARRAY: acceptStartArray();

            default:
                throw new EUnexpected();  // todo: invalid json
        }
    }

    private void acceptStartArray() throws IOException {

        parseDepth++;
        parseStack[parseDepth].stateType = ParseStateType.ARRAY;

        handlePushArray(parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);
    }

    private void acceptEndArray() throws IOException {

        handlePopArray(parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);

        parseDepth--;

        if (parseStack[parseDepth].stateType == ParseStateType.FIELD)
            parseDepth--;
    }

    private void acceptArrayContinue(JsonToken token) throws IOException {

        var parseState = parseStack[parseDepth];

        if (token.isScalarValue()) {
            handleArrayValue(parseState, parseDepth);
            return;
        }

        switch (token) {
            case START_OBJECT: acceptStartObject(); break;
            case START_ARRAY: acceptStartArray(); break;
            case END_ARRAY: acceptEndArray();

            default:
                throw new EUnexpected(); // todo: invalid json
        }
    }

    private void acceptStartObject() throws IOException {

        parseDepth++;
        parseStack[parseDepth].stateType = ParseStateType.OBJECT;

        handlePushObject(parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);
    }

    private void acceptEndObject() throws IOException {

        handlePopObject(parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);

        parseDepth--;

        if (parseStack[parseDepth].stateType == ParseStateType.FIELD)
            parseDepth--;
    }

    private void acceptObjectContinue(JsonToken token) throws IOException {

        switch (token) {

            case FIELD_NAME: acceptFieldName();
            case END_OBJECT: acceptEndObject();

            default:
                throw new EUnexpected();  // todo: invalid json
        }
    }

    private void acceptFieldName() throws IOException {

        parseDepth++;
        parseStack[parseDepth].stateType = ParseStateType.FIELD;
        parseStack[parseDepth].fieldName = lexer.currentName();

        handleFieldName(parseStack[parseDepth], parseStack[parseDepth-1], parseDepth);
    }

    private void acceptFieldValue(JsonToken token) throws IOException {

        var parseState = parseStack[parseDepth];

        if (token.isScalarValue()) {
            handleFieldValue(parseState, parseDepth);
            return;
        }

        switch (token) {

            case START_OBJECT: acceptStartObject(); break;
            case START_ARRAY: acceptStartArray(); break;

            default:
                throw new EUnexpected();  // todo: invalid json
        }
    }

    static class ParseState {

        ParseStateType stateType;
        String fieldName;
    }
}
