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

package org.finos.tracdap.common.config;

import org.finos.tracdap.common.exception.EConfigParse;
import org.finos.tracdap.common.exception.EUnexpected;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;


public class ConfigParser {

    public static <TConfig extends Message>
    TConfig parseConfig(byte[] configData, ConfigFormat configFormat, Class<TConfig> configClass) {

        return parseConfig(configData, configFormat, configClass, /* leniency = */ false);
    }

    public static <TConfig extends Message>
    TConfig parseConfig(byte[] configData, ConfigFormat configFormat, Class<TConfig> configClass, boolean leniency) {

        try {

            var newBuilder = configClass.getMethod("newBuilder");
            var builder = (TConfig.Builder) newBuilder.invoke(null);

            switch (configFormat) {

                case PROTO:
                    return parseProtoConfig(configData, builder);

                case JSON:
                    return parseJsonConfig(configData, builder, leniency);

                case YAML:
                    return parseYamlConfig(configData, builder, leniency);

                default:
                    throw new EConfigParse(String.format("Unknown config format [%s]", configFormat));
            }
        }
        catch (InvalidProtocolBufferException e) {

            throw new EConfigParse("Invalid config: " + e.getMessage(), e);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {

            // Error invoking reflective method for builder
            throw new EUnexpected();
        }
    }

    @SuppressWarnings("unchecked")
    private static <TConfig extends Message>
    TConfig parseProtoConfig(byte[] protoData, TConfig.Builder builder) throws InvalidProtocolBufferException {

        return (TConfig) builder
                .mergeFrom(protoData)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static <TConfig extends Message>
    TConfig parseJsonConfig(byte[] jsonData, TConfig.Builder builder, boolean lenient) throws InvalidProtocolBufferException {

        var parser = JsonFormat.parser();

        if (lenient)
            parser = parser.ignoringUnknownFields();

        try (var in = new ByteArrayInputStream(jsonData);
             var reader = new InputStreamReader(in, StandardCharsets.UTF_8)) {

            parser.merge(reader, builder);
            return (TConfig) builder.build();
        }
        catch (InvalidProtocolBufferException e) {
            throw e;
        }
        catch (IOException e) {
            throw new EUnexpected();
        }
    }

    private static <TConfig extends Message>
    TConfig parseYamlConfig(byte[] yamlData, TConfig.Builder builder, boolean leniency) throws InvalidProtocolBufferException {

        // To parse YAML, first convert into JSON using Jackson
        // Then parse JSON using the Protobuf built-in JSON parser

        try  {

            var yamlFactory = new YAMLFactory();
            var jsonFactory = new JsonFactory();

            var reader = new YAMLMapper(yamlFactory);
            var writer = new JsonMapper(jsonFactory);

            var obj = reader.readValue(yamlData, Object.class);
            var json = writer.writeValueAsBytes(obj);

            return parseJsonConfig(json, builder, leniency);
        }
        catch (InvalidProtocolBufferException e) {
            throw e;
        }
        catch (IOException e) {

            throw new EConfigParse("Invalid config: " + e.getMessage(), e);
        }
    }

    public static <TConfig extends Message>
    byte[] quoteConfig(TConfig config, ConfigFormat configFormat) {

        try {

            switch (configFormat) {

                case PROTO:
                    return quoteProtoConfig(config);

                case JSON:
                    return quoteJsonConfig(config);

                case YAML:
                    return quoteYamlConfig(config);

                default:
                    throw new EConfigParse(String.format("Unknown config format [%s]", configFormat));
            }
        }
        catch (InvalidProtocolBufferException error) {

            throw new EConfigParse("Invalid config: " + error.getMessage(), error);
        }
    }

    private static <TConfig extends Message>
    byte[] quoteProtoConfig(TConfig config) throws InvalidProtocolBufferException {

        return config.toByteArray();
    }

    private static <TConfig extends Message>
    byte[] quoteJsonConfig(TConfig config) throws InvalidProtocolBufferException {

        var quoter = JsonFormat.printer();

        try (var out = new ByteArrayOutputStream();
             var writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {

            quoter.appendTo(config, writer);
            writer.flush();

            return out.toByteArray();
        }
        catch (InvalidProtocolBufferException e) {
            throw e;
        }
        catch (IOException e) {
            throw new EUnexpected();
        }
    }

    private static <TConfig extends Message>
    byte[] quoteYamlConfig(TConfig config) throws InvalidProtocolBufferException {

        // To quote YAML, first quote JSON then convert into YAML using Jackson

        try  {

            var json = quoteJsonConfig(config);

            var jsonFactory = new JsonFactory();
            var yamlFactory = new YAMLFactory();

            var reader = new JsonMapper(jsonFactory);
            var writer = new YAMLMapper(yamlFactory);

            var obj = reader.readValue(json, Object.class);
            return writer.writeValueAsBytes(obj);
        }
        catch (InvalidProtocolBufferException e) {
            throw e;
        }
        catch (IOException e) {

            throw new EConfigParse("Invalid config: " + e.getMessage(), e);
        }
    }
}

