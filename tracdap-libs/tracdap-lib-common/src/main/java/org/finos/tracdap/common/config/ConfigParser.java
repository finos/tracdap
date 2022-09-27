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

import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.EUnexpected;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;


public class ConfigParser {

    public static <TConfig extends Message>
    TConfig parseConfig(byte[] configData, ConfigFormat configFormat, Class<TConfig> configClass) {

        return parseConfig(configData, configFormat, configClass, /* leniency = */ false);
    }

    @SuppressWarnings("unchecked")
    public static <TConfig extends Message>
    TConfig parseConfig(byte[] configData, ConfigFormat configFormat, Class<TConfig> configClass, boolean leniency) {

        try {

            var newBuilder = configClass.getMethod("newBuilder");
            var builder = (TConfig.Builder) newBuilder.invoke(null);
            var blankConfig = (TConfig) builder.build();

            switch (configFormat) {

                case PROTO:
                    return parseProtoConfig(configData, blankConfig);

                case JSON:
                    var json = new String(configData, StandardCharsets.UTF_8);
                    return parseJsonConfig(json, blankConfig, leniency);

                case YAML:
                    var yaml = new String(configData, StandardCharsets.UTF_8);
                    return parseYamlConfig(yaml, blankConfig, leniency);

                default:
                    throw new EStartup(String.format("Unknown config format [%s]", configFormat));
            }
        }
        catch (InvalidProtocolBufferException e) {

            throw new EStartup("Invalid config: " + e.getMessage(), e);
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {

            // Error invoking reflective method for builder
            throw new EUnexpected();
        }
    }

    @SuppressWarnings("unchecked")
    private static <TConfig extends Message>
    TConfig parseProtoConfig(byte[] protoData, TConfig defaultConfig) throws InvalidProtocolBufferException {

        var builder = defaultConfig.newBuilderForType();

        return (TConfig) builder
                .mergeFrom(protoData)
                .build();
    }

    @SuppressWarnings("unchecked")
    private static <TConfig extends Message>
    TConfig parseJsonConfig(String jsonData, TConfig defaultConfig, boolean lenient) throws InvalidProtocolBufferException {

        var parser = JsonFormat.parser();
        var builder = defaultConfig.newBuilderForType();

        if (lenient)
            parser = parser.ignoringUnknownFields();

        parser.merge(jsonData,  builder);

        return (TConfig) builder.build();
    }

    private static <TConfig extends Message>
    TConfig parseYamlConfig(String yamlData, TConfig defaultConfig, boolean leniency) throws InvalidProtocolBufferException {

        // To parse YAML, first convert into JSON using Jackson
        // Then parse JSON using the Protobuf built-in JSON parser

        try  {

            var yamlFactory = new YAMLFactory();
            var jsonFactory = new JsonFactory();

            var reader = new YAMLMapper(yamlFactory);
            var writer = new JsonMapper(jsonFactory);

            var obj = reader.readValue(yamlData, Object.class);
            var jsonData = writer.writeValueAsString(obj);

            return parseJsonConfig(jsonData, defaultConfig, leniency);
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
    }

}

