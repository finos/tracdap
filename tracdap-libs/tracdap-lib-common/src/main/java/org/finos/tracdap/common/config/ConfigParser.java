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
import io.netty.buffer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.*;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;


public class ConfigParser {

    private static final Logger log = LoggerFactory.getLogger(ConfigParser.class);


    public static <TConfig extends Message, B extends Message.Builder>
    TConfig parseConfig(
            ByteBuf configData, ConfigFormat configFormat,
            Class<TConfig> configClass) {





        try {

            var newBuilder = configClass.getMethod("newBuilder");
            var builder = (TConfig.Builder) newBuilder.invoke(null);
            var blankConfig = (TConfig) builder.build();

            switch (configFormat) {

                case PROTO:
                    return parseProtoConfig(configData, blankConfig);

                case JSON:
                    return parseJsonConfig(configData, blankConfig);

                case YAML:
                    return parseYamlConfig(configData, blankConfig);

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
        finally {

            configData.release();
        }
    }

    private static <TConfig extends Message>
    TConfig parseProtoConfig(ByteBuf configData, TConfig defaultConfig) throws InvalidProtocolBufferException {

        return (TConfig) defaultConfig.toBuilder()
                .mergeFrom(configData.array())
                .build();
    }

    private static <TConfig extends Message>
    TConfig parseJsonConfig(ByteBuf configData, TConfig defaultConfig) throws InvalidProtocolBufferException {

        var json = configData.getCharSequence(0, configData.readableBytes(), StandardCharsets.UTF_8).toString();

        var parser = JsonFormat.parser();
        var builder = defaultConfig.toBuilder();

        parser.merge(json, builder);

        return (TConfig) builder.build();
    }

    private static <TConfig extends Message>
    TConfig parseYamlConfig(ByteBuf configData, TConfig defaultConfig) throws InvalidProtocolBufferException {

        var json = Unpooled.EMPTY_BUFFER;

        try (var stream = (InputStream) new ByteBufInputStream(configData)) {

            var yamlFactory = new YAMLFactory();
            var jsonFactory = new JsonFactory();

            var reader = new YAMLMapper(yamlFactory);
            var writer = new JsonMapper(jsonFactory);

            var obj = reader.readValue(stream, Object.class);
            var jsonBytes = writer.writeValueAsBytes(obj);

            json = Unpooled.wrappedBuffer(jsonBytes);

            return parseJsonConfig(json, defaultConfig);
        }
        catch (IOException e) {

            throw new EStartup(e.getMessage(), e);
        }
        finally {

            if (json != Unpooled.EMPTY_BUFFER)
                json.release();
        }
    }



    static <TConfig> TConfig parseStructuredConfig(
            String configData, ConfigFormat configFormat,
            Class<TConfig> elementClass) throws EStartup {

        switch (configFormat) {

            case YAML: return parseYamlConfig_(configData, elementClass);

            default:
                throw new EStartup(String.format("Unsupported config format '%s'", configFormat));
        }
    }

    static <TConfig> TConfig parseYamlConfig_(String configData, Class<TConfig> elementClass) throws EStartup {

        // TODO: This is a scratch implementation!
        // TODO: Work is needed to make this much more robust and report errors in a sensible way

        try {

            var loaderOptions = new LoaderOptions();
            loaderOptions.setEnumCaseSensitive(false);

            var constructor = new Constructor(elementClass, loaderOptions);
            var yaml = new Yaml(constructor);

            return yaml.loadAs(configData, elementClass);
        }
        catch (MarkedYAMLException e) {

            // TODO: Errors
            log.error("YAML Error", e);
            throw new EStartup("YAML error", e);

        }
        catch (YAMLException e) {
            throw new EStartup("There was an unexpected problem parsing YAML config: " + e.getMessage(), e);
        }
    }
}

