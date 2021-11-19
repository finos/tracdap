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

package com.accenture.trac.common.config;

import com.accenture.trac.common.exception.EStartup;

import com.accenture.trac.common.exception.EUnexpected;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.*;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;


public class ConfigParser {

    private static final Logger log = LoggerFactory.getLogger(ConfigParser.class);

    static <TConfig> TConfig parseStructuredConfig(
            String configData, ConfigFormat configFormat,
            Class<TConfig> elementClass) throws EStartup {

        switch (configFormat) {

            case YAML: return parseYamlConfig(configData, elementClass);

            default:
                throw new EStartup(String.format("Unsupported config format '%s'", configFormat));
        }
    }

    static <TConfig> TConfig parseYamlConfig(String configData, Class<TConfig> elementClass) throws EStartup {

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

