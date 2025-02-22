/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.common.config.test;

import com.google.protobuf.Descriptors;
import org.finos.tracdap.common.plugin.ITracExtension;
import org.finos.tracdap.test.config.ConfigExtensionsProto;

import java.util.List;


public class TestConfigExtension implements ITracExtension {

    private static final String EXTENSION_NAME = "TEST_CONFIG_EXTENSION";

    private final List<Descriptors.FileDescriptor> protoFiles;

    public TestConfigExtension() {
        this.protoFiles = List.of(ConfigExtensionsProto.getDescriptor());
    }

    @Override
    public String extensionName() {
        return EXTENSION_NAME;
    }

    @Override
    public List<Descriptors.FileDescriptor> configExtensions() {
        return protoFiles;
    }
}
