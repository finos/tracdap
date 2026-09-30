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

package org.finos.tracdap.svc.amin.test;

import org.finos.tracdap.common.exception.EConsistencyValidation;
import org.finos.tracdap.common.exception.EInputValidation;
import org.finos.tracdap.common.exception.EMetadataDuplicate;
import org.finos.tracdap.common.service.ConfigWrite;
import org.finos.tracdap.common.service.IConfigWriteHook;
import org.finos.tracdap.metadata.CredentialDefinition;
import org.finos.tracdap.metadata.ObjectDefinition;
import org.finos.tracdap.metadata.ObjectType;

import java.util.Map;


/**
 * Acts only on config classes starting with {@link #CLASS_PREFIX}, so other tests in the module are unaffected.
 * Records the last write it saw for assertions.
 */
public class TestConfigWriteHook implements IConfigWriteHook {

    public static final String CLASS_PREFIX = "hook_test";

    public static final String TRANSFORMED_PROPERTY = "hook_transformed";
    public static final String CHANGE_TYPE_PROPERTY = "hook_change_type";
    public static final String REJECT_PROPERTY = "hook_reject";

    public static volatile ConfigWrite lastTransform;
    public static volatile ConfigWrite lastValidate;
    public static volatile ObjectDefinition lastValidated;
    public static volatile int lastReaderCount;

    public static void reset() {
        lastTransform = null;
        lastValidate = null;
        lastValidated = null;
        lastReaderCount = -1;
    }

    @Override
    public ObjectDefinition transform(ConfigWrite write) {

        if (!write.configClass().startsWith(CLASS_PREFIX))
            return write.definition();

        lastTransform = write;

        var definition = write.definition();

        if ("true".equals(properties(definition).get(CHANGE_TYPE_PROPERTY))) {

            return ObjectDefinition.newBuilder()
                    .setObjectType(ObjectType.CREDENTIAL)
                    .setCredential(CredentialDefinition.newBuilder().putProperties("changed", "true"))
                    .build();
        }

        return withProperty(definition, TRANSFORMED_PROPERTY, "true");
    }

    @Override
    public void validate(ConfigWrite write, ObjectDefinition definition) {

        if (!write.configClass().startsWith(CLASS_PREFIX))
            return;

        lastValidate = write;
        lastValidated = definition;

        lastReaderCount = write.tenant() == null
                ? write.reader().listPlatformConfig(write.configClass(), false).size()
                : write.reader().listConfig(write.tenant(), write.configClass(), false).size();

        var source = definition != null ? definition : write.priorDefinition();
        var reject = properties(source).get(REJECT_PROPERTY);

        if ("input".equals(reject))
            throw new EInputValidation("Rejected by test hook");

        if ("consistency".equals(reject))
            throw new EConsistencyValidation("Rejected by test hook");

        if ("duplicate".equals(reject))
            throw new EMetadataDuplicate("Rejected by test hook");
    }

    private static Map<String, String> properties(ObjectDefinition definition) {

        switch (definition.getObjectType()) {
            case CONFIG: return definition.getConfig().getPropertiesMap();
            case IDENTITY: return definition.getIdentity().getPropertiesMap();
            default: return Map.of();
        }
    }

    private static ObjectDefinition withProperty(ObjectDefinition definition, String key, String value) {

        switch (definition.getObjectType()) {

            case CONFIG:
                return definition.toBuilder()
                        .setConfig(definition.getConfig().toBuilder().putProperties(key, value))
                        .build();

            case IDENTITY:
                return definition.toBuilder()
                        .setIdentity(definition.getIdentity().toBuilder().putProperties(key, value))
                        .build();

            default:
                return definition;
        }
    }
}
