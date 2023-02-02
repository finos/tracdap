/*
 * Copyright 2023 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.auth.external.common;

import io.netty.channel.ChannelHandlerContext;
import org.finos.tracdap.common.auth.external.AuthResult;
import org.finos.tracdap.common.auth.external.IAuthHeaders;
import org.finos.tracdap.common.auth.external.IAuthProvider;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.config.ISecretLoader;

import org.finos.tracdap.common.exception.ETracInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BuiltInAuthProvider implements IAuthProvider {

    private static final Logger log = LoggerFactory.getLogger(BasicAuthProvider.class);

    private static final String DISPLAY_NAME_ATTR = "displayName";

    private final ISecretLoader userDb;

    public BuiltInAuthProvider(ConfigManager configManager) {

        this.userDb = configManager.getUserDb();
    }

    @Override
    public AuthResult attemptAuth(ChannelHandlerContext ctx, IAuthHeaders headers) {

        throw new ETracInternal("Not implemented yet");
    }
}
