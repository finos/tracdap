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

package org.finos.tracdap.common.auth.external;


import io.netty.channel.ChannelHandlerContext;
import org.finos.tracdap.common.config.ISecretLoader;


public interface IAuthProvider {

    AuthResult attemptAuth(ChannelHandlerContext ctx, IAuthHeaders headers);

    // These methods allow using the TRAC user DB for basic auth
    // TODO: Find a more elegant solution

    default boolean wantTracUsers() {
        return false;
    }

    default void setTracUsers(ISecretLoader userDb) {
        // no-op
    }
}
