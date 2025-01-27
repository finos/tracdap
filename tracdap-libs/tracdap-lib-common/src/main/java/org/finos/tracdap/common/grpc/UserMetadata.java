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

package org.finos.tracdap.common.grpc;

import io.grpc.Context;

import javax.annotation.Nonnull;
import java.io.Serializable;


/**
 * Provide a mechanism for passing user metadata into gRPC service calls.
 * <br/>
 *
 * Service extensions can set user info to make it available inside service calls.
 * This is intended for passing user info to the TRAC metadata store, or
 * to add user annotations to jobs, service logs etc. It is not intended
 * as a way to manage authentication or access control.
 */
public class UserMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String UNKNOWN_USER_ID = "#trac_unknown";
    public static final String UNKNOWN_USER_NAME = "Unknown User";

    private static final UserMetadata USER_METADATA_NOT_SET = new UserMetadata(UNKNOWN_USER_ID, UNKNOWN_USER_NAME);
    private static final Context.Key<UserMetadata> USER_METADATA_KEY = Context.keyWithDefault("trac-user-metadata", USER_METADATA_NOT_SET);

    public static Context set(Context context, UserMetadata userMetadata) {

        return context.withValue(USER_METADATA_KEY, userMetadata);
    }

    public static UserMetadata get(Context context) {

        return USER_METADATA_KEY.get(context);
    }

    private final String userId;
    private final String userName;

    public UserMetadata(@Nonnull String userId, @Nonnull String userName) {
        this.userId = userId;
        this.userName = userName;
    }

    public String userId() {
        return userId;
    }

    public String userName() {
        return userName;
    }
}
