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

package org.finos.tracdap.common.auth.internal;

import org.finos.tracdap.common.exception.EAuthorization;


public class AuthHelpers {

    public static UserInfo currentAuthUser() {

        var authUser = AuthConstants.TRAC_AUTH_USER_KEY.get();

        if (authUser != null)
            return authUser;

        throw new EAuthorization("User details are not available");
    }

    public static UserInfo currentUser() {

        var delegate = AuthConstants.TRAC_DELEGATE_KEY.get();

        if (delegate != null)
            return delegate;

        var authUser = AuthConstants.TRAC_AUTH_USER_KEY.get();

        if (authUser != null)
            return authUser;

        throw new EAuthorization("User details are not available");
    }

    public static String printCurrentUser() {

        var authUser = AuthConstants.TRAC_AUTH_USER_KEY.get();
        var delegate = AuthConstants.TRAC_DELEGATE_KEY.get();

        return printCurrentUser(authUser, delegate);
    }

    public static String printCurrentAuthUser() {

        var authUser = AuthConstants.TRAC_AUTH_USER_KEY.get();

        return printCurrentAuthUser(authUser);
    }

    static String printCurrentAuthUser(UserInfo authUser) {

        return String.format("%s <%s>", authUser.getDisplayName(), authUser.getUserId());
    }

    static String printCurrentUser(UserInfo authUser, UserInfo delegate) {

        if (delegate != null)
            return String.format("%s <%s> on behalf of %s <%s>",
                    authUser.getDisplayName(), authUser.getUserId(),
                    delegate.getDisplayName(), delegate.getUserId());

        else
            return String.format("%s <%s>", authUser.getDisplayName(), authUser.getUserId());
    }
}
