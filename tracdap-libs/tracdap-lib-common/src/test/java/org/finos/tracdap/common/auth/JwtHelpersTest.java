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

package org.finos.tracdap.common.auth;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class JwtHelpersTest {

    @Test
    void jwtRoundTrip() {

        var userInfo = new UserInfo();
        userInfo.setUserId("fb2876");
        userInfo.setDisplayName("Fred Blogs Jnr.");

        var token = JwtHelpers.encodeToken(userInfo, Duration.of(1, ChronoUnit.HOURS));

        var rtSession = JwtHelpers.decodeAndValidate(token);
        var rtUserInfo = rtSession.getUserInfo();

        Assertions.assertEquals(userInfo.getUserId(), rtUserInfo.getUserId());
        Assertions.assertEquals(userInfo.getDisplayName(), rtUserInfo.getDisplayName());
    }
}
