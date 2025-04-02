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

package org.finos.tracdap.common.secrets.jks;

import org.finos.tracdap.common.config.CryptoHelpers;
import org.finos.tracdap.common.exception.EConfigLoad;
import org.finos.tracdap.common.secrets.ISecretService;
import org.finos.tracdap.common.secrets.ScopedSecretService;
import org.finos.tracdap.common.startup.StartupLog;
import org.slf4j.event.Level;

import java.util.Properties;


public class JksSecretService extends JksSecretLoader implements ISecretService {

    public JksSecretService(Properties properties) {
        super(properties);
    }

    @Override
    public ISecretService scope(String scope) {
        return ScopedSecretService.rootScope(this).scope(scope);
    }

    @Override
    public void storePassword(String secretName, String password) {

        try {
            CryptoHelpers.writeTextEntry(keystore, secretKey, secretName, password);
        }
        catch (EConfigLoad e) {
            var message = String.format("Password could not be saved to the key store: [%s] %s", secretName, e.getMessage());
            StartupLog.log(this, Level.ERROR, message);
            throw new EConfigLoad(message, e);
        }
    }
}
