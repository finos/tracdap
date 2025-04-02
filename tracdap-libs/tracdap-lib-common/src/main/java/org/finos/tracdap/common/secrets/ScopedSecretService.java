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

package org.finos.tracdap.common.secrets;

import org.finos.tracdap.common.config.ConfigManager;

import java.security.PrivateKey;
import java.security.PublicKey;


public class ScopedSecretService implements ISecretService {

    private static final String ROOT_SCOPE = "/";

    private final ISecretService delegate;
    private final String scope;

    public static ISecretService rootScope(ISecretService delegate) {
        return new ScopedSecretService(delegate, ROOT_SCOPE);
    }

    private ScopedSecretService(ISecretService delegate, String scope) {
        this.delegate = delegate;
        this.scope = scope;
    }

    @Override
    public void init(ConfigManager configManager) {
        delegate.init(configManager);
    }

    @Override
    public ISecretService scope(String scope) {
        var childScope = translateScope(scope);
        return new ScopedSecretService(delegate, childScope);
    }

    @Override
    public boolean hasSecret(String secretName) {
        return delegate.hasSecret(translateScope(secretName));
    }

    @Override
    public boolean hasAttr(String secretName, String attrName) {
        return delegate.hasAttr(translateScope(secretName), attrName);
    }

    @Override
    public void storePassword(String secretName, String password) {
        delegate.storePassword(translateScope(secretName), password);
    }

    @Override
    public String loadPassword(String secretName) {
        return delegate.loadPassword(translateScope(secretName));
    }

    @Override
    public String loadAttr(String secretName, String attrName) {
        return delegate.loadAttr(translateScope(secretName), attrName);
    }

    @Override
    public PublicKey loadPublicKey(String secretName) {
        return delegate.loadPublicKey(translateScope(secretName));
    }

    @Override
    public PrivateKey loadPrivateKey(String secretName) {
        return delegate.loadPrivateKey(translateScope(secretName));
    }

    private String translateScope(String secretName) {
        return scope + "/" + secretName;
    }
}
