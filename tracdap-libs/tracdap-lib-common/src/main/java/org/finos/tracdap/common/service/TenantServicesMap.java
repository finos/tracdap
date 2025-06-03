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

package org.finos.tracdap.common.service;

import org.finos.tracdap.common.exception.ETenantNotFound;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;


public class TenantServicesMap<TServices> {

    private final ConcurrentHashMap<String, TServices> tenants;

    protected TenantServicesMap() {
        this.tenants = new ConcurrentHashMap<>();
    }

    public boolean addTenant(String tenantCode, TServices tenant) {
        var prior = tenants.putIfAbsent(tenantCode, tenant);
        return prior == null;
    }

    public TServices removeTenant(String tenantCode) {
        return tenants.remove(tenantCode);
    }

    public TServices lookupTenant(String tenantCode) {

        var tenant = tenants.get(tenantCode);

        if (tenant == null)
            throw new ETenantNotFound("Tenant not found: [" + tenantCode + "]");

        return tenant;
    }

    public void closeAllTenants(BiConsumer<String, TServices> closeFunc) {

        for (var i = tenants.entrySet().iterator(); i.hasNext();) {

            var entry = i.next();
            i.remove();

            closeFunc.accept(entry.getKey(), entry.getValue());
        }
    }
}
