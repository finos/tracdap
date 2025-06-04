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

package org.finos.tracdap.common.metadata.dal;

import org.finos.tracdap.common.exception.ETenantNotFound;
import org.finos.tracdap.common.metadata.test.IDalTestable;
import org.finos.tracdap.common.metadata.test.JdbcIntegration;
import org.finos.tracdap.common.metadata.test.JdbcUnit;

import org.finos.tracdap.metadata.TenantInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.finos.tracdap.test.meta.SampleMetadata.*;
import static org.finos.tracdap.test.meta.SampleMetadata.TEST_TENANT;


abstract class MetadataDalTenantsTest implements IDalTestable {

    private IMetadataDal dal;

    public void setDal(IMetadataDal dal) {
        this.dal = dal;
    }

    @ExtendWith(JdbcUnit.class)
    static class UnitTest extends MetadataDalTenantsTest {
    }

    @Tag("integration")
    @Tag("int-metadb")
    @ExtendWith(JdbcIntegration.class)
    static class IntegrationTest extends MetadataDalTenantsTest {
    }

    @Test
    void listTenants() {

        var listing = dal.listTenants();

        Assertions.assertEquals(2, listing.size());
        Assertions.assertTrue(listing.stream().anyMatch(tenant -> tenant.getTenantCode().equals(TEST_TENANT)));
        Assertions.assertTrue(listing.stream().anyMatch(tenant -> tenant.getTenantCode().equals(ALT_TEST_TENANT)));
    }

    @Test
    void updateTenant_ok() {

        var listing = dal.listTenants();
        var originalTenant = listing.get(0);

        Assertions.assertNotEquals("New display name", originalTenant.getDescription());

        var modifiedTenant = originalTenant.toBuilder().setDescription("New display name").build();
        dal.updateTenant(modifiedTenant);

        var updatedListing = dal.listTenants();

        var updatedTenant = updatedListing.stream()
                .filter(t -> t.getTenantCode().equals(originalTenant.getTenantCode()))
                .findFirst();

        Assertions.assertTrue(updatedTenant.isPresent());
        Assertions.assertEquals("New display name", updatedTenant.get().getDescription());
    }

    @Test
    void updateTenant_notFound() {

        var modifiedTenant = TenantInfo.newBuilder()
                .setTenantCode("NONEXISTENT_TENANT")
                .setDescription("New display name")
                .build();

        Assertions.assertThrows(ETenantNotFound.class, () -> dal.updateTenant(modifiedTenant));
    }
}
