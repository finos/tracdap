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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.SearchParameters;
import org.finos.tracdap.svc.meta.dal.IMetadataDal;

import java.util.List;
import java.util.concurrent.CompletableFuture;


public class MetadataSearchService {

    private final IMetadataDal dal;

    public MetadataSearchService(IMetadataDal dal) {
        this.dal = dal;
    }

    public CompletableFuture<List<Tag>>
    search(String tenant, SearchParameters searchParameters) {

        // Validation currently in the API layer

        return dal.search(tenant, searchParameters);
    }
}
