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

package org.finos.tracdap.svc.meta.services;

import org.finos.tracdap.metadata.Tag;
import org.finos.tracdap.metadata.SearchParameters;
import org.finos.tracdap.meta.dal.IMetadataDal;

import java.util.List;


public class MetadataSearchService {

    private final IMetadataDal dal;

    public MetadataSearchService(IMetadataDal dal) {
        this.dal = dal;
    }

    public List<Tag>
    search(String tenant, SearchParameters searchParameters) {

        // Validation currently in the API layer

        return dal.search(tenant, searchParameters);
    }
}
