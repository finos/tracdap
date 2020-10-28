/*
 * Copyright 2020 Accenture Global Solutions Limited
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

package com.accenture.trac.svc.meta.api;

import com.accenture.trac.common.api.meta.MetadataSearchApiGrpc;
import com.accenture.trac.common.api.meta.MetadataSearchRequest;
import com.accenture.trac.common.api.meta.MetadataSearchResponse;
import com.accenture.trac.common.util.ApiWrapper;
import com.accenture.trac.svc.meta.services.MetadataSearchService;
import io.grpc.stub.StreamObserver;


public class MetadataSearchApi extends MetadataSearchApiGrpc.MetadataSearchApiImplBase {

    private final ApiWrapper apiWrapper;
    private final MetadataSearchService searchService;

    public MetadataSearchApi(MetadataSearchService searchService) {
        this.apiWrapper = new ApiWrapper(getClass(), ApiErrorMapping.ERROR_MAPPING);
        this.searchService = searchService;
    }

    @Override
    public void search(MetadataSearchRequest request, StreamObserver<MetadataSearchResponse> response) {

        apiWrapper.unaryCall(response, () -> {

            var tenant = request.getTenant();
            var searchParams = request.getSearchParams();

            var searchResult = searchService.search(tenant, searchParams);

            return searchResult.thenApply(resultList -> MetadataSearchResponse.newBuilder()
                    .addAllSearchResult(resultList)
                    .build());
        });
    }
}
