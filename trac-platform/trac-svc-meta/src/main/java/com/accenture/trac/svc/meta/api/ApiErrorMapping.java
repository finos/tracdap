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

import com.accenture.trac.svc.meta.exception.*;
import io.grpc.Status;

import java.util.Map;


public class ApiErrorMapping {

    static final Map<Class<? extends Throwable>, Status.Code> ERROR_MAPPING = Map.of(

            AuthorisationError.class, Status.Code.PERMISSION_DENIED,
            InputValidationError.class, Status.Code.INVALID_ARGUMENT,

            MissingItemError.class, Status.Code.NOT_FOUND,
            DuplicateItemError.class, Status.Code.ALREADY_EXISTS,
            WrongItemTypeError.class, Status.Code.FAILED_PRECONDITION
    );
}
