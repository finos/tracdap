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
