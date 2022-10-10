#  Copyright 2021 Accenture Global Solutions Limited
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.


class ETrac(Exception):

    """
    Root of the TRAC runtime exception hierarchy
    """

    pass


class EStartup(ETrac):

    """
    An error during the runtime startup sequence
    """

    pass


class EConfig(ETrac):

    """
    Indicates an error in a config file (either system or job config)
    """

    pass


class EConfigLoad(EConfig):

    """
    Config errors related to loading files and resources
    Includes errors loading config from platform-specific backends using plugins (e.g. cloud key stores and buckets)
    Does not include errors relating to the content of the configuration
    """

    pass


class EConfigParse(EConfig):

    """
    Config errors relating to syntax, structure and config validation.
    Does not include errors loading the file, e.g. disk read failures or timeouts.
    """

    pass


class EPluginNotAvailable(ETrac):

    pass


class EValidation(ETrac):

    """
    Base class for validation errors.

    Validation occurs on the inside of all platform interfaces,
    including the runtime API for models, the web API and the CLI and platform/jpb config interfaces
    """

    pass


class EJobValidation(EValidation):

    """
    A job submitted to the engine has failed validation

    This could be because the job config is invalid or does not align with the platform config
    It could also happen if the job references resources that are superseded or expunged
    """

    pass


class EModelValidation(EValidation):

    """
    Validation failure when a model is imported or loaded

    For model import failures, this means the define_* functions in the model class are not valid.
    For model load failures, it means the define_* functions do not match the model metadata.
    """

    pass


class ERuntimeValidation(EValidation):

    """
    Validation failure when a model calls into the TRAC API using the methods in TracContext
    """

    pass


class EModel(ETrac):

    """
    Base class for model errors
    """

    pass


class EModelLoad(EModel):

    """
    Indicates a failure to load the model class, or other runtime resources
    """

    pass


class EModelExec(ETrac):

    """
    An error occurred during the execution of model code

    Models may throw this exception explicitly to indicate a failure. TRAC will also wrap unhandled errors that escape
    model code with EModelExec, such as unhandled failures in number format conversion or null handling. This may result
    in some technical errors being classed as EModelExec, e.g. Spark communication failures during model execution.

    This exception is not used for TRAC errors, e.g. validation or storage errors, which have their own exceptions.
    Error types that extend BaseException instead of Exception will not be wrapped; this specifically includes
    SystemExit and KeyboardInterrupt errors.
    """

    pass


class EData(ETrac):

    """
    A data exception indicates a problem with primary data itself (as opposed to storage or marshalling errors)
    """

    pass


class EDataCorruption(EData):

    """
    Data is considered corrupt when it cannot be understood according to its own format

    How corrupt data is detected depends on the data format. For example, for JSON text data,
    a data stream might be corrupt if it contains an invalid Unicode sequence, or if the JSON
    fails to parse, for example because of a missing brace. For binary formats can occur in the
    structure of the binary data stream and will be detected if that data stream cannot be
    understood.

    Data corruption does not include data constraints, such as non-null or range constraints,
    which are reported as EDataConstraint.
    """

    pass


class EDataConformance(EData):

    """
    Data does not conform to the required the schema (and cannot be coerced)
    """

    pass


class EStorage(ETrac):

    """
    Indicates an error that occurs in the storage layer
    """

    pass


class EStorageConfig(EStorage):

    """
    Storage referenced in metadata is either not supported or not configured
    """

    pass


class EStorageCommunication(EStorage):

    """
    Communication with the storage layer fails for a technical reason

    These errors will manifest as time-outs, partial or garbled responses.
    """

    pass


class EStorageAccess(EStorage):

    """
    The storage layer responds, but access is denied
    """

    pass


class EStorageRequest(EStorage):

    """
    The storage layer responds, but the request fails for a structural reason

    E.g. File not found or attempting to create a file that already exists.
    """

    pass


class EModelRepo(ETrac):

    """
    Indicates an error that occurs with the model loading mechanism
    """

    pass


class EModelRepoConfig(EModelRepo):

    """
    Model repo referenced in metadata is either not supported or not configured
    """

    pass


class EModelRepoCommunication(EModelRepo):

    """
    Communication with the model repository fails for a technical reason

    These errors will manifest as time-outs, partial or garbled responses.
    """

    pass


class EModelRepoAccess(EModelRepo):

    """
    The model repository responds, but access is denied
    """

    pass


class EModelRepoRequest(EModelRepo):

    """
    The model repository responds, but the request fails for a structural reason

    E.g. Unknown package, version, tag or commit hash.
    """

    pass


class ETracInternal(ETrac):

    """
    An internal error has occurred in the TRAC runtime engine (this is a bug)

    This error indicates a problem with the runtime engine or with one of its subsystems or plugins.
    It always represents a bug (although sometimes there may be a genuine error as well that has not been
    properly handled). Internal errors are normally fatal because the engine has gone into an inconsistent state.
    A few special cases may be recoverable (e.g. discarding a failing plugin on load).
    """

    pass


class EUnexpected(ETracInternal):

    """
    An unexpected error has occurred in the TRAC runtime engine (this is a bug)

    An unexpected error is an internal error that should never happen, it represents a logical
    inconsistency within a single component or subsystem. For example, a failed sanity
    check or a case where a condition that should be guaranteed is not met. Unexpected errors
    are always fatal.

    The conditions for EUnexpected should be wholly expressed by a single component or subsystem.
    If a component exposes an interface to other components of the engine, and requests via that interface
    are invalid or can put the component into an illegal state, that does not count as unexpected.
    If this condition is not met, use ETracInternal instead.
    """

    pass


class EValidationGap(ETracInternal):

    """
    A validation error has occurred in the TRAC runtime engine (this is a bug)

    A validation gap is a type of internal error, it indicates a condition inside
    TRAC that should have been caught higher up the stack in a validation layer.
    """

    pass
