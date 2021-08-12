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


class EDataValidation(EValidation):

    """
    Validation failure when a model output is checked for conformance to its data schema
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
    It should be used to guard against error conditions between different modules or sub-systems in the engine.
    In most cases an internal error will cause the platform to shut down, however there may be cases in some
    modules where these errors can be handled (e.g. by discarding a failing plugin).

    This error can be raised e.g. by an internal component that receives an invalid request from client code
    in another part of the engine. For error conditions that are wholly under the control o a single module
    or sub-system, use EUnexpected instead.
    """

    pass


class EUnexpected(ETrac):

    """
    An unexpected error has occurred in the TRAC runtime engine (this is a bug)

    This error always indicates a bug, it signals that the engine is in an inconsistent state.
    It should be used only for errors that can never happen, within the scope of a module or sub-system
    when that module is accessed via its public API. Never use EUnexpected for expected errors!
    The runtime will be shut down with a failed exit code, any running or pending jobs will also be failed.

    This error can be raised e.g. for failed sanity checks, where the engine should guarantee some condition
    and that condition has not been met. For error conditions that might occur as a result of problems in
    other parts of the engine (but not relating to external systems), use ETracInternal instead.
    """

    pass
