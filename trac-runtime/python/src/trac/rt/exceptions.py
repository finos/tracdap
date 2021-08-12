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


class EBadConfig(ETrac):

    """
    Indicates an error in a config file (either system or job config)

    Includes errors for syntax, structure and config validation.
    Doe not include errors loading the file, e.g. disk read failures or timeouts.
    """

    pass


class EModelValidation(ETrac):

    """
    Validation failure when a model is imported or loaded

    For model import failures, this means the define_* functions in the model class are not valid.
    For model load failures, it means the define_* functions do not match the model metadata.
    """

    pass


class ERuntimeValidation(ETrac):

    """
    Validation failure when a model calls into the TRAC API using the methods in TracContext
    """

    pass


class EDataValidation(ETrac):

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


class EUnexpected(ETrac):

    """
    An unexpected error has occurred in the TRAC runtime engine (this is a bug)

    This error always indicates a bug, it signals that the engine is in an inconsistent state.
    It should never be raised for errors that might legitimately occur during normal operation.
    The runtime will be shut down with a failed exit code, any running or pending jobs will also be failed.

    This error can be raised for failed sanity checks, where the engine should guarantee some condition
    and that condition has not been met.
    """

    pass
