#  Licensed to the Fintech Open Source Foundation (FINOS) under one or
#  more contributor license agreements. See the NOTICE file distributed
#  with this work for additional information regarding copyright ownership.
#  FINOS licenses this file to you under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with the
#  License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import abc as _abc
import typing as _tp


class IExternalSystem(metaclass=_abc.ABCMeta):

    """
    Extension interface for external systems.

    TRAC will create one instance of an external system plugin for each external system
    resource in the system config. The resource configuration is received through the
    constructor. No clients are created when the plugin is loaded.

    To use an external system in a model, the model must define a compatible resource in
    :py:meth:`define_resources() <tracdap.rt.api.TracModel.define_resources>` and then at
    runtime call :py:meth:`get_external_system() <tracdap.rt.api.TracContext.get_external_system>`,
    which results in a call to :py:meth:`create_client() <create_client>` to create the client object.
    TRAC ensures that there is exactly one call to :py:meth:`close_client() <close_client>`
    for every client object. All the clients created by a single model are guaranteed to be closed
    when the model completes. Client objects are not shared between models.

    A single system can be used multiple times, so plugins should expect multiple calls to
    :py:meth:`create_client() <create_client>`. This can happen in 3 ways:

    * A single model can create multiple clients with the same protocol
    * Different models in the same flow can use the same external system
    * Multiple jobs can execute within a single instance of the TRAC runtime (e.g. using a job group)

    .. seealso::
        :py:meth:`define_resources() <tracdap.rt.api.TracModel.define_resources>`,
        :py:meth:`get_external_system() <tracdap.rt.api.TracContext.get_external_system>`
    """

    @_abc.abstractmethod
    def supported_types(self) -> _tp.List[type]:

        """
        Provide a list of the supported Python types this plugin can create.

        TRAC will only ask the plugin to create client objects for types that are in this list.
        Types must match exactly, models trying to create a client using a subtype or supertype
        of a supported type will get a runtime validation error.

        :return: The list of supported Python types this plugin can create
        :rtype: list[type]
        """

        pass

    @_abc.abstractmethod
    def supported_args(self) -> _tp.Optional[_tp.Dict[str, type]]:

        """
        Provide a list of supported arguments that can be passed to :py:meth:`create_client() <create_client>`.

        Any client args that are required must be defined by this method before they can be used in
        :py:meth:`create_client() <create_client>`. TRAC uses this information to perform runtime validation
        on arguments supplied from model code.

        Client args are restricted to simple types (str, bool, int etc.). Defining a client arg with an
        unsupported type will cause an error on startup. If no client args are required,
        this method can return ``None``.

        :return: An optional dictionary of supported arguments and their types
        :rtype: dict[str, type] | None
        """

        pass

    @_abc.abstractmethod
    def create_client(self, client_type: type, **client_args) -> _tp.Any:

        """
        Create a client object for the external system.

        Client type will always be one of the types returned by :py:meth:`supported_types() <supported_types>`.
        The configuration received in the plugin constructor should be sufficient to create a valid instance.

        Client args can be used for models to pass extra parameters to configure the client at runtime.
        It should be possible to create a client without any client args. Key details such as address
        and authentication should always be part of the configuration, but client args can be used to
        customize a particular client instance, for example by setting model parameters in a remote system.
        If client args are used, they must be defined by :py:meth:`supported_args() <supported_args>`,
        TRAC will reject any client args that are not defined or have the wrong type.

        The returned object must be an instance of the requested client type (which may be a subclass).
        If it is not TRAC will raise a validation error, which will result in a job failure.

        :param client_type: The type of client object to create
        :param client_args: Optional arguments that can be used when creating the client
        :return: A client object of the requested type

        :type client_type: type
        :rtype: Any
        """

        pass

    @_abc.abstractmethod
    def close_client(self, client: _tp.Any):

        """
        Close a client object for the external system.

        Only client objets that were created by :py:meth:`create_client() <create_client>` will be supplied
        and each client will be closed exactly once.

        :param client: The client object to close
        :type client: Any
        """

        pass
