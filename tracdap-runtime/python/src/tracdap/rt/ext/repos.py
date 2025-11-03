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
import dataclasses as _dc
import pathlib as _pathlib


@_dc.dataclass(kw_only=True)
class ModelPackage:

    language: str = ""
    repository: str = ""
    packageGroup: str | None = None
    package: str = ""
    version: str = ""
    path: str | None = None


class IModelRepository(metaclass=_abc.ABCMeta):

    """
    Extension interface for model repositories.

    Model repositories are long-lived resources that are configured in the runtime system config
    (normally sys_config.yaml). The runtime framework instantiates all the configured repositories
    on startup and they are available for the lifetime of the process to check out model code when needed.
    TRAC checks out all the models needed for a particular job before that job starts executing, there is
    no way for model code to directly call into a model repository or trigger a checkout.

    Some types of repository are configured with a separate repository for each package. E.g. this layout
    is common for source packages in Git, where each repository is likely to have its own access controls.
    Other types are configured with a single repository which hosts multiple packages, which is common e.g.
    for enterprise proxy package servers. In this case, the package information is taken from the package
    parameter to the :py:meth:checkout()` method.
    """

    @_abc.abstractmethod
    def checkout(self, package: ModelPackage, checkout_dir: _pathlib.Path) -> _pathlib.Path:

        """
        Perform a checkout for the given package, into the supplied checkout dir.

        The checkout dir will be empty before this method is called.

        The return value is the path given to the module loader, to load the model.
        I.e. it should contain the root packages / modules that the model will load.

        Repo implementations are free to put packages directly into checkout_dir,
        in which case the return value should be checkout_dir. It may be helpful to
        create a folder structure inside checkout_dir, e.g. to separate temporary files,
        in which case the return value will be a subfolder of checkout_dir containing
        just the model packages to be loaded. Source repositories are likely to have a
        folder structure in the model repo itself, for example in a source repository where
        the root source folder is "src", the return value might be <checkout_dir>/src.

        :param package: The model package to check out
        :param checkout_dir: Empty directory into which the checkout will be performed
        :return: Path for the root packages / modules that the model will load
        """

        pass

    @_abc.abstractmethod
    def get_checkout_path(self, package: ModelPackage, checkout_dir: _pathlib.Path) -> _pathlib.Path:

        """
        Get the checkout path that would be returned by :py:meth:`checkout()`,
        for the given package and checkout_dir, without doing a checkout.

        This is used by the loader mechanism to avoid repeating the same checkout multiple times.
        E.g. if there are multiple models or resources being loaded from the same package,
        or if a model is referenced multiple times.

        This method is always required and may be called before :py:meth:`checkout()`.
        The return value should be deterministic for a given package and checkout_dir.

        :param package: Model package for which the package path is requested
        :param checkout_dir: Directory where the model is (or will be) checked out
        :return: Path for the root packages / modules that the model will load
        """

        pass
