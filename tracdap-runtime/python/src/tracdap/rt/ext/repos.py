#  Copyright 2022 Accenture Global Solutions Limited
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

import abc
import pathlib
import typing as tp

import tracdap.rt.metadata as _meta


class IModelRepository:

    @abc.abstractmethod
    def checkout_key(self, model_def: _meta.ModelDefinition) -> str:

        """
        A unique key identifying the checkout required for the given model definition.

        For example, in Git repositories the checkout key might be a commit hash or tag.
        For binary packages in Nexus, the checkout key might involve both package and version.
        Other repositories might need to use the path as well.

        This key is used to avoid duplicate checkouts. So for example, if several models exist in
        the same version of a package, returning the same key will mean the checkout is only performed once.

        :param model_def: Model to request a checkout key for
        :return: Checkout key for the given model
        """

        pass

    @abc.abstractmethod
    def package_path(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: pathlib.Path) \
            -> tp.Optional[pathlib.Path]:

        """
        Get the root directory for package loading, assuming the model is checked out in the supplied directory.
        The package directory will contain the Python package for the model described by the model definition,
        it can be used as a package root (Python source root) by the TRAC model loading mechanism.

        For example, in Git repositories this will be the path from the model definition, relative to the checkout dir.

        :param model_def: Model for which the package path is requested
        :param checkout_dir: Directory where the model is checked out (checkout must have happened previously)
        :return: A package root directory, containing the Python package of the requested model
        """

        pass

    @abc.abstractmethod
    def do_checkout(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: pathlib.Path) \
            -> tp.Optional[pathlib.Path]:

        """
        Perform a checkout for the given model definition, into the supplied checkout dir.

        The checkout dir must be empty before this method is called.
        The return value is the package path for the model, as per :py:meth:`package_path()`.

        :param model_def: The model to check out
        :param checkout_dir: Empty directory into which the checkout will be performed
        :return: The package path for the model, as per :py:meth:`package_path()`.
        """

        pass
