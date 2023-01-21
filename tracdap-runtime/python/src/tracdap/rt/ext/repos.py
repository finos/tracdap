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

import abc as _abc
import pathlib as _pathlib

import tracdap.rt.metadata as _meta


class IModelRepository:

    @_abc.abstractmethod
    def do_checkout(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: _pathlib.Path) \
            -> _pathlib.Path:

        """
        Perform a checkout for the given model definition, into the supplied checkout dir.

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

        :param model_def: The model to check out
        :param checkout_dir: Empty directory into which the checkout will be performed
        :return: Path for the root packages / modules that the model will load
        """

        pass

    @_abc.abstractmethod
    def package_path(
            self, model_def: _meta.ModelDefinition,
            checkout_dir: _pathlib.Path) \
            -> _pathlib.Path:

        """
        Get the package path that would be returned by :py:meth:`do_checkout()`,
        for the given checkout_dir and model_dir, without doing a checkout.

        This is used by the loader mechanism to avoid repeating the same checkout multiple times.
        E.g. if there are multiple models or resources being loaded from the same package,
        or if a model is referenced multiple times.

        This method is always required and may be called before :py:meth:`do_checkout()`.
        The return value should be deterministic for a given model_def and checkout_dir.

        :param model_def: Model for which the package path is requested
        :param checkout_dir: Directory where the model is (or will be) checked out
        :return: Path for the root packages / modules that the model will load
        """

        pass
