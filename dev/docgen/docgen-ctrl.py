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

import pathlib
import logging
import inspect
import argparse
import subprocess as sp
import platform
import shutil
import os
import fileinput
import re


SCRIPT_DIR = pathlib.Path(__file__) \
    .parent \
    .absolute() \
    .resolve()

ROOT_DIR = SCRIPT_DIR\
    .joinpath("../..") \
    .resolve()

CODEGEN_SCRIPT = ROOT_DIR \
    .joinpath('dev/codegen/protoc-ctrl.py')

DOC_DIR = ROOT_DIR \
    .joinpath("doc")

BUILD_DIR = ROOT_DIR. \
    joinpath('build/doc')


class DocGen:

    """

    Control the build of documentation, including API auto documentation and content held in doc/.

    This script is intended as a master control script for building documentation, running the targets
    "clean all dist" will produce a full, clean build of the documentation under build/doc/main/html.

    ReadTheDocs (RTD) uses an automated build system that starts by invoking Sphinx. So, the conf.py
    file for the main Sphinx project includes calls back into this script when running on RTD. Calls
    are made to the individual targets, excluding main_sphinx which is already being executed by RTD.

    """

    def __init__(self):

        logging_format = f"%(asctime)s %(levelname)s %(name)s - %(message)s"
        logging.basicConfig(format=logging_format, level=logging.INFO)

        self._log = logging.getLogger(self.__class__.__name__)

        self._build_dir = BUILD_DIR

    def set_build_dir(self, build_dir: pathlib.Path):
        self._build_dir = build_dir

    def clean(self):

        self._log_target()

        self._rm_tree(self._build_dir)

    def all(self):

        self._log_target()

        self.python_runtime_codegen()
        self.main_codegen()
        self.main_sphinx()

    def main(self):

        self.main_codegen()
        self.main_sphinx()

    def main_codegen(self):

        # Include generation of the core platform API (i.e. everything defined in the API .proto files)
        # Do not include runtime packages, APIs in other languages or indexing of implementation code

        self._log_target()

        codegen_exe = "python"
        codegen_args = [
            str(CODEGEN_SCRIPT), "api_doc",
            "--proto_path", "tracdap-api/tracdap-services/src/main/proto",
            "--proto_path", "tracdap-api/tracdap-metadata/src/main/proto",
            "--out", f"{self._build_dir}/code/platform_api",
            "--package", "tracdap",
            "--no-internal"]

        self._run_subprocess(codegen_exe, codegen_args)

    def main_sphinx(self):

        # Assume main_codegen has already been run, either by docgen or by sphinx

        self._log_target()

        sphinx_exe = 'sphinx-build'
        sphinx_src = DOC_DIR
        sphinx_dst = self._build_dir.joinpath('main').resolve()
        sphinx_args = ['-M', 'html', f'{sphinx_src}', f'{sphinx_dst}']

        self._mkdir(sphinx_dst)
        self._run_subprocess(sphinx_exe, sphinx_args)

    def python_runtime_codegen(self):

        self._log_target()

        runtime_src = ROOT_DIR.joinpath('tracdap-runtime/python/src')
        doc_src = self._build_dir.joinpath("code/runtime_python")

        # Set up the tracdap.rt package
        self._mkdir(doc_src.joinpath("tracdap"))
        self._mkdir(doc_src.joinpath("tracdap/rt"))

        # Copy only API packages / modules from the runtime library
        api_modules = [
            "tracdap/rt/__init__.py",
            "tracdap/rt/_version.py",
            "tracdap/rt/api/",
            "tracdap/rt/launch/",
            "tracdap/rt/exceptions.py"]

        for module in api_modules:

            src_module = runtime_src.joinpath(module)
            tgt_module = doc_src.joinpath(module)

            if src_module.is_dir():
                self._cp_tree(src_module, tgt_module)
            else:
                self._cp(src_module, tgt_module)

        # We include the runtime metadata classes at global scope in the api package for convenience
        # Having them show up in both places in the docs is confusing (for users, and the autoapi tool)!
        # So, remove those imports from the API package before running Sphinx

        # This can probably be improved by setting __all__ instead

        self._log.info("* fix docgen imports")

        fix_import_modules = [
            'tracdap/rt/api/__init__.py',
            'tracdap/rt/api/model_api.py',
            'tracdap/rt/api/static_api.py'
        ]

        for module in fix_import_modules:
            for line in fileinput.input(doc_src.joinpath(module), inplace=True):
                if "DOCGEN_REMOVE" not in line:
                    print(line, end="")

        # We also want the runtime metadata
        # There is a separate mechanism for generating this and getting it into the library for packaging
        # For doc gen, we generate a new copy and just move it to the right place
        codegen_exe = "python"
        codegen_args = [
            str(CODEGEN_SCRIPT), "python_doc",
            "--proto_path", "tracdap-api/tracdap-metadata/src/main/proto",
            "--out", f"{self._build_dir}/code/runtime_python"]

        self._run_subprocess(codegen_exe, codegen_args)
        self._mv(doc_src.joinpath('tracdap/metadata.py'), doc_src.joinpath('tracdap/rt/metadata.py'))

    def set_version_and_release(self, version, release):
        setattr(self, "_version", version)
        setattr(self, "_release", release)

    def get_version_and_release(self):

        self._log_target()

        if hasattr(self, "_version") and hasattr(self, "_release"):
            return getattr(self, "_version"), getattr(self, "_release")

        self._log.info(f"Looking up version / release info...")

        if platform.system().lower().startswith("win"):

            version_script = ROOT_DIR.joinpath("dev/version.ps1")

            version_exe = "powershell.exe"
            version_args = [
                "-ExecutionPolicy", "Bypass",
                "-File", str(version_script)]

        else:

            version_exe = ROOT_DIR.joinpath("dev/version.sh")
            version_args = []

        version_result = self._run_subprocess(version_exe, version_args, capture_output=True, use_venv=False)
        version_output = version_result.stdout.decode("utf-8").strip()
        version = re.sub(r"(\d+\.\d+).*$", r"\1", version_output)
        release = re.sub(r"([^+]+)\+.+$", r"\1 (dev)", version_output)

        self._log.info(f"TRAC D.A.P. version: {version}")
        self._log.info(f"TRAC D.A.P. release: {release}")

        setattr(self, "_version", version)
        setattr(self, "_release", release)

        return version, release

    def _log_target(self):

        target_frame = inspect.stack()[1]  # Calling function is frame index 1
        target_name = target_frame.function

        self._log.info(f"Building target [{target_name}]")

    def _touch(self, tgt):

        self._log.info(f'* touch {tgt}')

        tgt_path = pathlib.Path(tgt)
        if not tgt_path.exists():
            tgt_path.write_text("")

    def _mkdir(self, target_dir):

        self._log.info(f'* mkdir {target_dir}')
        target_dir.mkdir(parents=True, exist_ok=True)

    def _cp(self, src, tgt):

        self._log.info(f"* cp {src} {tgt}")

        shutil.copy2(src, tgt)

    def _cp_tree(self, source_dir, target_dir):

        self._log.info(f"* cp -R {source_dir} {target_dir}")

        shutil.copytree(source_dir, target_dir, dirs_exist_ok=True)

    def _mv(self, src, tgt):

        self._log.info(f"* mv {src} {tgt}")

        shutil.move(src, tgt)

    def _rm_tree(self, target_dir):

        self._log.info(f"* rm -r {target_dir}")

        if target_dir.exists():
            shutil.rmtree(target_dir)

    def _run_subprocess(self, sp_exe, sp_args, use_venv=True, capture_output=False):

        self._log.info(f'* {sp_exe} {" ".join(sp_args)}')

        # Binaries in the virtual env bin dir are not automatically available to sp.run (Windows 10, Python 3.9)
        # So, build the full path to the binary instead

        if not use_venv or 'VIRTUAL_ENV' not in os.environ:

            # Try to find sp_exe in the environment PATH
            # Python will not do this automatically!!

            path_env = os.environ["PATH"]
            path_split = ";" if platform.system().lower().startswith("win") else ":"
            path_dirs = path_env.split(path_split)

            exe_paths = map(lambda p: pathlib.Path(p).joinpath(sp_exe), path_dirs)
            exe_ = next(filter(lambda p: p.exists(), exe_paths))

        elif platform.system().lower().startswith("win"):

            exe_ = pathlib.Path(os.environ['VIRTUAL_ENV'])\
                .joinpath('Scripts')\
                .joinpath(sp_exe)\
                .with_suffix(".exe")

        else:

            exe_ = pathlib.Path(os.environ['VIRTUAL_ENV']) \
                .joinpath('bin') \
                .joinpath(sp_exe)

        # On Linux/macOS, arg 0 is the name given to the process and is not actually passed to it!
        # On winds arg 0 is the path to the binary

        if platform.system().lower().startswith("win"):
            args_ = [str(exe_)] + sp_args
        else:
            args_ = [sp_exe] + sp_args

        # Ready to run!
        result = sp.run(executable=exe_, args=args_, stdout=sp.PIPE, cwd=ROOT_DIR)

        if result.returncode != 0:
            err = f"{sp_exe} failed with exit code {result.returncode}"
            self._log.error(err)
            raise sp.SubprocessError(err)

        return result


def _find_targets(doc_gen: DocGen):

    methods = inspect.getmembers(doc_gen, predicate=inspect.ismethod)
    targets = {fn: f for (fn, f) in methods if not fn.startswith("_")}

    return targets


if __name__ == "__main__":

    _docgen = DocGen()
    _targets = _find_targets(_docgen)

    parser = argparse.ArgumentParser(description='Documentation generator')
    parser.add_argument(
        'targets', type=str, metavar="target", choices=list(_targets.keys()), nargs='+',
        help='The documentation targets to build')
    parser.add_argument(
        "--build-dir", type=pathlib.Path, metavar="build_dir", default=None,
        help="Local of docgen build outputs")

    args = parser.parse_args()

    if args.build_dir is not None:
        _docgen.set_build_dir(args.build_dir)

    for target in args.targets:
        _targets[target].__call__()
