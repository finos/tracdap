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

import pathlib
import logging
import inspect
import argparse
import subprocess as sp
import platform
import os


ROOT_DIR = pathlib.Path(__file__).parent \
    .joinpath("../..") \
    .absolute() \
    .resolve()

BUILD_DIR = ROOT_DIR \
    .joinpath('build/doc')


class DocGen:

    def __init__(self):

        logging_format = f"%(asctime)s %(levelname)s %(name)s - %(message)s"
        logging.basicConfig(format=logging_format, level=logging.INFO)

        self._log = logging.getLogger(self.__class__.__name__)

    def clean(self):

        self._log_target()
        self._log.info(f"* rm -r {BUILD_DIR}")

        def rm_tree(pth):
            if pth.exists():
                for child in pth.iterdir():
                    if child.is_file():
                        child.unlink()
                    else:
                        rm_tree(child)
                pth.rmdir()

        rm_tree(BUILD_DIR)

    def all(self):

        self._log_target()

        self.metadata()
        self.platform_api()
        self.runtime_python()
        self.metadata()

        self.main()

    def main(self):

        self._log_target()

        sphinx_exe = 'sphinx-build'
        sphinx_src = ROOT_DIR.joinpath('doc').resolve()
        sphinx_dst = BUILD_DIR.joinpath('main').resolve()
        sphinx_args = ['-M', 'html', f'{sphinx_src}', f'{sphinx_dst}', '-c', 'main']

        self._log.info(f'* mkdir {sphinx_dst}')
        sphinx_dst.mkdir(parents=True, exist_ok=True)

        self._log.info(f'* {sphinx_exe} {" ".join(sphinx_args)}')
        sphinx = self._run_subprocess(sphinx_exe, sphinx_args, use_venv=True)

        if sphinx.returncode != 0:
            raise sp.SubprocessError()

    def metadata(self):
        pass

    def platform_api(self):
        pass

    def runtime_python(self):
        pass

    def dist(self):
        pass

    def _log_target(self):

        target_frame = inspect.stack()[1]  # Calling function is frame index 1
        target_name = target_frame.function

        self._log.info(f"Building target [{target_name}]")

    @staticmethod
    def _run_subprocess(sp_exe, sp_args, use_venv=False):

        # Binaries in the virtual env bin dir are not automatically available to sp.run (Windows 10, Python 3.9)
        # So, build the full path to the binary instead

        if not use_venv or 'VIRTUAL_ENV' not in os.environ:
            exe_ = sp_exe

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
        return sp.run(executable=exe_, args=args_)


def _find_targets(doc_gen: DocGen):

    methods = inspect.getmembers(doc_gen, predicate=inspect.ismethod)
    targets = {fn: f for (fn, f) in methods if not fn.startswith("_")}

    return targets


_doc_gen = DocGen()
_targets = _find_targets(_doc_gen)

parser = argparse.ArgumentParser(description='Documentation generator')
parser.add_argument(
    'targets', type=str, metavar="target", choices=list(_targets.keys()), nargs='+',
    help='The documentation targets to build')

args = parser.parse_args()

for target in args.targets:
    _targets[target].__call__()
