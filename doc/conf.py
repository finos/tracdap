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


# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import pathlib
import subprocess as sp
import os
import re

ROOT_DIR = pathlib.Path(__file__) \
    .parent \
    .joinpath("..") \
    .resolve()

ON_RTD = os.environ.get('READTHEDOCS') == 'True'


if ON_RTD:

    def run_main_codegen():

        print("Running codegen for main doc target...")

        docgen_script_path = ROOT_DIR.joinpath("dev/docgen/docgen-ctrl.py")
        sp.run(["python", str(docgen_script_path), "main_codegen"])


    def config_init_hook(app, config):  # noqa

        print("Running Sphinx config hook...")
        run_main_codegen()


    def setup(app):
        app.connect('config-inited', config_init_hook)


def rtd_get_version_and_release():

    rtd_raw_version = os.environ["READTHEDOCS_VERSION"]

    if rtd_raw_version == "latest":
        return rtd_raw_version, "(dev latest)"

    version_tag_pattern = re.compile(r"v(\d.*)")
    version_tag_match = version_tag_pattern.match(rtd_raw_version)

    if not version_tag_match:
        raise RuntimeError("Unexpected version format")

    full_release_number = version_tag_match.group(1)
    version_ = re.sub(r"[+-].+$", "", full_release_number)
    release_ = re.sub(r"\+.+$", " + DEV", full_release_number)

    return version_, release_


# -- Project information -----------------------------------------------------

project = 'TRAC'
copyright = '2021, Accenture'  # noqa
author = 'Martin Traverse'

if ON_RTD:
    v_, r_ = rtd_get_version_and_release()
    version = v_
    release = r_

else:
    version = "{DOCGEN_VERSION}"
    release = "{DOCGEN_RELEASE}"

# This gets displayed as the root of the navigation path in the header / footer
short_title = f"{project} {release}"


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'autoapi.extension',
    
    'sphinxcontrib.fulltoc',
    'cloud_sptheme.ext.relbar_links'

]

autosectionlabel_prefix_document = True

autoapi_dirs = [
    '../build/doc/code/platform_api'
]

autodoc_typehints = 'description'
autoapi_member_order = 'groupwise'
autoapi_add_toctree_entry = False

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = [
    'modelling/*/*',
    '_templates'
]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = 'cloud'

master_doc = 'contents'
index_doc = 'index'

relbar_links = [("contents", "contents")]

html_theme_options = {
    'max_width': '80%',
    'externalrefs': False
}

html_sidebars = {'**': ['globaltoc.html', 'searchbox.html']}

html_show_sphinx = False

html_context = {
    'shorttitle': short_title
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
# html_static_path = ['_static']
