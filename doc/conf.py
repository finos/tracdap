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
import subprocess as sp
import os

import importlib.util
docgen_spec = importlib.util.spec_from_file_location("docgen", "../dev/docgen/docgen-ctrl.py")
docgen_module = importlib.util.module_from_spec(docgen_spec)
docgen_spec.loader.exec_module(docgen_module)
docgen = docgen_module.DocGen()  # noqa

ON_RTD = os.environ.get('READTHEDOCS') == 'True'

ROOT_DIR = pathlib.Path(__file__) \
    .parent \
    .joinpath("..") \
    .resolve()

DOCGEN_SCRIPT = str(ROOT_DIR.joinpath("dev/docgen/docgen-ctrl.py"))


# Running on RTD, Sphinx build for the root docs is the main entry point
# So, we need to call back into docgen to build all the other parts of the documentation

if ON_RTD:

    def build_dependencies():

        docgen.main_codegen()
        docgen.python_runtime_codegen()

    def config_init_hook(app, config):  # noqa

        build_dependencies()

    def setup(app):
        app.connect('config-inited', config_init_hook)


# -- Project information -----------------------------------------------------

project = 'TRAC D.A.P.'
copyright = '2024, finTRAC Limited'  # noqa
author = 'finTRAC Limited'

# ReadTheDocs does not fetch tags by default and we need them to get version information
# Only needed in the root config
if ON_RTD:
    sp.run(["git", "fetch", "--tags"])

v_, r_ = docgen.get_version_and_release()
version = v_
release = r_


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
    
    'sphinx_design',
    "sphinx_wagtail_theme"

]


# Auto API configuration

autoapi_dirs = [
    '../build/doc/code/platform_api',
    '../build/doc/code/runtime_python'
]

autoapi_options = [
    'members',
    'undoc-members',
    # 'private-members',
    'show-inheritance',
    'show-module-summary',
    'special-members',
    'imported-members'
]

autodoc_typehints = 'description'
autoapi_member_order = 'groupwise'
autoapi_add_toctree_entry = False


# Content generation

header_links = [
    "GitHub|https://github.com/finos/tracdap",
    "fintrac.co.uk|https://fintrac.co.uk/"
]

footer_links = [
    "Website|https://fintrac.co.uk/",
    "Contact|https://fintrac.co.uk/contact",
    "Legal|https://fintrac.co.uk/legal",
]

autosectionlabel_prefix_document = True

rst_prolog = """
.. meta::
   :theme-color: #1D3D59
"""



# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = "sphinx_wagtail_theme"
html_css_files = ["tracdap.css"]

html_theme_options = dict(
    project_name = f"TRAC D.A.P. Documentation (Version {version})",
    logo = "tracdap_logo.svg",
    logo_alt = "TRAC D.A.P. Logo",
    logo_height = 59,
    logo_url = "/",
    logo_width = 45,

    github_url = None,
    header_links = ",".join(header_links),
    footer_links = ",".join(footer_links)
)

html_copy_source = False
html_show_sourcelink = False

html_favicon = "_static/tracdap_favicon.ico"
html_static_path = ["_static"]
