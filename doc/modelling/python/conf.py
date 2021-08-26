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


import sphinx_rtd_theme
import os

import importlib.util
docgen_spec = importlib.util.spec_from_file_location("docgen", "../../../dev/docgen/docgen-ctrl.py")
docgen_module = importlib.util.module_from_spec(docgen_spec)
docgen_spec.loader.exec_module(docgen_module)
docgen = docgen_module.DocGen()  # noqa

ON_RTD = os.environ.get('READTHEDOCS') == 'True'


# -- Project information -----------------------------------------------------

project = 'TRAC Runtime (Python)'
copyright = '2021, Accenture'  # noqa
author = 'Martin Traverse'

v_, r_ = docgen.get_version_and_release()
version = v_
release = r_


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.napoleon',
    'autoapi.extension',
    'sphinx_rtd_theme'
]

autoapi_dirs = [
    '../../../build/doc/code/runtime_python'
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

# autoapi_root = '.'
autoapi_keep_files = False
autodoc_typehints = 'description'

master_doc = 'contents'
index_doc = 'index'

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.

html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']


if ON_RTD:

    html_js_files = [
        ('https://assets.readthedocs.org/static/javascript/readthedocs-doc-embed.js', {'async': 'async'})]

    html_css_files = [
        "https://assets.readthedocs.org/static/css/readthedocs-doc-embed.css"]
