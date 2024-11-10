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

import fileinput
import pathlib
import subprocess as sp
import os

import importlib.util
import sys

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

# Modules to document as a single package (do not report contents of submodules)
# Note tracdap.rt.metadata is already flat-packed during code gen, so do not flatten again here
FLATTEN_MODULES = ["tracdap.rt.api", 'tracdap.rt.launch']


# Running on RTD, Sphinx build for the root docs is the main entry point
# So, we need to call back into docgen to build all the other parts of the documentation

def setup(sphinx):

    if ON_RTD:
        sphinx.connect('config-inited', config_init_hook)

    sphinx.connect("autoapi-skip-member", skip_fattened_modules)
    sphinx.connect("build-finished", fix_module_references)


def config_init_hook(app, config):  # noqa

    build_dependencies()


def build_dependencies():

    docgen.main_codegen()
    docgen.python_runtime_codegen()


def skip_fattened_modules(app, what, name, obj, skip, options):

    if what == "module":
        for flat_module in FLATTEN_MODULES:
            if name.startswith(flat_module):
                skip = True

    return skip


def fix_module_references(sphinx, exception):

    if exception:
        return

    print("* Applying post-build fixes", file=sys.stderr)

    # Sphinx struggles with type resolution when the same type exists in different namespaces
    # The current doc build works for all the types except this one, which is nested and refers to an outer scope
    # No more nested types will be added to the metadata model!
    # If possible, the existing nested types should be migrated to a flat structure

    rt_data_part_page = pathlib.Path(sphinx.outdir).joinpath("autoapi/tracdap/rt/metadata/DataDefinition.Part.html")

    rt_data_part_match = 'href="../../metadata/PartKey.html#tracdap.metadata.PartKey" title="tracdap.metadata.PartKey"'
    rt_data_part_fixed = 'href="PartKey.html#tracdap.rt.metadata.PartKey" title="tracdap.rt.metadata.PartKey"'

    for line in fileinput.input(rt_data_part_page, inplace=True):
        if rt_data_part_match in line:
            print(line.replace(rt_data_part_match, rt_data_part_fixed), end="")
        else:
            print(line, end="")


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
    'autoapi.extension',
    
    'sphinx_design',
    "sphinx_wagtail_theme"
]

# Custom templates are being used for autoapi
templates_path = ["_templates"]

# Directories that should not be built into the TOC tree
exclude_patterns = ["_templates", "unused"]

# Auto API configuration

autoapi_dirs = [
    '../build/doc/code/platform_api',
    '../build/doc/code/runtime_python'
]

autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
    'imported-members'
]

autoapi_add_toctree_entry = False
autoapi_member_order = "groupwise"
autoapi_own_page_level = "class"
autoapi_template_dir = "_templates/autoapi"

autodoc_typehints = 'description'


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
