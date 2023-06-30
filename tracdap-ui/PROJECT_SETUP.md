# Typescript

This project is in the process of being converted to [Typescript](https://www.typescriptlang.org/). The project's
definitions are stored in src/types. The types for dependencies are listed in package.json where available e.g.
@types/react.

# TRAC

There are two TRAC related repositories needed to run the TRAC-UI repository locally. The first is
the [TRAC Web API](https://www.npmjs.com/package/trac-web-api)
which is required as a dependency to this project. The second is
the [TRAC repository](https://github.com/Accenture/trac), this contains the TRAC database and handles the API calls from
the browser. In most cases cloning the repo and following the set up documentation in its README.md will create a
working instance. If you are going to be suggesting changes to the TRAC repository then it should be forked before being
cloned.

To enable you to keep your forked repository in sync with the main Accenture branch the following git commands will add
a new remote to your project  (taken
from [here](https://stackoverflow.com/questions/3903817/pull-new-updates-from-original-github-repository-into-forked-github-repository))

    cd <PROJECT_NAME>  
    git remote add upstream https://github.com/Accenture/trac.git

To get updates from the upstream remote:

    git fetch upstream   
    git rebase upstream/main

Here `<PROJECT_NAME>` is the root folder for your local project where the fork was cloned into e.g. C:
\Users\greg.wiltshire\PycharmProjects\trac.

# Tenants

### Adding a tenant in the user interface

In TRAC all objects such as data, models and files are stored under a single tenant which can represent a division or a
team within a business. A tenant can not be created from the user interface, this is a back end task (see the TRAC
documentation).

Once a tenant has been created in TRAC this can be added to the user interface via the _Admin > Application setup_ page.
There is a dataset called 'Tenant options' that can be added to TRAC (it only needs to be created once per tenant and
the interface tells you if it already exists). Once created this dataset can be edited to add the name of the new
tenant. The newly created tenant is added in as the TENANT_ID variable with a display name added in TENANT_NAME. The
TENANT_ID is case-sensitive.

Note that a 'Tenant options' dataset needs to be created in each tenant.

The 'Tenant options' is used to create a list of tenant that the user can access, only one tenant can be loaded at a
time and when the tenant is changed all unsaved work in the application in the previous tenant is deleted. The list of
tenants is accessible in the settings menu accessible through the cog icon top right.

### Setting a default tenant

The application config _src/config/config.js_ contains a _defaultTenant_ field that is the ID that the user interface
will default to in the absence of any previous user setting. This will only work if the user has access to the default
tenant.

The user is free to change tenant using the settings menu accessible through the cog icon top right. Changing this sets
a browser cookie which replaces the default setting in the config.

In order to prevent users being presented with a list of tenants that they do not have access an API call is made to
TRAC when the application loads to get a list of the tenants that each user has access to via their profile. Tenants not
in the allowed list will still be visible (if in the list) to the user but not selectable.

# Typescript project

# Layout framework

This project is based upon React Boostrap (a package that wraps Bootstrap 5). 

# Icon package

The project uses Bootstrap icons for icons. These are rendered through the Icon component. More modern than free fontAwesome
icons. Where icons are not available then images in.

# When the page loads the OnLoad component runs several functions the save information in the application Store

# Sccs and custom css are used to set the css
# themes - light, dark and client

applicationStore is not reset when tenant changes except for data

all others are reset when tenant changes

the theme name sets the banner image by a class, defaults to light

client shading needs to be set in scc in client theme

GeneralChartConfig has image details, application eeds to know native dimensions and display options. The former is used to convert image in PDF exports
and the latter to display the imagesi n the TopBanner. Both a dark and a light background image need to be specified. Copy them if not both available.

global.d.ts sets models for png image imports see react-typescript-cheatsheet.netlify.app/docs/basic/troubleshooting/non_ts_files/

externalLinks in config has paths to resources such as models/cnfluence and other tooling. Currently assumes one each
icons not part of bootstrap icons so additonal icons will be needed - current ones downloaded from the free icon packs in fontawesome
https://fontawesome.com/icons/git-alt?s=brands

src/index.js
turn off strict mode in prod

there are value, option, toggle, date sleectors

config_trac_classificatins maps trac api to ui  e.g. lists of the dat types - maps names to basic ChartDefinitions too

business sgements 


If you add a tenant then you need to add a repository definition in the config to know what the model respositories that the upload the model needs to know about