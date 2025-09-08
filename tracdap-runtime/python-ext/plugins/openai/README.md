<h1 align="center">

![tracdap](https://github.com/finos/tracdap/raw/main/doc/_images/tracmmp_horizontal_400.png)

</h1>

<p align="center">
  <a href="https://pypi.org/project/tracdap-ext-openai"><img alt="PyPI Version" src="https://img.shields.io/pypi/v/tracdap-ext-openai.svg?maxAge=3600" /></a>
  <a href="https://pypi.org/project/tracdap-ext-openai"><img alt="Python Versions" src="https://img.shields.io/pypi/pyversions/tracdap-ext-openai.svg?maxAge=3600" /></a>
  <a href="https://github.com/finos/tracdap/actions/workflows/packaging.yaml?query=branch%3Amain"><img alt="Packaging status" src="https://github.com/finos/tracdap/actions/workflows/packaging.yaml/badge.svg?branch:main&workflow:CI" /></a>
  <a href="https://github.com/finos/tracdap/actions/workflows/compliance.yaml?query=branch%3Amain"><img alt="Compliance status" src="https://github.com/finos/tracdap/actions/workflows/compliance.yaml/badge.svg?branch:main&workflow:CI" /></a>
  <a href="https://community.finos.org/docs/governance/software-projects/stages/incubating/"><img alt="FINOS - Incubating" src="https://cdn.jsdelivr.net/gh/finos/contrib-toolbox@master/images/badge-incubating.svg" /></a>
</p>


# OpenAI Extension for the TRAC Model Runtime

This extension makes the OpenAI Python SDK available to use from inside a TRAC model.

- Use the native OpenAI client classes directly in TRAC model code
- Connection settings managed by TRAC for both local and deployed models
- Supports both OpenAI and AzureOpenAI clients

Models that make external calls are not considered repeatable,
and will be flagged as not repeatable when they run on the TRAC platform.

This extension is a pre-release and will be finalized inTRAC 0.10.


## Installing

The OpenAI extension can be installed with [pip](https://pip.pypa.io):

```shell
$ pip install tracdap-ext-openai
```

The package has the following dependencies:

- tracdap-runtime (version 0.10.0-beta1 or later)
- openai (version 1.x)


## Using the OpenAI client

Here is a minimum working example of a TRAC model using the OpenAI client:

```python
import tracdap.rt.api as trac
import openai

class OpenAIModel(trac.TracModel):

    # ... define parameters, inputs and outputs

    def define_resources(self):

        return {
            "openai": trac.define_external_system("openai", openai.OpenAI),
        }

    def run_model(self, ctx: trac.TracContext):

        with ctx.get_external_system("openai", openai.OpenAI) as client:

            response = client.responses.create(
                model="gpt-4o",
                instructions="You are a coding assistant that talks like a pirate.",
                input="How do I check if a Python object is an instance of a class?",
            )

            ctx.log.info(response.output_text)

if __name__ == '__main__':
    import tracdap.rt.launch as launch
    launch.launch_model(OpenAIModel, "config/job_config.yaml", "config/sys_config.yaml")
```

To make this example work, you will need to add ``openai`` as a resource in the system config file:

```yaml
resources:

  openai:
    resourceType: EXTERNAL_SYSTEM
    protocol: openai
```

The client can be customized by setting additional properties on the resource,
which are passed through to the OpenAI client.

```yaml
resources:

  openai:
    resourceType: EXTERNAL_SYSTEM
    protocol: openai
    properties:
      project: proj_xxxxxxxxxxxxx
```

The following configuration properties are supported:

- api_key, string, required
- organization, string, optional
- project, string, optional
- base_url, string, default = https://api.openai.com/v1/
- timeout, float, defeault = openai.DEFAULT_TIMEOUT.read (currently 600 seconds)
- max_retries, int, default = openai.DEFAULT_MAX_RETRIES (currently 2)

The ``api_key`` should not be put into a config file in plain text,
for local development it is recommended to set the OPENAI_API_KEY environment variable instead.
If both the config property and the environment variable are set, the config property takes precedence.


## Using the AzureOpenAI client

Here is a minimum working example of a TRAC model using the AzureOpenAI client.
This assumes the required resources and deployments have been set up in Azure.

```python
import tracdap.rt.api as trac
import openai

class TestModel(trac.TracModel):

    # ... define parameters, inputs and outputs

    def define_resources(self):

        return {
            "openai_azure": trac.define_external_system("openai", openai.AzureOpenAI)
        }

    def run_model(self, ctx: trac.TracContext):

        with ctx.get_external_system("openai_azure", openai.AzureOpenAI) as client:

            completion = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    { "role": "system", "content": "You are a coding assistant that talks like a pirate."},
                    { "role": "user", "content": "How do I check if a Python object is an instance of a class?" },
                ]
            )

            ctx.log.info(completion.choices[0].message.content)

if __name__ == '__main__':
    import tracdap.rt.launch as launch
    launch.launch_model(TestModel, "config/job_config.yaml", "config/sys_config.yaml")
```

To make this example work, you will need to add ``openai_azure`` as a resource in the system config file:

```yaml
resources:

  openai_azure:
    resourceType: EXTERNAL_SYSTEM
    protocol: openai
    subProtocol: azure
    properties:
      api_version: 2025-04-01-preview
      azure_endpoint: https://my-azure-endpoint.cognitiveservices.azure.com/
```

Setting ``supProtcol: azure`` is required for to create an Azure client.
The ``api_version`` and ``azure_endpoint`` properties must be specified,
and ``model`` parameter in the client call must refer to live model deployment on that endpoint.

All the configuration properties supported by the regular client are also supported by the Azure client.
Additionally, the Azure client supports these extra properties:

- api_version, string, required
- azure_endpoint, string, required
- azure_deployment, string, optional
- azure_ad_token, string, optional

For he Azure client, if ``api_key`` is not specified in the config file
it is read from the environment variable ``AZURE_OPENAI_API_KEY``.
Similarly, ``azure_ad_token`` can be read from the environment variable ``AZURE_OPENAI_AD_TOKEN``.
If both the config property and the environment variable are set, the config property takes precedence.
