Introduction
============

TRAC D.A.P. is an open-source model orchestration toolkit, maintained by Fintrac Limited in
association with `FINOS <https://www.finos.org/>`_. Designed to work alongside any modern analytics
stack, TRAC D.A.P. creates a highly controlled environment for running analytics.

This project contains:

* The TRAC platform services (metadata, data, orchestrator and gateway)
* The Python model runtime, also available to download and install from `pypi.org <https://pypi.org/>`_.

See `CONTRIBUTING.md <https://github.com/finos/tracdap/blob/main/CONTRIBUTING.md>`_ for the
project's governance and how to get involved.


Metadata-driven architecture
-----------------------------

TRAC D.A.P. is built around a single metadata model, which catalogues and describes every
asset and every traceable action on the platform. This is what makes it possible to enforce consistency
and produce an audit trail automatically, rather than as a separate, manually-maintained process.

See :doc:`metadata_model` for a full description including objects, tags, versioning and how to query them.


Self-describing models
-----------------------

Models are built using a standard API (see :doc:`/reference/model_api_python`) and can be deployed to the platform
without code change or manual deployment activity. The model schema (inputs, parameters and outputs)
is declared in code, which allows jobs to be compiled at runtime, rather than via mutable environment
configuration.

For more on writing and running models, see :doc:`/modelling/tutorial/index`.


Immutability and repeatability
-------------------------------

Model code stays in an external repository and is only fetched when a job runs. TRAC D.A.P. data service also
enforces an append-only data model.

Because the underlying objects (models and data) are immutable resubmitting a job will always produce the
same result, and old jobs can be loaded, tweaked and resubmitted, for example to run last year's models against
this year's data. Runs are isolated and stateless, so many jobs - using different versions of the same model or
dataset - can execute in parallel.
