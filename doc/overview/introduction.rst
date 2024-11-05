Introduction
====================

TRAC is a universal model orchestration solution that combines your existing data and compute infrastructure,
model development environments and the repository of versioned code, to create a single ecosystem in
which to build and deploy models, orchestrate complex workflows and run analytics.

The platform is built around three key principles, selected to break the trade-off that has traditionally
been required, between flexible (but uncontrolled) analytics solutions and highly controlled (but
inflexible) production platforms.

.. list-table::
    :widths: 30 40 200

    * - |icon-sufficient|
      - **SUFFICIENT**
      - The same infrastructure, tools and business assets support both production and experimental model runs, and post-run analytics. TRAC therefore supports all possible uses of a model and no other deployment environments are required.

    * - |icon-corrupt|
      - **INCORRUPTIBLE**
      - The platform's design makes it impossible to accidentally damage or destroy deployed data, models or flows. Model developers and users can therefore self-serve with confidence, free from the constraints of traditional change control processes.

    * - |icon-self-doc|
      - **SELF-DOCUMENTING**
      - TRAC automatically generates governance-ready documentation with no manual input required, eliminating the need to manually compile paper evidence for model deployment oversight, data lineage reporting and internal audit.

Because TRAC is sufficient, incorruptible and self-documenting you get the best of both worlds. Maximal
control and transparency plus analytical flexibility, in a single solution.

.. |icon-sufficient| image:: /_images/icon_sufficient.png
   :width: 85px
   :height: 85px

.. |icon-corrupt| image:: /_images/icon_corrupt.png
   :width: 85px
   :height: 85px

.. |icon-self-doc| image:: /_images/icon_self_doc.png
   :width: 85px
   :height: 85px




TRAC Metadata Model
====================

Structural Model
~~~~~~

TRAC is built around a structural metadata model which catalogues, describes and controls almost everything that happens on the platform. The model consists of two layers.

.. list-table::
    :widths: 25 200

    * - **OBJECTS**
      - Objects are the model’s structural elements. Data, models and jobs are all described by metadata objects. Each type of object has a metadata structure that is
        defined as part of the TRAC API.

    * - **TAGS**
      - Tags are used to index, describe and control objects, they are made up of key-value attributes.
        Some attributes are controlled by the platform, others can be set by client applications or
        edited by users.

Primary Object Types
~~~~~~~~
All model orchestration and analytics use cases can be understood in reference to four primary object types. They are not the
the only types of object on TRAC but they are the most common and important.

.. list-table::
    :widths: 25 25 65 100
    :header-rows: 1

    * - OBJECT
      - TYPE
      - EXTERNAL REFERENCE
      - OBJECT COMPONENTS
    * - |icon-data|
      - **DATA**
      - Collections of documents and records which have been imported into a TRAC-controlled Data Store
      - A structural representation of the data schema plus information about its physical storage
    * - |icon-model|
      - **MODEL**
      - Discrete units of code stored in a repository, which are exposed to TRAC via the model import process
      - A structural representation of the model schema plus reference to immutable model code or a binary package (e.g. in Git or Nexus)
    * - |icon-flow|
      - **FLOW**
      - NA - flows exists only as TRAC metadata objects
      - A calculation graph where inputs, outputs and models are the nodes and edges represent data flow
    * - |icon-job|
      - **JOB**
      - A process or calculation orchestrated by TRAC which may use one or more external system or resource. There are five job types; ImportModel, ImportData, RunModel, RunFlow and ExportData
      - The detail varies by job type but it will map the job reference to the objects used as inputs and those generated as outputs. For a RunFlow job this includes the flow plus models, data and parameters used as inputs and the datasets the job generated.



.. |icon-data| image:: /_images/icon_data.png
   :width: 85px
   :height: 85px

.. |icon-model| image:: /_images/icon_model.png
   :width: 85px
   :height: 85px

.. |icon-flow| image:: /_images/icon_flow.png
   :width: 85px
   :height: 85px

.. |icon-job| image:: /_images/icon_job.png
   :width: 85px
   :height: 85px


Versioning
~~~~~~~~

Metadata records are maintained using an immutable, time-indexed version history, with "updates" being
performed by creating a new version of the object or tag. TRAC metadata therefore provides a fully
consistent historical view of the platform and a complete audit history that is both machine and human
readable.

Where metadata objects refer to external resources such as models and data, those resources are
also immutable. This is achieved using e.g. GitHub tags or Nexus binary versions for models, and data
areas owned by TRAC with controlled write access for primary data.


Virtual Deployment Framework
====================

Self-describing Models
~~~~~~~~
Models can be imported and used with zero code modifications or platform-level interventions, so long as
the model code contains a custom function which declares the model's schema to the platform. A model schema
consists of:

* The schema of any data inputs the model needs to run

* The schema of any optional or required parameters which affect how the model runs

* The schema of the output data which the model produces when it runs


Model Deployment Process
~~~~~~~~

TRAC uses a 'virtual' model deployment framework, in which model code remains in an external repository
and is accessed at runtime. There are three main processes involved in this framework and TRAC performs
validations at each of the steps. These validations replace the traditional route-to-live process and
allow models to be deployed and used without platform-level interventions or code changes.

.. list-table::
    :widths: 35 40 140 70
    :header-rows: 1

    * - OBJECT
      - PROCESS
      - SUMMARY
      - RTL VALIDATION

    * - |icon-model|
      - **IMPORT MODELS**
      - Importing a model creates an object in the TRAC metadata store which refers to and describes the model. This record includes the model schema. The model is not deployed (in the traditional, physical sense) because the code remains in the repository.
      - Does the model code contain a properly constructed function declaring its schema?

    * - |icon-flow|
      - **BUILD FLOW**
      - Flows can be built and validated on the platform using only the schema representations of the models. Flows exist only as metadata objects, so a flow is like a ‘virtual’ deployment of some models into an execution process.
      - Is the model schema compatible with it's proposed placing in the calculation graph?

    * - |icon-job|
      - **RUN JOBS**
      - For a RunFlow job you first pick a flow and the. select the data and model objects to use for each node, plus any required parameters. TRAC then fetches the model code and the data records from storage and orchestrates the calculations as a single job.
      - Does the model code generate outputs which are consistent with the declared schema?


In addition to these steps, the TRAC Runtime can be deployed to your IDE of choice,
giving you all the type safety of production and ensuring that models translate to production without
modification. Any model which executes via the TRAC Runtime service in the IDE with local data inputs
will run on the platform.


TRAC Guarantee
====================

TRAC offers a unique control environment which is characterised by three guarantees.

.. list-table::
    :widths: 30 30 200

    * - |icon-audit|
      - **AUDITABLE ACTIONS**
      - Any action that changes a tag or creates an object is recorded in a time-consistent fashion in the
        metadata model. The metadata is designed to be easily understood by humans and machines and
        standard report formats can be used to create governance-ready documentation with no manual input
        required.

    * - |icon-repeat|
      - **REPEATABLE JOBS**
      - Any RunModel or RunFlow job can be re-resubmitted and because the inputs are immutable you will
        get the same result, guaranteed. We account for multiple factors that cause non-deterministic
        model output: threading (don't use it!), random number generation, time, external calls and
        dynamic execution (these are disabled), language and library versions (these are recorded
        with the metadata).

    * - |icon-persist|
      - **RISK FREE PLATFORM**
      - Every version of every object (model, data, flow) remains permanently available to use and there is
        no possibility of accidental loss or damage to deployed assets. Therefore, there is no change risk
        (as traditionally defined) on TRAC.

.. |icon-audit| image:: /_images/icon_audit.png
   :width: 85px
   :height: 85px

.. |icon-repeat| image:: /_images/icon_repeat.png
   :width: 85px
   :height: 85px

.. |icon-persist| image:: /_images/icon_persist.png
   :width: 85px
   :height: 85px

.. note::
    The repeatability guarantee applies to RunModel, RunFlow and ExportData jobs. A model cannot be
    imported twice so an ImportModel job cannot be repeated. An ImportData job can be repeated but
    due to the dependence on an external source, TRAC cannot guarantee that the same outputs will be produced.


Experimentation & Analytics
====================

In addition to supporting highly-controlled (or 'production') model execution processes, TRAC also provide two main ways to
construct 'experimental' model runs.

.. list-table::
    :widths: 40 200

    * - **EXPERIMENTAL FLOWS**
      - Separate flows can be created for any standardised analytic process, from sensitivity analysis
        to periodic model monitoring. Under the virtual deployment framework, Jobs which use
        these experimental flows are safely executed on production data and infrastructure.

    * - **EXPERIMENTAL INPUTS**
      - Using a 'production' flow, alternate model versions, data inputs
        or parameter values can be selected. For quick and simple what-if analysis, old
        jobs can be loaded, edited and resubmitted, for example to run last year's models with
        this year's data, or vice versa

TRAC can execute as many parallel jobs as the underlying compute infrastructure will allow and because they
are isolated and stateless, multiple runs can use different versions of the same model or dataset
concurrently. This greatly reduces the time required to complete more complex comparative analytics.