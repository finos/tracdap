Key concepts


Metadata Model
====================

TRAC is built around a structural metadata model which catalogues and describes everything on the platform. The model consists of three layers:

.. list-table::
    :widths: 40 200

    * - **OBJECTS**
      - Objects are the model’s structural elements and each object type has its own metadata structure. The most

    * - **TAGS**
      - Tags are used to index, describe and control objects. Some tags are controlled by the platform, some you can set yourself.

    * - **TRACEABLE ACTIONS**
      - Traceable actions are actions that create objects, such as running jobs or data imports. Read-only
    actions such as querying data or metadata searches are not recorded in the metadata model.


Metadata records are maintained using an immutable, time-indexed version history, with "updates" being performed
by creating a new version of the object or tag with the required changes. Because of this, the TRAC metadata
provides a fully consistent historical view of the platform for any previous point in time. It also provides
a complete audit history that is both machine and human readable, with no manual effort.

Where objects refer to external resources such as models and data, those resources are also immutable.
This is achieved using e.g. GitHub tags or Nexus binary versions for models, and data areas owned by TRAC with
controlled write access for primary data. The combination of immutable metadata and immutable resources allows
TRAC to recreate any previous calculation that has run on the platform.


Objects
_______

All model orchestration use-cases involve four primary object types. The TRAC metadata model includes other object types, but these are the most common.

.. list-table::
    :widths: 40 40 100 100

    * - |icon-data|
      - **DATA**
      - Collections of documents and records which have been imported into a TRAC-controlled Data Store
      - Structural representation of the data schema, plus its physical storage location

    * - |icon-model|
      - **MODEL**
      - Discrete units of code stored in a repository and exposed to TRAC via the model upload process
      - The model schema (inputs, outputs and parameters) plus reference to immutable model code or a binary package (e.g. in Git or Nexus)

    * - |icon-flow|
      - **FLOW**
      - None, the flow is abstract and does not refer to specific data or models
      - A calculation involving multiple models represented as a graph where inputs, outputs and models are nodes and edges represent data flow

    * - |icon-job|
      - **JOB**
      - A process TRAC orchestrates. The five job types are; ImportModel, ImportData, RunModel, RunFlow and ExportData
      - The metadata record varies by job type but will record objects which were uses in the process.


.. |icon-data| image:: ../../_images/icons/icon_data.png
   :width: 66px
   :height: 66px

.. |icon-model| image:: ../../_images/icons/icon_model.png
   :width: 66px
   :height: 66px

.. |icon-flow| image:: ../../images/icons/icon_flow.png
   :width: 66px
   :height: 66px

.. |icon-job| image:: ../../images/icons/icon_job.png
   :width: 66px
   :height: 66px

.. note::
    Because Model and Data objects refer to and describe a persistent external asset which TRAC controls (model code & data records) these objects can also be called "Assets".



Virtual Deployment
----------
TRAC uses a ‘virtual model deployment' approach in which all model code resides in an external repository
until it is needed for a calculation, so th virtual deployment is therefore crystalised at runtime. There are three main steps involved in the virtual deployment approach.

.. list-table::
    :widths: 30 200

    * - **IMPORT MODELS**
      - Uploading a model creates a model object in the TRAC metadata store which includes a schema representation of the model. The model code remains in the repository.

    * - **BUILD FLOW**
      - Flows can be built and validated using the schema representation of the models. Because the flows themselves exist only as metadata object, we can describe a flow as being a ‘virtual’ deployment of the model into a complex execution process.

    * - **RUN JOBS**
      - To execute a RunFlow job you pick a flow and select the data and model objects to use for each node in the flow, plus any required model parameters. TRAC then fetches the model code from the repository and the data records from storage and executes the job.



Models can be deployed and used with no coding or platform-level interventions if they contain the required
schema function.


.. list-table::
    :widths: 30 200

    * - **INPUTS**
      - The schema of the data inputs the model needs to run

    * - **PARAMETERS**
      - The schema of any parameters which influence how the model runs, which should be provided at runtime.

    * - **OUTPUTS**
      - The schema of the output data which the model generates.

.. note::
    See :ref:`modelling` for more details on the TRAC Model API how to build TRAC-ready models.


The existence of a properly declared model schema is confirmed when importing a model onto TRAC using
an ImportModel job. When constructing a flow, the platform validates that the proposed graph is consistent
with the schemas of the model objects. Finally, when executing a RunModel or RunFlow job, TRAC validates
that the model code generates outputs which are consistent with the declared schema.


TRAC Guarantee
____________

A central feature of the platform is the control environment it creates, which is built on immutabilty and repeatabiltiy. This is embodied by three things:

.. list-table::
    :widths: 45 60 200

    * - |icon-audit|
      - **AUDITABLE**
      - Every action that changes a tag or an object is recorded in a fully time-consistent fashion
        in the metadata model, so a complete version history is maintained by default.

    * - |icon-repeat|
      - **REPEATABLE**
      - Any RunModel or RunFlow job can be re-resubmitted and because the inputs are
        immutable, TRAC can repeat calculation and deliver the same result, guaranteed.

    * - |icon-persist|
      - **RISK FREE**
      - Every version of every object (model, data, flow) remains permanently available to use and there is
        no possibility of accidental loss or damage to deployed assets, so there is no change risk.

.. |icon-audit| image:: ../../images/icons/icon_audit.png
   :width: 66px
   :height: 66px

.. |icon-repeat| image:: ../../images/icons/icon_repeat.png
   :width: 66px
   :height: 66px

.. |icon-persist| image:: ../../images/icons/icon_persist.png
   :width: 66px
   :height: 66px

.. note::
    The repeatability guarantee does not apply to an ImportData job because changes in the external data source may mean that different data is brought across, and a model cannot be imported twice so an ImportModel job cannot be repeated.


Some other useful features
____________

    -   **Automated governance documentation** - The metadata is designed to br easily understood by
        both humans and machines and is fully controlled and searchable. Standard report formats can be
        used to create governance-ready documentation for model implementation oversight, data lineage
        reporting and internal audit.

    -   **Tweak and repeat** - Old jobs can be loaded up into the same tools used to create them originally,
        because the metadata format is the same. They can then be edited and resubmitted with any desired
        changes. Run last year's models with this year's data, or a series of what-if scenarios.
        If the new data and models are not compatible, TRAC will explain exactly what the differences are.

    -   **Parallel runs, parallel versions** - TRAC can execute as many parallel runs as the underlying compute
        infrastructure will allow. Because the runs are isolated and stateless, multiple runs can use different
        versions of the same model or the same dataset at the same time.

    -   **Combine model versions** - It is even possible to load different versions of the same model code within
        a single run. This can be useful to run challenger versions of individual components in a long model
        chain, or if some model components are versioned independently. TRAC handles the complexity of loading
        multiple versions of the same codebase into the executor process.
