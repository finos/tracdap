
Metadata Model
==============


The TRAC metadata model is a structured data model that records and catalogs every asset and
traceable action [#tracable]_ known to the TRAC platform. It consists of two layers:

    *   Objects are the structural element of the model, they represent assets and actions. Data, models
        and jobs are all described by metadata objects. Each type of object has a metadata structure that is
        defined as part of the TRAC API.

    *   Tags are used to index, describe and control objects, they are made up of key-value attributes.
        Some attributes are controlled by the platform, others can be set by client applications or
        edited by users.

Both objects and tags are versioned with an immutable, time-indexed version history, "updates" are performed
by creating a new version of the object or tag with the required changes. Because of this, the TRAC metadata
provides a fully consistent historical view of the platform for any previous point in time. It also provides
a complete audit history that is both machine and human readable, with no manual effort.

Where objects refer to external resources such as models and data, those resources are also immutable [#mutable]_.
This is achieved using e.g. GitHub tags or Nexus binary versions for models, and data areas owned by TRAC with
controlled write access for primary data. The combination of immutable metadata and immutable resources allows
TRAC to recreate any previous calculation that has run on the platform. As a result, generated and intermediate
data can often be discarded and recreated later if needed.


.. [#tracable]
    *Traceable actions are actions that create assets, such as running jobs or data imports. Read-only
    actions such as querying data or metadata searches are not currently recorded in the TRAC metadata.*

.. [#mutable]
    *TRAC objects can refer to mutable resources, however these cannot be used to build repeatable
    calculation jobs or workflows. Typically mutable objects are used as a source for import jobs.*

.. seealso::
    For a comprehensive reference of data structures in the metadata model, see the
    :doc:`metadata listing <../autoapi/trac/metadata/index>`.


Objects
-------

The four most common types of object show how a calculation run is expressed on the TRAC platform.

.. list-table::
    :header-rows: 1
    :widths: 66 66 200 200 200

    * -
      -
      - Metadata
      - Resource
      - Immutability

    * - **DATA**
      - |icon-data|
      - Structural representation of the data schema, plus its physical storage location
      - Reference to a physical data location [#extref]_
      - Physical datasets can be appended but never updated

    * - **MODEL**
      - |icon-model|
      - A model schema, describing the inputs, outputs and parameters of a model
      - Reference to immutable model code or a binary package (e.g. in Git or Nexus) [#extref]_
      - The model repository guarantees immutability of each model version,
        multiple versions can be imported

    * - **FLOW**
      - |icon-flow|
      - Structural representation of the data schema, plus its physical storage location
      - N/A
      - Metadata is always immutable

    * - **JOB**
      - |icon-job|
      - Structural representation of the data schema, plus its physical storage location
      - N/A
      - Metadata is always immutable

.. [#extref]
    *References to physical locations are not stored directly in the metadata.
    Instead the metadata refers to location keys which are described in the platform configuration,
    so resource locations can be moved when the platform is migrated or re-deployed.*

.. |icon-data| image:: ../_images/icon-data.png
   :width: 66px
   :height: 66px

.. |icon-model| image:: ../_images/icon-model.png
   :width: 66px
   :height: 66px

.. |icon-flow| image:: ../_images/icon-flow.png
   :width: 66px
   :height: 66px

.. |icon-job| image:: ../_images/icon-job.png
   :width: 66px
   :height: 66px


For the full API reference on metadata objects, see the
:class:`ObjectDefinition class reference <trac.metadata.ObjectDefinition>`.


.. rst-class:: html-toggle

More object types
~~~~~~~~~~~~~~~~~

.. list-table::
    :header-rows: 1
    :widths: 66 66 600

    * -
      -
      - Metadata

    * - **FILE**
      - |icon-file|
      - Structural representation of the data schema, plus its physical storage location

    * - **CUSTOM**
      - |icon-custom|
      - A model schema, describing the inputs, outputs and parameters of a model

    * - **STORAGE**
      - |icon-storage|
      - Structural representation of the data schema, plus its physical storage location

.. |icon-file| image:: ../_images/icon-file.png
   :width: 66px
   :height: 66px

.. |icon-custom| image:: ../_images/icon-custom.png
   :width: 66px
   :height: 66px

.. |icon-storage| image:: ../_images/icon-storage.png
   :width: 66px
   :height: 66px

Tags
----


:class:`Tags<trac.metadata.Tag>` are the core informational element of TRAC’s metadata model, they are
used to index, describe and control objects. Every object has a tag and each tag refers to a single object,
i.e. there is a one-to-one association.

A tag is made up of:

    * A header that identifies the tag and associated object
    * A set of attributes (key-value pairs)
    * The associated object definition

The object definition may sometimes be omitted, for example search results for metadata queries
do not include the full object definition.

Here is an example of a set of tag attributes to illustrate some ways they can be used::

    # A descriptive field intended for human users.

    display_name: "Customer accounts for March 2020, corrected April 6th"

    # A classification that can be used for searching or indexing.
    # Client applications can also use this to find datasets of a certain
    # type; typically an application will define a set of attributes that are
    # "structural", i.e. the application uses those attributes to decide which
    # objects to present for certain purposes.

    dataset_class: "customer_accounts"

    # Properties of an item can be added as individual attributes so they can
    # be searched and displayed individually. This avoids the anti-pattern of
    # putting multiple attributes into a single name/label field:
    #    customer_accounts_mar20_scotland_commercial_approved

    accounting_date: (DATE) 2020-03-31
    region: "Scotland"
    book: "commercial_property"
    figures_approved: (BOOLEAN) true

    # Attributes can be multi-valued. This can be helpful for applying
    # regulatory classifiers, where multiple classifiers may apply to a
    # single item.

    data_classification: ["confidential", "gdpr_pii", "audited"]

    # TRAC records a number of "controlled" attributes, these are set by the
    # platform and cannot be modified directly through the metadata API.
    # Controlled attributes start with the prefix "trac_".

    trac_create_time: (DATETIME) 2020-04-01 10:37:05
    trac_create_user_id: "jane.doe"
    trac_create_user_name: "Jane Doe"

Tag attributes are created and updated using :class:`TagUpdate<trac.metadata.TagUpdate>` operations.
Tag updates are instructions to add, replace, append (for multi-valued attributes) or delete an attribute.
These instructions can be supplied when an object is created or updated, in which case TRAC will fill
in some attributes automatically (timestamp, sign-off state etc). It is also possible to update tags
without changing the associated object, for example to reclassify a dataset or change a description.


Versioning
----------

Tags use immutable versioning in the same way as objects - each version of a tag is immutable and
“updating” a tag means creating a new version with one or more modified attributes. Each version of
an object has its own series of tags starting at tag version 1.

As an example of this versioning, consider a partitioned dataset with daily account records. Version X of
the dataset contains data up to a certain date and might have a tag saying it is signed off. A user/process
then adds a new partition with the next day’s data, creating version X+1. In this case, object version X
would still be signed off while version X+1 is awaiting approval. When version X+1 is approved, the tag for
that version can be “updated”. The application could decide whether to show the most recent version of the
data, or an earlier version that has the sign-off attribute set.


.. seealso::
    :class:`TagHeader <trac.metadata.TagHeader>`


Selectors
---------

A :class:`TagSelector <trac.metadata.TagSelector>` refers to a single object ID and identifies a specific
object version and tag version for that object. They are used throughout the TRAC platform whenever an
object is referenced, so it is always possible to specify versions using these selection criteria. The
available criteria are:

    1.  | Select the latest available version
        | - *Variable selector, will return a different result when an object or tag is updated to a new version*

    2.  | Select a fixed version number
        | - *Fixed selector, will always return the same result*

    3.  | Select a previous point in time
        | - *Fixed selector, will always return the same result*

Selectors are used in API calls, for example reading a single object from the metadata API uses a tag selector.
Sending API calls with selectors referring to a previous point in time allows client applications to display a
consistent historical view of the platform.

Selectors are also stored in the metadata model to express links between objects. For example, a job definition
uses tag selectors to identify the inputs and models that will be used to execute the job. In the case of a
job definition, the selectors are always stored as fixed selectors to indicate the precise object versions
used; if the user submits a job requesting the latest version of a model or input, TRAC will convert that
selector to a fixed selector before storing the job definition.

Selectors refer to object and tag versions independently and there is no requirement to use the same selection
criteria for both. A selector for objectVersion = 3 with latestTag = true is perfectly valid, this could be
used for example to check the current sign-off state of a particular version of a model.


Queries
-------

The TRAC metadata can be searched using logical expressions to match against tag attributes. Version
and/or timestamp information can also be included as search parameters. It is not possible to search the
contents of an object definition; any properties of an object that are needed for searching must be set
as tag attributes to make them available for metadata queries.

A search expression is a logical combination of search terms that can be built up as an expression tree.
The logical operators available are AND, OR and NOT. A search term matches an individual attribute using
one of the available search operators.


.. list-table::
    :header-rows: 1
    :widths: 75 500

    *
        - Operator
        - Meaning

    *   - **EQ** ==
        -   | Matches an attribute exactly. The attribute must be present and have the correct type and value.
              If the attribute is multi-valued, EQ will match if any of the values match.
            | *EQ may behave erratically for floating point attributes, using EQ, NE or IN with float values
              is not recommended.*

    *   - **NE** !=
        -   The logical inverse of EQ, matches precisely when EQ does not match. If the search attribute is
            not present, NE will match. If the search attribute is multi-value, NE will match only when none
            of the values match.

    *   - **IN**
        -   attr IN [a, b, c] is equivalent to attr == a OR attr = b OR attr = c. If the attribute is multi-
            valued, IN will match if any of the search values match any of the attribute values.

    *   -
            | **GT** >
            | **GE** >=
            | **LT** <
            | **LE** <=

        -   Ordered comparisons, for ordered data types only. The attribute must be present and the type must
            match the search type (comparing an integer to a float, or a date to a date-time value will not match).
            Ordered comparisons will never match if the search attribute is multi-valued.


By default, only the latest versions of objects and tags are considered in a search. Even if a prior version
of an object or tag version would have matched, that prior version is not considered. There are options in the
search parameters to include prior versions, in which case all matching versions of an object or tag will be
returned.

All searches can optionally be run as-of a previous point in time, which will cause the search to ignore
metadata generated after that time. These searches still have the option to include prior versions if
required. Using this feature allows clients to show a consistent historical view of the platform for
functionality that relies on metadata queries.

For the full API reference on metadata searches, see the reference pages for
:class:`SearchParameters<trac.metadata.SearchParameters>` and
:meth:`TracMetadataApi.search()<trac.api.TracMetadataApi.search>`.
