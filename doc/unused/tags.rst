
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

Versioning is supported for both objects and tags. For objects, versions are a series of immutable
copies where TRAC guarantees compatibility and continuity between versions. The general principal
for compatibility is that new versions will work in place of old versions (i.e. object versions are
backwards-compatible, but the reverse is not necessarily true) and for continuity is that the object
should describe the same resource. The exact requirements for these rules vary depending on object type.

Of particular interest are data updates. In this case, updates can include (1) adding a delta to a
dataset, (2) providing a new snapshot of a dataset (3) adding a partition or (4) updating a partition
with a new snapshot or delta. A new version of the metadata object is created that refers to the new set
of primary data files, including any that are unchanged from the previous version. For example if a delta
is added, the new data definition would refer to all the files referenced in the previous version, plus
the new delta.

A series of tag versions is assigned to every object version. Let's illustrate this with an example::

    v = 1, t = 1  # Initial creation of an object
                  # Let's say it's a dataset containing customer data for some date T0

    v = 1, t = 2  # Add a tag attribute, extra_attr = "some_value"

    v = 2, t = 1  # Corrections are applied to the data, so a new object version is created
                  # By default the attributes from v=1, t=2 are copied to the new tag

    v = 3, t = 1  # Data is added for a second day T1, in a separate partition

    v = 2, t = 2  # The data for T0 is signed off and the policy service updates the sign-off tag
                  # The tag applies to object version 2, which includes data for T0 with the corrections

Object and tag versions are given numbers as shown here, they are also given timestamps which are
recorded by the system when a new object or tag version is created. Either a version number or a
timestamp can be used to uniquely identify versions for both objects and tags.


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

    3.  | Select the version for a previous point in time
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
