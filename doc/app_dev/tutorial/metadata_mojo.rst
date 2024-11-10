
#########################
Chapter 2 - Metadata Mojo
#########################

This tutorial is based on the *metadata_mojo.js* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/apps/javascript*.

It may be helpful to read the :doc:`metadata model overview </overview/metadata_model>`
before starting this tutorial.

Searching
=========

Metadata searches allow you to search the metadata database using tag attributes. Any and all
tag attributes are searchable. A selection of standard search operators are available, which
can be combined in logical expressions to form fine-grained searches. Any search can be executed
"as-of" a previous point in time, to provide a consistent view of the historical state  of the
platform.

Find a single object
""""""""""""""""""""

Starting with the simplest search, let's look for an object based on a single tag attribute.
In this example we're looking for a particular type of schema using a single search term,
``schema_type = "customer_records"``.

Let's create a request for this search:

.. literalinclude:: ../../../examples/apps/javascript/src/metadata_mojo.js
    :language: JavaScript
    :lines: 27 - 42
    :linenos:
    :lineno-start: 27

In the :class:`SearchParameters<tracdap.metadata.SearchParameters>` we set the type of object to search
for, and provide a search expression containing a single search term.

To execute the search we use the :meth:`search()<tracdap.api.TracMetadataApi.search>` method of the
:class:`TracMetadataApi<tracdap.api.TracMetadataApi>`.

.. literalinclude:: ../../../examples/apps/javascript/src/metadata_mojo.js
    :language: JavaScript
    :lines: 47 - 60
    :linenos:
    :lineno-start: 47

This method returns a search result, which is a list of tags without their definition bodies.
In a real-world scenario, the list might be displayed in a search grid with the attributes as
columns, so the user can look through the results and select the one they want. For this example
we just select the first result.

We return the ``header`` field of the selected objet, which is a :class:`TagHeader<tracdap.metadata.TagHeader>`
object that can be used as an object ID. For example, this ID could be used in the
:ref:`app_hello_loading_objects` example in the previous tutorial, to load tag with its full definition.

Logical search expressions
""""""""""""""""""""""""""

Search terms can be combined to form logical expressions, where each expression is either
a single term or a logical combination of other expressions.

Here is a simple example, starting with expression for the single term from the previous example:

.. literalinclude:: ../../../examples/apps/javascript/src/metadata_mojo.js
    :language: JavaScript
    :lines: 62 - 71
    :linenos:
    :lineno-start: 62

Now a second expression, we want to know if the business division attribute
is one of the business divisions we are interested in:

.. literalinclude:: ../../../examples/apps/javascript/src/metadata_mojo.js
    :language: JavaScript
    :lines: 71 - 86
    :linenos:
    :lineno-start: 71

See the documentation for :class:`SearchOperator<tracdap.metadata.SearchOperator>` for the
list of all available search operators.

Now let's create a logical expression, which combines the two previous expressions:

.. literalinclude:: ../../../examples/apps/javascript/src/metadata_mojo.js
    :language: JavaScript
    :lines: 88 - 96
    :linenos:
    :lineno-start: 71

Any number of expressions can be added to the ``AND`` clause without nesting, since ``AND`` is
an associative operation. The ``OR`` operator works the same way. If you want to combine ``AND``
and ``OR`` operations then nesting is required (a single search term with the ``IN`` operator
can often remove the need to use ``OR``). For the logical ``NOT`` operator, list of expressions
must contain exactly one expression (again, most search operators have negative equivalents which
remove the need to use ``NOT``).

Once the top level search expression is built, it can be included in a search request and used
to call the :meth:`search()<tracdap.api.TracMetadataApi.search>` method of the
:class:`TracMetadataApi<tracdap.api.TracMetadataApi>`.

.. literalinclude:: ../../../examples/apps/javascript/src/metadata_mojo.js
    :language: JavaScript
    :lines: 98 - 108
    :linenos:
    :lineno-start: 98


More Mojo
=========

Several other metadata features are available in the current release of TRAC, including:

* The ability to update objects and retrieve the full history of object versions
* Tags can be updated independently of their objects, with a full history of tag updates
* Point-in-time searches and selectors, providing a historical snapshot across all of TRAC's metadata
* The metadata type system, which allows attributes to be defined with any supported data type
* Multi-valued tag attributes

These features can be explored by looking at the documentation for the
:class:`TracMetadataApi<tracdap.api.TracMetadataApi>` and the :mod:`Metadata Listing <tracdap.metadata>`.
