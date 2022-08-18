
########################
Chapter 3 - Schema Files
########################

This tutorial is based on the *schema_files.py* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.

.. literalinclude:: ../../../examples/models/python/src/tutorial/schema_files.py
    :caption: schema_files.py
    :name: schema_files.py
    :lines: 15-

.. literalinclude:: ../../../examples/models/python/src/tutorial/schemas/customer_loans.csv
    :caption: customer_loans.csv
    :name: customer_loans.csv

.. literalinclude:: ../../../examples/models/python/src/tutorial/schemas/profit_by_region.csv
    :caption: profit_by_region.csv
    :name: profit_by_region.csv

The system config and job config files are exactly the same as in the *using_data* example,
they do not need to change when schemas are defined in schema files.
