
########################
Chapter 3 - Schema Files
########################

This tutorial is based on the *schema_files.py* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.

.. literalinclude:: ../../../examples/models/python/src/tutorial/schema_files.py
    :caption: schema_files.py
    :language: python
    :name: schema_files.py
    :lines: 15-

.. csv-table:: customer_loans.csv
   :file: ../../../examples/models/python/src/tutorial/schemas/customer_loans.csv
   :header-rows: 1

.. csv-table:: profit_by_region.csv
   :file: ../../../examples/models/python/src/tutorial/schemas/profit_by_region.csv
   :header-rows: 1

The system config and job config files are exactly the same as in the *using_data* example,
they do not need to change when schemas are defined in schema files.
