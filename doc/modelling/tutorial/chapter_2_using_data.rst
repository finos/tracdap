
######################
Chapter 2 - Using Data
######################

This tutorial is based on the *using_data.py* example, which can be found in the
`TRAC GitHub Repository <https://github.com/finos/tracdap>`_
under *examples/models/python*.


Wrap existing code
------------------

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :caption: examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_1
    :lines: 15-51
    :linenos:
    :lineno-start: 15


Defining model requirements
---------------------------

Define parameters

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_2
    :lines: 52-67
    :linenos:
    :lineno-start: 52

Define inputs

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_3
    :lines: 68-78
    :linenos:
    :lineno-start: 68

Define outputs

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_4
    :lines: 79-86
    :linenos:
    :lineno-start: 79


Running the model
-----------------

Get inputs from the context

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_5
    :lines: 87-94
    :linenos:
    :lineno-start: 87

Call the model function

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_6
    :lines: 95-98
    :linenos:
    :lineno-start: 95

Give outputs to the context

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_7
    :lines: 99-101
    :linenos:
    :lineno-start: 90

Launch the model for local testing

.. literalinclude:: ../../../examples/models/python/src/tutorial/using_data.py
    :name: using_data_py_part_8
    :lines: 102-
    :linenos:
    :lineno-start: 102

Configure local data
--------------------

.. literalinclude:: ../../../examples/models/python/config/using_data.yaml
    :caption: examples/models/python/config/using_data.yaml
    :name: using_data.yaml
    :lines: 2-

.. literalinclude:: ../../../examples/models/python/config/sys_config.yaml
    :caption: examples/models/python/config/sys_config.yaml
    :name: sys_config.yaml
    :lines: 2-11
