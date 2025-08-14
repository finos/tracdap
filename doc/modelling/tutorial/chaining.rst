
***************************
Chapter 3 - Chaining Models
***************************

This tutorial is based on example code which can be found in the |examples_repo|.


Adding a second model
---------------------

In :doc:`using_data` we wrote a simple model to perform PnL aggregation on
some customer accounts. In this example we add a second model, to pre-filter the account
data. We can chain these two models together to create a FLOW. TRAC will run the flow
for us as a single job.

First, here is a new model that we can use to build the chain:

.. literalinclude:: ../../../examples/models/python/src/tutorial/chaining.py
    :caption: src/tutorial/chaining.py
    :name: chaining_py_part_1
    :language: python
    :lines: 22 - 50
    :linenos:
    :lineno-start: 22

The model takes a single parameter, ``filter_region``, and filers out any records in the
dataset that match that region. The schema of the input and output datasets are the same.

Notice that that input dataset key, ``customer_loans``, is the same key we used in the
``PnLAggregation`` model. Since this input is expected to refer to the same dataset, it
makes sense to give it the same key. The output key, ``filtered_loans``, is different so
we will have to tell TRAC how to connect these models together.


Defining a flow
---------------

To run a flow locally, we need to define the flow in YAML. Here is an example of a flow YAML file
that wires together the customer data filter with our PnL aggregation model:

.. literalinclude:: ../../../examples/models/python/config/chaining_flow.yaml
    :caption: config/chaining_flow.yaml
    :name: chaining_flow_yaml
    :language: yaml

The flow describes the chain of models as a graph, with **nodes** and **edges**. This example has
one input, two models and one output, which are defined as the flow *nodes*. Additionally,
the model nodes have to include the names of their inputs and outputs, so that TRAC can
understand the shape of the graph. The model inputs and outputs are called **sockets**.

TRAC wires up the *edges* of the graph based on name. If all the names are consistent and unique,
you might not need to define any edges at all! In this case we only need to define a single edge,
to connect the ``filtered_loans`` output of the filter model to the ``customer_loans`` input of
the aggregation model.

In this example the input and output nodes will be connected automatically, because their names
mach the appropriate model inputs and outputs. If we wanted to define those extra two edges
explicitly, it would look like this:

.. code-block:: yaml
    :class: container

      - source: { node: customer_loans }
        target: { node: customer_data_filter, socket: customer_loans }

      - source: { node: pnl_aggregation, socket: profit_by_region }
        target: { node: profit_by_region }

Notice that the input and output nodes do not have *sockets*, this is because each input and
output represents a single dataset, while models can have multiple inputs and outputs.

.. note::
    Using a consistent naming convention for the inputs and outputs of models in a single project
    can make it significantly easier to build and manage complex flows.


Setting up a job
----------------

Now we have a flow definition, in order to run it we will need a job config file.
Here is an example job config for this flow, using the two models we have available:

.. literalinclude:: ../../../examples/models/python/config/chaining.yaml
    :caption: config/chaining.yaml
    :name: chaining_yaml
    :language: yaml

The job type is now ``runFlow`` instead of ``runModel``. We supply the path to the flow YAML
file, which is resolved relative to the job config file. The parameters section has the
parameters needed by all the models in the flow. For the inputs and outputs, the keys
(``customer_loans`` and ``profit_by_region`` in this example) have to match the input and
output nodes in the flow.

In the models section, we specify which model to use for every model node in the flow.
It is important to use the fully-qualified name for each model, which means the Python
package structure should be set up correctly. See :doc:`hello_world` for a refresher on
setting up the repository layout and package structure.


Running a flow locally
----------------------

A flow can be launched locally as a job in the same way as a model.
You don't need to pass the model class (since we are not running a single model),
so just the job config and sys config files are required:

.. literalinclude:: ../../../examples/models/python/src/tutorial/chaining.py
    :caption: src/tutorial/chaining.py
    :name: chaining_py_part_2
    :language: python
    :lines: 53 - 55
    :linenos:
    :lineno-start: 53

This approach works well in some simple cases, such as this example, but for large codebases with
lots of models and multiple flows it is usually easier to launch thw flow directly. You can launch
a TRAC flow from the command line like this:

.. code-block::
    :class: container

    python -m tracdap.rt.launch --job-config config/chaining.yaml --sys-config config/sys_config.yaml --dev-mode

You can set this command up to run from your IDE and then use the IDE tools to run the command
in debug mode, which will let you debug into all the models in the chain. For example in PyCharm
you can set this command up as a Run Configuration.

.. note::
    Launching TRAC from the command line does not enable dev mode by default,
    always use the ``--dev-mode`` flag for local development.
