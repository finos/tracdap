
***********
Using Files
***********

This tutorial is based on example code which can be found in the |examples_repo|.


Not every model input or output is a table. The runtime also supports files - images, documents,
or any other binary content - as first-class inputs and outputs alongside tabular datasets.


Writing a file output
-----------------------

File inputs and outputs are defined with :py:func:`define_input() <tracdap.rt.api.define_input>` and
:py:func:`define_output() <tracdap.rt.api.define_output>`, using a
:py:class:`FileType <tracdap.rt.metadata.FileType>` in place of a schema.
:py:class:`CommonFileTypes <tracdap.rt.api.CommonFileTypes>` provides a set of ready-made file types
for common formats, or a custom one can be built with
:py:func:`define_file_type() <tracdap.rt.api.define_file_type>`.

This example reads a tabular dataset and writes out an SVG chart built with Matplotlib:

.. literalinclude:: ../../../examples/models/python/src/tutorial/file_io_matplotlib.py
    :caption: src/tutorial/file_io_matplotlib.py
    :name: file_io_matplotlib_py_io
    :language: python
    :lines: 32 - 43
    :linenos:
    :lineno-start: 32

To write a file output, use :py:meth:`put_file_stream() <tracdap.rt.api.TracContext.put_file_stream>`,
which gives a writable binary stream for the output. The stream must be used in a ``with`` block, as
shown here where the Matplotlib figure is saved directly into the output stream.

.. literalinclude:: ../../../examples/models/python/src/tutorial/file_io_matplotlib.py
    :name: file_io_matplotlib_py_run_model
    :language: python
    :class: container
    :lines: 45 - 64
    :linenos:
    :lineno-start: 45

There is also a :py:meth:`put_file() <tracdap.rt.api.TracContext.put_file>` method that takes the
file content directly as bytes, for cases where the whole file is already available in memory rather
than being written incrementally to a stream.


Reading and writing files
----------------------------

Reading a file input works the same way, in reverse. This example reads a PowerPoint template as a
file input, along with a tabular dataset, and produces a new PowerPoint file with a chart added to it:

.. literalinclude:: ../../../examples/models/python/src/tutorial/file_io_powerpoint.py
    :caption: src/tutorial/file_io_powerpoint.py
    :name: file_io_powerpoint_py_io
    :language: python
    :lines: 34 - 48
    :linenos:
    :lineno-start: 34

:py:meth:`get_file_stream() <tracdap.rt.api.TracContext.get_file_stream>` gives a readable binary
stream for a file input, again used in a ``with`` block. The runtime guarantees that the file's
recorded mime type and extension match what was declared in :py:func:`define_input()
<tracdap.rt.api.define_input>`, but does not inspect or guarantee anything about the content of the
file itself.

.. literalinclude:: ../../../examples/models/python/src/tutorial/file_io_powerpoint.py
    :name: file_io_powerpoint_py_run_model
    :language: python
    :class: container
    :lines: 50 - 72
    :linenos:
    :lineno-start: 50

As with writing, there is a :py:meth:`get_file() <tracdap.rt.api.TracContext.get_file>` method that
returns the whole file as bytes, for cases where streaming isn't needed.


.. seealso::
    Full source code is available for the
    :example:`Matplotlib File IO example <src/tutorial/file_io_matplotlib.py>` and the
    :example:`PowerPoint File IO example <src/tutorial/file_io_powerpoint.py>` on GitHub.
