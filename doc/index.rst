
##############################
TRAC Data & Analytics Platform
##############################

.. centered::
    *A next-generation data and analytics platform for use in highly regulated environments*
	

.. note::
    We are building the documentation for TRAC in parallel with the open source version of the
    platform, both are in active deveopment. This documentaiton is presented in the hope that
    it will be useful before it is complete!

    You can see the current development status and roadmap for the platform on the
    `roadmap page <https://github.com/finos/tracdap/wiki/Development-Roadmap>`_.
    If you have particular questions or issues, please raise a ticket on our
    `issue tracker <https://github.com/finos/tracdap/issues>`_.

.. grid:: 1 2 2 2
    :gutter: 3

    .. grid-item-card::
        :class-footer: sd-border-0

        **Learn about TRAC**
        ^^^^^^^^^^^^^^^^^^^^

        Learn about the TRAC platform, starting with the metadata model.

        +++
        .. button-ref:: overview/introduction
            :color: primary
            :outline:
            :expand:

            Platform overview

    .. grid-item-card::
        :class-footer: sd-border-0

        **Build and run models**
        ^^^^^^^^^^^^^^^^^^^^^^^^

        Use the TRAC runtime APIs to build portable, self-documenting models.

        +++
        .. button-ref:: modelling/index
            :color: primary
            :outline:
            :expand:

            Modelling

    .. grid-item-card::
        :class-footer: sd-border-0

        **Build applications and workflows**
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        Connect web or desktop applications to the TRAC services and metadata catalog.

        *(Also relevant for system-to-system integration).*

        +++
        .. button-ref:: app_dev/index
            :color: primary
            :outline:
            :expand:

            App development

    .. grid-item-card::
        :class-footer: sd-border-0


        **Deploy and manage the platform**
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        Everything to do with deployment, configuration and technology integration,
        for dev-ops engineers and systems administrators.

        +++
        .. button-ref:: deployment/index
            :color: primary
            :outline:
            :expand:

            Deployment


.. toctree::
    :class: toc-hidden
    :maxdepth: 1

    overview/index
    modelling/index
    app_dev/index
    deployment/index
    reference/index


.. rubric:: Can't find what you're looking for?

* :ref:`search` let's you search across all the TRAC documentation
* :ref:`modindex` lists all the code modules with auto-generated documentation
* :ref:`genindex` is an index of individual methos and fields
