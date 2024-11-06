
##############################
TRAC: The Modern Model Platform
##############################

TRAC is a universal model orchestration solution designed for the most complex, critical and highly-governed
use cases.

The core platform services - i.e. TRAC Data & Analytics Platform (or TRAC D.A.P.) - are maintained by
`finTRAC Limited <https://www.fintrac.co.uk>`_ in association with the `finos Foundation <https://www.finos.org>`_
under the `Apache Software License version 2.0 <https://www.apache.org/licenses/LICENSE-2.0>`_.

This documentation site focuses on how to deploy the TRAC D.A.P. services and build both models and
applications which leverage those services. Commercially supported deployments of TRAC are separately
available from `finTRAC Limited <https://www.fintrac.co.uk>`_.


.. note::
   You can see the current development status of TRAC D.A.P. and a roadmap for the platform on the
    `roadmap page <https://github.com/finos/tracdap/wiki/Development-Roadmap>`_.
    If you have particular questions or issues, please raise a ticket on our
    `issue tracker <https://github.com/finos/tracdap/issues>`_.

.. grid:: 1 2 2 2
    :gutter: 3

    .. grid-item-card::
        :class-footer: sd-border-0

        **Learn about TRAC**
        ^^^^^^^^^^^^^^^^^^^^

        Learn about the TRAC, the metadata model, virtual deployment framework and the TRAC Guarantee.

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

        Use the TRAC runtime APIs to build portable, self-describing models.

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


        **Deploy and manage TRAC D.A.P.**
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

        Explore deployment, configuration and integration,
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
