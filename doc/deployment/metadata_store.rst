
Metadata Store
==============

To use a different SQL backend, get the JDBC driver JAR for your chosen database and copy it into the
*plugins* folder of the metadata service installation. Then edit the main configuration file
(usually trac-platform.yaml) with the correct settings from below.

.. note::

    A selection of popular database drivers is available in the TRAC plugins package,
    available with each release on the GitHub releases page:

    * https://github.com/finos/tracdap/releases

Properties for the JDBC datasource are passed directly to the JDBC driver. For example, if you are using
the SQLSERVER dialect, then *sqlserver.databaseName* will be passed to the driver as the *databaseName*,
*sqlserver.user* will be passed as *user* and so on. The properties supported by each driver are described
in their respective documentation pages. Pool size and overflow options are available for all dialects.

For database accounts secured with passwords, you may want to use the TRAC secrets configuration mechanism
which allows any property to be stored as a secret in the secret store. For details on setting up secrets
see :doc:`secrets`. In a cloud deployments it may be possible to use service roles, which removes the need
to keep credentials in the configuration altogether. Other authentication mechanisms such as Kerberos are
also possible and can be achieved by setting the correct JDBC properties, environment variables,
Java properties etc. If you have a particular requirement that does not work through the standard mechanisms,
please `get in touch <https://github.com/finos/tracdap/issues>`_.


**H2 configuration**

.. code-block:: yaml

    metadata:
      format: PROTO
      database:
        protocol: JDBC
        properties:
          dialect: H2
          jdbcUrl: <path to database file>
          h2.user: <username>
          h2.pass: <password>
          h2.schema: <schema>
          pool.size: 10
          pool.overflow: 5

.. note::
    H2 is mostly used in development scenarios where the password is not sensitive.
    If you want to use a secret for the H2 password, you can!

**MySQL configuration**

.. code-block:: yaml

    metadata:
      format: PROTO
      database:
        protocol: JDBC
        properties:
          dialect: MYSQL
          jdbcUrl: //<host>:<port>/<database>
          mysql.user: <username>
          pool.size: 10
          pool.overflow: 5
        secrets:
          mysql.password: metadb_password

**MariaDB configuration**

.. code-block:: yaml

    metadata:
      format: PROTO
      database:
        protocol: JDBC
        properties:
          dialect: MARIADB
          jdbcUrl: //<host>:<port>/<database>
          mariadb.user: <username>
          pool.size: 10
          pool.overflow: 5
        secrets:
          mariadb.password: metadb_password

.. note::
    The MariaDB driver is not available in the TRAC plugins package due to licensing restrictions.
    However, you can download it from Maven Central:

    * https://central.sonatype.com/artifact/org.mariadb.jdbc/mariadb-java-client/versions


**PostgreSQL configuration**

.. code-block:: yaml

    metadata:
      format: PROTO
      database:
        protocol: JDBC
        properties:
          dialect: POSTGRESQL
          jdbcUrl: //<host>:<port>/<database>
          postgresql.user: <username>
          pool.size: 10
          pool.overflow: 5
        secrets:
          postgresql.password: metadb_password

**SQL Server configuration**

.. code-block:: yaml

    metadata:
      format: PROTO
      database:
        protocol: JDBC
        properties:
          dialect: SQLSERVER
          jdbcUrl: //<host>:<port>
          sqlserver.databaseName: <database>
          sqlserver.user: <username>
          pool.size: 10
          pool.overflow: 5
        secrets:
          sqlserver.password: metadb_password

.. note::
    Oracle support is available but not actively tested in CI due to licensing issues. If you would like support for
    a different SQL dialect, please `get in touch <https://github.com/finos/tracdap/issues>`_.