
Metadata Store
==============

To use a different SQL backend, get the JDBC driver JAR for your chosen database and copy it into the
*plugins* folder of the metadata service installation. Then edit the main configuration file
(usually trac-platform.yaml) with the correct settings from below.
Pool size and overflow options are always available, the values below are examples only.

Oracle support is available but not actively tested in CI due to licensing issues. If you would like support for
a different SQL dialect, please `get in touch <https://github.com/finos/tracdap/issues>`_.

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
          mysql.password: <password>
          pool.size: 10
          pool.overflow: 5

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
          mariadb.password: <password>
          pool.size: 10
          pool.overflow: 5

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
          postgresql.password: <password>
          pool.size: 10
          pool.overflow: 5

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
          sqlserver.password: <password>
          pool.size: 10
          pool.overflow: 5

