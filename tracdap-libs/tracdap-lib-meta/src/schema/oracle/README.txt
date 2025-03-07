Support for the Oracle SQL dialect was originally added along with the initial
set of dialects when the metadata service was created. It was built and tested
at that time using an Oracle instance hosted under their development license terms.

Due to difficulties accessing containerized images of the database, we do not build
against Oracle as part of our integration test suite. As the metadata model and
physical database schema are evolving, this means we can no longer guarantee the
Oracle dialect will work as expected. For this reason, we are dropping Oracle support
for the time being.

The original schema file and dialect implementation will remain in the source code
for now, so that they can be revived later should the need arise. In order to do this,
we would need to access to instances of the Oracle database running under a development
license, for both development and CI. Alternatively, if a containerized version becomes
available we can set up CI builds for Oracle similar to the other supported dialects.
