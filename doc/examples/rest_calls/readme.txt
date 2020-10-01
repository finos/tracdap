
If you follow the instructions in README.md, you should get a running instance
of the metadata service and the gateway, with the gateway listening on
localhost:8080. You should also have a test tenant created, called ACME_CORP.

To load the sample flow into TRAC, you can use this API call:

POST http://localhost:8080/trac-meta/api/v1/ACME_CORP/flow/new-object

You should get a response with the object ID of the new object. To run the
example search, you can use this call:

POST http://localhost:8080/trac-meta/api/v1/ACME_CORP/FLOW/search

If you create objects and versions of objects with different tag attributes
you will be able to experiment with search parameters.

This is a very basic example of using the metadata service, more realistic /
instructive examples will be possible as more of the platform comes available!
