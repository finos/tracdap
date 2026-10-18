These examples call the TRAC metadata API over REST. They need a running TRAC
platform with the gateway listening on localhost:8080 and a tenant called
ACME_CORP, for example the sandbox package that is published with each release
(see the main README for details).

To load the sample flow into TRAC, send the contents of create_flow.json in
this API call:

POST http://localhost:8080/trac-meta/api/v1/ACME_CORP/create-object

You should get a response with the object ID of the new object. To run the
example search, send the contents of search.json in this call:

POST http://localhost:8080/trac-meta/api/v1/ACME_CORP/search

The body of the search call is the search parameters on their own, as in
search.json, not a full search request. If you create objects and versions of
objects with different tag attributes you will be able to experiment with
search parameters.
