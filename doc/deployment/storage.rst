
Storage Configuration
=====================


Local Storage
-------------

Local storage is available in the base platform and does not require installing any plugins.
For instructions on setting up local storage, see the
:doc:`sandbox quick start guide <sandbox>`

AWS S3 Storage
--------------

You will need to set up an S3 bucket and suitable role permissions. The role can be managed
by AWS, or explicitly by creating an IAM user or group assigned to the role.

Permissions can be managed at the bucket level or via role policies, either way you will need
statements to grant at a minimum these permissions:

.. code-block:: json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::<bucket_name>"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:*Object",
                    "s3:*ObjectAttributes"
                ],
                "Resource": "arn:aws:s3:::<bucket_name>/*"
            }
        ]
    }

To install the AWS storage plugin, download the plugins package from the latest release on the
`release page <https://github.com/finos/tracdap/releases>`_. Inside the plugins package you
will find a folder for the AWS storage plugin, copy the contents of this folder into the *plugins*
folder of your TRAC D.A.P. installation.

You will then be able to configure an S3 storage instance in your platform configuration. Region and bucket name
are required. The *prefix* property is optional, if specified it will be used as a prefix for all objects stored
in the bucket. TRAC follows the convention of using path-like object keys, so backslashes can be used in the
prefix if desired.

Credentials can be supplied in various different ways. For deployments where all the TRAC services are running
in AWS, the •default• mechanism allows permissions to be managed using AWS roles without need for further
configuration in TRAC. This is the preferred mechanism for deployments where it is available. If the TRAC
services are not running in AWS the *static* method can be used to authenticate with an access key.

.. code-block:: yaml

  storage:

    buckets:

      TEST_PLUGIN:
        protocol: S3
        properties:
          region: <aws_region>
          bucket: <aws_bucket_name>
          prefix: <storage_prefix>

          # For credentials supplied automatically by AWS assigned roles
          credentials: default

          # Or for credentials assigned to an explicit IAM user
          credentials: static
          accessKeyId: <aws_access_key_id>
          secretAccessKey: <aws_secret_access_key>
