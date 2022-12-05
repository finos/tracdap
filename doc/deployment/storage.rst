
Storage Configuration
=====================


Local Storage
-------------

Local storage is available in the base platform and does not require installing any plugins.
For instructions on setting up local storage, see the
:doc:`sandbox quick start guide <sandbox>`

AWS S3 Storage
--------------

You will need to set up an S3 bucket, and an IAM user with permissions to access that bucket.
These are the permissions that need to be assigned to the bucket.

.. code-block:: json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "ListObjectsInBucket",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::<aws_account_id>:user/<iam_user>"
                },
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::<bucket_name>"
            },
            {
                "Sid": "AllObjectActions",
                "Effect": "Allow",
                "Principal": {
                    "AWS": "arn:aws:iam::<aws_account_id>:user/<iam_user>"
                },
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

You will then be able to configure an S3 storage instance in your platform configuration. The region,
bucket name and access key properties are required.

The *path* property is optional, if specified it will be used as a prefix for all objects stored in the bucket.
TRAC follows the convention of using path-like object keys, so backslashes can be used in the path prefix if desired.

.. code-block:: yaml

  storage:

    buckets:

      TEST_PLUGIN:
        protocol: S3
        properties:
          region: <aws_region>
          bucket: <aws_bucket_name>
          path: <storage_prefix>
          accessKeyId: <aws_access_key_id>
          secretAccessKey: <aws_secret_access_key>
