Testing
=======

Before you can run the test suite, you will need to set the following environment variables::

    export AWS_S3_BUCKET='<bucket name>'
    export AWS_S3_ACCESS_KEY='<access key>'
    export AWS_S3_SECRET_KEY='<secret key>'

Once the environment variables have been set, the test suite can be run with::

    python setup.py test
