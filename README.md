S3 Object Store
-------

![badge](https://github.com/MartinHowarth/s3os/workflows/Test/badge.svg)

Simple pythonic wrapper to use s3 as a python object store.

Provides simple object storage methods; and a Dict-like interface for more complicated uses.

Examples
--------
Basic usage:

    from s3os import store_simple, retrieve_simple, delete_simple
    
    my_object = [1, 2, 3]
    
    store_simple("my_key", my_object)
    
    assert retrieve_simple("my_key") == my_object
    
    delete_simple("my_key")

The above example uses a global namespace in the bucket "s3os" - i.e. all the default settings of this package.

You can specify your own namespaces (i.e. buckets) as follows:

    from s3os import store, retrieve, delete, ObjectLocation, Bucket
    
    my_bucket = Bucket("my_bucket")
    my_object_location = ObjectLocation("my_key", bucket=my_bucket)
    my_object = [1, 2, 3]
    
    store(my_object_location, my_object)
    
    assert retrieve(my_object_location) == my_object
    
    delete(my_object_location)


Or simply use s3 like a normal python dictionary:

    from s3os import S3Dict, S3DictConfig, Bucket
    
    my_bucket = Bucket("my_bucket")
    s3dict = S3Dict(_config=S3DictConfig(id="my_dict_id", bucket=my_bucket))
    
    # Store information in s3
    s3dict["apples"] = 5
    s3dict["bananas"] = 2
    
    ...
    
    # Later, or in a different python executable, access the same dictionary again:
    my_bucket = Bucket("my_bucket")
    s3dict = S3Dict(_config=S3DictConfig(id="my_dict_id", bucket=my_bucket))
    
    print(s3dict["apples])  # 5
    print(s3dict.get_all_from_s3())  # {"apples": 5, "bananas": 2}
    


By default, `S3Dict` uses an internal cache to speed up item retrieval. 
Set and Delete operations are always performed synchronously.


Installation
------------
Install the package:

    pip install s3os

Setup your AWS credentials. For example set these environment variables:

    AWS_ACCESS_KEY_ID=...
    AWS_SECRET_ACCESS_KEY=...
    
Also see https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html for more authentication options.

> Note: `boto.client()` and `Session` authentication methods are not currently supported - raise an issue or submit a PR if you want them!


Development installation
------------------------
Install poetry - see https://pypi.org/project/poetry/

The following command should be used to install the dependencies:

    poetry install


Testing
-------
The following command should be used to run the tests:

    poetry run pytest tests

Valid AWS authentication credentials are required to run some of the tests.
See setup instructions.

The tests make a very small number of calls to S3, so the cost of running the tests is negligible.
